//! Read-your-writes across independently credited channels (SPEC 8 §8.2.1).
//!
//! A link sends commit units on the writes channel and opens on the requests
//! channel. Each channel is FIFO, but credit and scheduling can deliver a
//! later open before an earlier write, and a reconnect resends both with no
//! order between them. So before its next request after new writes, the
//! sending link puts a [`SyncMessage::ReadBarrier`] naming how many commit
//! units it has sent on this link. The receiving link holds that barrier and
//! every later request until it has answered that many commit units.
//!
//! Counting is per link on both sides: a new link starts both counts at zero,
//! and the sender uploads its outbox before reissuing its opens, so a
//! reissued open names every write resent ahead of it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::protocol::SyncMessage;
use crate::tx::{DurabilityTier, Fate, TxId};
use crate::wire::channel_credit::BufferLease;

use super::channel_endpoint::message_class;
use crate::wire::channels::ChannelClass;

/// Sending side: the commit units this link has put on the wire, and the
/// count the last barrier named.
#[derive(Debug, Default)]
pub(super) struct ReadBarrierSender {
    commit_units_sent: u64,
    barrier_sent: u64,
}

impl ReadBarrierSender {
    /// Record one commit unit the transport accepted on this link.
    pub(super) fn commit_unit_sent(&mut self) {
        self.commit_units_sent += 1;
    }

    /// The barrier to send before the next request, if writes were sent
    /// since the last one. Call [`Self::barrier_accepted`] once the
    /// transport accepts it.
    pub(super) fn pending_barrier(&self) -> Option<SyncMessage> {
        (self.commit_units_sent > self.barrier_sent).then_some(SyncMessage::ReadBarrier {
            writes_through: self.commit_units_sent,
        })
    }

    /// The transport accepted the barrier returned by
    /// [`Self::pending_barrier`].
    pub(super) fn barrier_accepted(&mut self) {
        self.barrier_sent = self.commit_units_sent;
    }
}

/// One request held behind an unanswered barrier, with the buffer lease that
/// keeps its transport credit until it is served. Holding the lease is what
/// bounds this queue: a sender cannot outrun the credit it is granted.
pub(super) struct HeldRequest {
    pub(super) message: SyncMessage,
    pub(super) lease: Option<BufferLease>,
    pub(super) receipts_validated: bool,
}

/// Receiving side of the barrier for one link.
#[derive(Default)]
pub(super) struct ReadBarrierReceiver {
    /// Commit units received on this link; the next one gets `received + 1`.
    received: u64,
    /// Ordinals of received commit units with no answer yet.
    unanswered: BTreeSet<u64>,
    /// The unanswered ordinals of each transaction, oldest first. A writer
    /// can resend a write, so one transaction can arrive more than once on a
    /// link.
    unanswered_by_tx: BTreeMap<TxId, VecDeque<u64>>,
    /// Writes this link has answered as stored here and not yet settled.
    /// A copy of one that arrives later is already answered: a recovering
    /// relay acknowledges a write it restored from its own storage before
    /// the writer's resent copy reaches it, and does not acknowledge the
    /// copy again. A terminal fate removes the entry, so it holds at most the
    /// writes still in flight on this link.
    answered: BTreeSet<TxId>,
    /// Requests from the first unsatisfied barrier on, in arrival order. The
    /// front is always a `ReadBarrier`.
    held: VecDeque<HeldRequest>,
    /// Requests a satisfied barrier released, served before new input.
    released: VecDeque<HeldRequest>,
}

impl ReadBarrierReceiver {
    /// Number a commit unit as it arrives on this link.
    pub(super) fn commit_unit_received(&mut self, tx_id: TxId) {
        self.received += 1;
        if self.answered.contains(&tx_id) {
            return;
        }
        self.unanswered.insert(self.received);
        self.unanswered_by_tx
            .entry(tx_id)
            .or_default()
            .push_back(self.received);
    }

    /// Record an answer this link sent downstream: the first `FateUpdate`
    /// for a write. It means this node has stored or decided the write: a
    /// node with an upstream first sends a local
    /// acknowledgement once the write is durable here, and the authority
    /// sends the write's terminal fate. Waiting for this node rather than the
    /// authority keeps a local-first relay serving its clients offline; a
    /// relay's own upstream link puts the write ahead of the requests it
    /// forwards. Returns whether held requests may now be released.
    pub(super) fn answer_sent(&mut self, message: &SyncMessage) -> bool {
        let SyncMessage::FateUpdate { tx_id, .. } = message else {
            return false;
        };
        let tx_id = *tx_id;
        // A terminal fate ends this answer's standing: a copy arriving after
        // a terminal fate gets that fate again.
        let settled = matches!(
            message,
            SyncMessage::FateUpdate {
                fate: Fate::Rejected(_),
                ..
            } | SyncMessage::FateUpdate {
                durability: Some(DurabilityTier::Global),
                ..
            }
        );
        if settled {
            self.answered.remove(&tx_id);
        } else {
            self.answered.insert(tx_id);
        }
        let Some(ordinals) = self.unanswered_by_tx.get_mut(&tx_id) else {
            return false;
        };
        let ordinal = ordinals
            .pop_front()
            .expect("no empty ordinal queues are kept");
        if ordinals.is_empty() {
            self.unanswered_by_tx.remove(&tx_id);
        }
        self.unanswered.remove(&ordinal);
        self.release_satisfied()
    }

    fn satisfied(&self, writes_through: u64) -> bool {
        self.received >= writes_through
            && self
                .unanswered
                .first()
                .is_none_or(|oldest| *oldest > writes_through)
    }

    /// Whether this inbound request must wait. A request waits behind any
    /// held barrier so requests keep their order, and an unsatisfied barrier
    /// starts the wait. Satisfied barriers are consumed here.
    pub(super) fn admit(&mut self, request: HeldRequest) -> Option<HeldRequest> {
        if message_class(&request.message).0 != ChannelClass::Requests {
            return Some(request);
        }
        if !self.held.is_empty() {
            self.held.push_back(request);
            return None;
        }
        match request.message {
            SyncMessage::ReadBarrier { writes_through } if self.satisfied(writes_through) => None,
            SyncMessage::ReadBarrier { .. } => {
                self.held.push_back(request);
                None
            }
            _ => Some(request),
        }
    }

    /// Move requests behind now-satisfied barriers to the released queue.
    fn release_satisfied(&mut self) -> bool {
        let mut released_any = false;
        while let Some(HeldRequest {
            message: SyncMessage::ReadBarrier { writes_through },
            ..
        }) = self.held.front()
        {
            if !self.satisfied(*writes_through) {
                break;
            }
            self.held.pop_front();
            while let Some(next) = self.held.front() {
                if matches!(next.message, SyncMessage::ReadBarrier { .. }) {
                    break;
                }
                self.released
                    .push_back(self.held.pop_front().expect("front exists"));
                released_any = true;
            }
        }
        released_any
    }

    /// The next request a barrier released, ahead of new input.
    pub(super) fn pop_released(&mut self) -> Option<HeldRequest> {
        self.released.pop_front()
    }

    /// Whether a request waits behind an unanswered barrier.
    pub(super) fn is_holding(&self) -> bool {
        !self.held.is_empty()
    }

    /// Whether a satisfied barrier released requests not yet served.
    pub(super) fn has_released(&self) -> bool {
        !self.released.is_empty()
    }

    #[cfg(test)]
    pub(super) fn held_len(&self) -> usize {
        self.held.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeUuid;
    use crate::protocol::PermissionAdviceRequestId;
    use crate::time::{GlobalTime, TxTime};
    use crate::tx::RejectionReason;

    fn tx(n: u64) -> TxId {
        TxId::new(TxTime(n), NodeUuid::from_bytes([1; 16]))
    }

    fn request(message: SyncMessage) -> HeldRequest {
        HeldRequest {
            message,
            lease: None,
            receipts_validated: false,
        }
    }

    fn open(id: u8) -> SyncMessage {
        SyncMessage::CurrentRowsCancel {
            request_id: PermissionAdviceRequestId([id; 16]),
        }
    }

    fn barrier(writes_through: u64) -> SyncMessage {
        SyncMessage::ReadBarrier { writes_through }
    }

    fn accepted(tx_id: TxId, durability: DurabilityTier) -> SyncMessage {
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(7)),
            durability: Some(durability),
        }
    }

    fn opened_ids(receiver: &mut ReadBarrierReceiver) -> Vec<u8> {
        std::iter::from_fn(|| receiver.pop_released())
            .map(|held| match held.message {
                SyncMessage::CurrentRowsCancel { request_id } => request_id.0[0],
                other => panic!("released {other:?}"),
            })
            .collect()
    }

    /// Alice writes, then reads. Her read reaches the server before her
    /// write. The server holds the read, and every request behind it, until
    /// it has answered her write (#3863).
    #[test]
    fn a_read_that_overtakes_its_write_waits_for_the_answer() {
        let mut server = ReadBarrierReceiver::default();
        assert!(server.admit(request(barrier(1))).is_none());
        assert!(server.admit(request(open(1))).is_none());
        assert!(server.admit(request(open(2))).is_none());
        assert_eq!(server.held_len(), 3);

        server.commit_unit_received(tx(1));
        assert!(server.pop_released().is_none(), "received is not answered");
        assert!(server.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
        assert_eq!(opened_ids(&mut server), [1, 2]);
        assert_eq!(server.held_len(), 0);
    }

    /// Carol's client writes through a local relay with no upstream. The
    /// relay's local acknowledgement answers the write, so her reads keep
    /// being served offline; the later global fate answers nothing more.
    #[test]
    fn a_local_acknowledgement_answers_the_write_on_a_relay() {
        let mut relay = ReadBarrierReceiver::default();
        relay.commit_unit_received(tx(1));
        relay.admit(request(barrier(1)));
        relay.admit(request(open(1)));
        let local_ack = SyncMessage::FateUpdate {
            tx_id: tx(1),
            fate: Fate::Pending,
            global_time: None,
            durability: Some(DurabilityTier::Local),
        };
        assert!(relay.answer_sent(&local_ack));
        assert_eq!(opened_ids(&mut relay), [1]);
        assert!(!relay.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
    }

    /// A worker that restarts restores its writer's write from storage and
    /// acknowledges it before the writer's resent copy arrives. The copy is
    /// answered already, and the reads behind it are served.
    #[test]
    fn a_write_acknowledged_before_its_copy_arrives_is_answered() {
        let local_ack = SyncMessage::FateUpdate {
            tx_id: tx(1),
            fate: Fate::Pending,
            global_time: None,
            durability: Some(DurabilityTier::Local),
        };
        let mut worker = ReadBarrierReceiver::default();
        assert!(!worker.answer_sent(&local_ack));
        worker.commit_unit_received(tx(1));
        assert!(worker.admit(request(barrier(1))).is_none());
        assert!(worker.admit(request(open(1))).is_some());

        // A terminal fate settles the write: a later copy gets that fate
        // again, so it waits for it.
        assert!(!worker.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
        worker.commit_unit_received(tx(1));
        worker.admit(request(barrier(2)));
        worker.admit(request(open(2)));
        assert!(worker.pop_released().is_none());
        assert!(worker.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
        assert_eq!(opened_ids(&mut worker), [2]);
    }

    #[test]
    fn a_read_after_answered_writes_passes_without_holding() {
        let mut server = ReadBarrierReceiver::default();
        server.commit_unit_received(tx(1));
        server.answer_sent(&SyncMessage::FateUpdate {
            tx_id: tx(1),
            fate: Fate::Rejected(RejectionReason::MalformedCommit("no".into())),
            global_time: None,
            durability: None,
        });
        assert!(server.admit(request(barrier(1))).is_none(), "consumed");
        assert!(server.admit(request(open(1))).is_some());
        // Messages outside the requests channel are never held.
        server.admit(request(barrier(5)));
        let fate = request(accepted(tx(9), DurabilityTier::Global));
        assert!(server.admit(fate).is_some());
    }

    /// Bob's second read names a later write than his first. Answering the
    /// first write releases only the first read; requests keep their order.
    #[test]
    fn barriers_release_in_order_and_only_when_their_own_writes_are_answered() {
        let mut server = ReadBarrierReceiver::default();
        server.commit_unit_received(tx(1));
        server.commit_unit_received(tx(2));
        server.admit(request(barrier(1)));
        server.admit(request(open(1)));
        server.admit(request(barrier(2)));
        server.admit(request(open(2)));

        // Answers can arrive out of order; the oldest unanswered write gates.
        assert!(!server.answer_sent(&accepted(tx(2), DurabilityTier::Global)));
        assert!(server.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
        assert_eq!(opened_ids(&mut server), [1, 2]);
    }

    #[test]
    fn a_resent_write_is_answered_once_per_arrival() {
        let mut server = ReadBarrierReceiver::default();
        server.commit_unit_received(tx(1));
        // A rejection settles the write, so a resent copy waits for its own
        // answer.
        assert!(!server.answer_sent(&SyncMessage::FateUpdate {
            tx_id: tx(1),
            fate: Fate::Rejected(RejectionReason::MalformedCommit("no".into())),
            global_time: None,
            durability: None,
        }));
        server.commit_unit_received(tx(1));
        server.admit(request(barrier(2)));
        server.admit(request(open(3)));
        assert!(opened_ids(&mut server).is_empty());
        // An answer for a transaction this link never sent changes nothing.
        assert!(!server.answer_sent(&accepted(tx(4), DurabilityTier::Global)));
        assert!(server.answer_sent(&accepted(tx(1), DurabilityTier::Global)));
        assert_eq!(opened_ids(&mut server), [3]);
    }

    #[test]
    fn sender_names_each_new_write_count_once() {
        let mut client = ReadBarrierSender::default();
        assert!(client.pending_barrier().is_none());
        client.commit_unit_sent();
        client.commit_unit_sent();
        assert_eq!(client.pending_barrier(), Some(barrier(2)));
        // Not accepted by the transport yet: still pending.
        assert_eq!(client.pending_barrier(), Some(barrier(2)));
        client.barrier_accepted();
        assert!(client.pending_barrier().is_none());
        client.commit_unit_sent();
        assert_eq!(client.pending_barrier(), Some(barrier(3)));
    }
}
