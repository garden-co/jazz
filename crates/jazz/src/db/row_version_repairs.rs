//! Partial-Edge repair uses current Core authorization, never shared-cache RLS.
use super::peer_connection::queue_sync_context_control;
use super::row_availability::{CurrentRowsResult, request_current_rows_for_owner};
use super::*;
use crate::protocol::{CurrentRowCoordinate, CurrentRowOutcome, PolicyBindingKey, RowVersionRef};

const MAX_PENDING_REPAIRS: usize = 8;
const RETRY_DELAY_MS: u64 = 100;

pub(super) struct PendingAuthorityRepair {
    requests: Vec<RowVersionRef>,
    context: PolicyBindingKey,
    session_revision: u64,
    requires_core: bool,
    coordinates: Vec<CurrentRowCoordinate>,
    request_coordinates: Vec<CurrentRowCoordinate>,
    next: usize,
    proofs: Vec<(RowVersionRef, CurrentRowCoordinate)>,
    authority: Option<AuthorityContext>,
    future: Option<futures::future::LocalBoxFuture<'static, CurrentRowsResult>>,
    retry_after: u64,
}

pub(super) fn enqueue_authority_repair(
    queue: &mut VecDeque<PendingAuthorityRepair>,
    requests: Vec<RowVersionRef>,
    binding: (AuthorSubject, BTreeMap<String, Value>),
    session_revision: u64,
    requires_core: bool,
) -> Result<(), Error> {
    if queue.len() >= MAX_PENDING_REPAIRS {
        return Err(Error::new(
            ErrorCode::Protocol,
            "partial Edge repair queue capacity reached",
        ));
    }
    queue.push_back(PendingAuthorityRepair {
        requests,
        context: PolicyBindingKey::from_canonical_parts(binding.0, binding.1),
        session_revision,
        requires_core,
        coordinates: Vec::new(),
        request_coordinates: Vec::new(),
        next: 0,
        proofs: Vec::new(),
        authority: None,
        future: None,
        retry_after: 0,
    });
    Ok(())
}

impl<S: OrderedKvStorage + ReopenableStorage + 'static> PeerConnection<S> {
    pub(super) async fn drive_pending_authority_repairs(
        &mut self,
        waker: Option<&std::task::Waker>,
    ) -> Result<(), Error> {
        let ConnectionLink::Subscriber(state) = &mut self.link else {
            return Ok(());
        };
        let Some(pending) = state.pending_authority_repairs.front_mut() else {
            return Ok(());
        };
        if pending.session_revision != state.session_claim_revision {
            // A fresh link restarts FIFO repair under its new authenticated
            // context. Dropping futures cancels every old exact router nonce.
            state.pending_authority_repairs.clear();
            return Err(Error::new(
                ErrorCode::Protocol,
                "repair authentication changed; reconnect before retrying",
            ));
        }
        let now = self.upload_retry_clock.borrow().now_ms();
        let selected = *self.admitted_upstream_authority.borrow();
        if pending.requires_core {
            if pending
                .authority
                .is_some_and(|old| selected.is_none_or(|new| !old.same_admitted_link(new)))
            {
                pending.future = None;
                pending.authority = None;
                pending.coordinates.clear();
                pending.request_coordinates.clear();
                pending.proofs.clear();
                pending.next = 0;
                pending.retry_after = 0;
            }
            if now < pending.retry_after {
                return Ok(());
            }
            if pending.coordinates.is_empty() && !pending.requests.is_empty() {
                let mut node = self.node.lock().await;
                for request in &pending.requests {
                    let coordinate = node.current_row_coordinate_for_repair(request).await?;
                    pending.request_coordinates.push(coordinate.clone());
                    if !pending.coordinates.contains(&coordinate) {
                        pending.coordinates.push(coordinate);
                    }
                }
            }
            if let Some(future) = pending.future.as_mut() {
                let mut cx =
                    std::task::Context::from_waker(waker.unwrap_or(std::task::Waker::noop()));
                let std::task::Poll::Ready(result) = future.as_mut().poll(&mut cx) else {
                    return Ok(());
                };
                pending.future = None;
                match result {
                    CurrentRowsResult::Applied(receipt)
                        if !receipt.outcomes.contains(&CurrentRowOutcome::Unknown) =>
                    {
                        for (coordinate, outcome) in receipt.rows.iter().zip(&receipt.outcomes) {
                            if *outcome == CurrentRowOutcome::Readable {
                                for (request, expected) in
                                    pending.requests.iter().zip(&pending.request_coordinates)
                                {
                                    if expected == coordinate {
                                        // The native payload resolver additionally checks the
                                        // requested body belongs to this exact physical lineage.
                                        pending.proofs.push((request.clone(), coordinate.clone()));
                                    }
                                }
                            }
                        }
                        pending.next += receipt.rows.len();
                    }
                    _ => {
                        pending.retry_after = now.saturating_add(RETRY_DELAY_MS);
                        if let Some(scheduler) = self.scheduler.borrow().as_ref() {
                            scheduler.schedule_tick_after(RETRY_DELAY_MS);
                        }
                        return Ok(());
                    }
                }
            }
            if pending.next < pending.coordinates.len() {
                if selected.is_none() {
                    pending.retry_after = now.saturating_add(RETRY_DELAY_MS);
                    if let Some(scheduler) = self.scheduler.borrow().as_ref() {
                        scheduler.schedule_tick_after(RETRY_DELAY_MS);
                    }
                    return Ok(());
                }
                pending.authority = selected;
                let end = (pending.next + crate::protocol::MAX_CURRENT_ROWS)
                    .min(pending.coordinates.len());
                pending.future = Some(Box::pin(request_current_rows_for_owner(
                    Rc::clone(&self.current_rows),
                    Rc::clone(&self.scheduler),
                    true,
                    pending.coordinates[pending.next..end].to_vec(),
                    pending.context.clone(),
                )));
                return Ok(());
            }
        }
        let bundles = {
            let mut node = self.node.lock().await;
            if pending.requires_core {
                // Catalogue changes can occur after native receipt ingestion but
                // before this downstream connection gets its next turn.
                for (request, expected) in pending.requests.iter().zip(&pending.request_coordinates)
                {
                    if node.current_row_coordinate_for_repair(request).await? != *expected {
                        pending.coordinates.clear();
                        pending.request_coordinates.clear();
                        pending.proofs.clear();
                        pending.next = 0;
                        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
                        return Ok(());
                    }
                }
            }
            let authorization = if pending.requires_core {
                crate::node::RowVersionRepairAuthorization::VerifiedCurrentRows(&pending.proofs)
            } else {
                crate::node::RowVersionRepairAuthorization::EnforceReadPolicy(AuthorSubject::SYSTEM)
            };
            node.row_version_payloads_for_refs(&pending.requests, authorization)
                .await?
        };
        state.pending_authority_repairs.pop_front();
        queue_sync_context_control(
            &mut self.pending_control_responses,
            SyncMessage::RowVersionPayloads {
                version_bundles: bundles,
            },
        );
        schedule_tick_in(&self.scheduler, TickUrgency::Immediate);
        Ok(())
    }
}
