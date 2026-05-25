use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeRole {
    Client,
    Replica,
    Coordinator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub seq: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Delivery {
    Delivered(Envelope),
    Dropped(Envelope),
    NoSuchNode(Envelope),
    NodeStopped(Envelope),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeliveryCounters {
    pub delivered: usize,
    pub dropped: usize,
    pub no_such_node: usize,
    pub node_stopped: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCounters {
    pub id: NodeId,
    pub role: NodeRole,
    pub generation: u64,
    pub running: bool,
    pub inbox_len: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HarnessTrace {
    pub deliveries: DeliveryCounters,
    pub nodes: Vec<NodeCounters>,
    pub pending: usize,
    pub dropped: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub role: NodeRole,
    pub generation: u64,
    pub running: bool,
    inbox: Vec<Envelope>,
}

impl Node {
    fn new(id: NodeId, role: NodeRole) -> Self {
        Self {
            id,
            role,
            generation: 0,
            running: true,
            inbox: Vec::new(),
        }
    }

    pub fn inbox(&self) -> &[Envelope] {
        &self.inbox
    }

    pub fn take_inbox(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.inbox)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Network {
    next_seq: u64,
    queued: VecDeque<Envelope>,
    dropped: Vec<Envelope>,
}

impl Network {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&mut self, from: NodeId, to: NodeId, payload: impl Into<Vec<u8>>) -> Envelope {
        let envelope = Envelope {
            seq: self.next_seq,
            from,
            to,
            payload: payload.into(),
        };
        self.next_seq += 1;
        self.queued.push_back(envelope.clone());
        envelope
    }

    pub fn queued(&self) -> impl Iterator<Item = &Envelope> {
        self.queued.iter()
    }

    pub fn dropped(&self) -> &[Envelope] {
        &self.dropped
    }

    pub fn pending_len(&self) -> usize {
        self.queued.len()
    }

    fn pop_next(&mut self) -> Option<Envelope> {
        self.queued.pop_front()
    }

    fn drop_next(&mut self) -> Option<Envelope> {
        let envelope = self.queued.pop_front()?;
        self.dropped.push(envelope.clone());
        Some(envelope)
    }

    fn drop_where(&mut self, predicate: impl FnMut(&Envelope) -> bool) -> Option<Envelope> {
        let index = self.queued.iter().position(predicate)?;
        let envelope = self
            .queued
            .remove(index)
            .expect("position came from the same queue");
        self.dropped.push(envelope.clone());
        Some(envelope)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Harness {
    nodes: BTreeMap<NodeId, Node>,
    network: Network,
    deliveries: DeliveryCounters,
}

impl Harness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, id: NodeId, role: NodeRole) -> &Node {
        let previous = self.nodes.insert(id, Node::new(id, role));
        assert!(previous.is_none(), "node already exists: {:?}", id);
        self.nodes.get(&id).expect("node was just inserted")
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(&id)
    }

    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn network_mut(&mut self) -> &mut Network {
        &mut self.network
    }

    pub fn send(&mut self, from: NodeId, to: NodeId, payload: impl Into<Vec<u8>>) -> Envelope {
        self.network.enqueue(from, to, payload)
    }

    pub fn deliver_next(&mut self) -> Option<Delivery> {
        let envelope = self.network.pop_next()?;
        let delivery = self.deliver_envelope(envelope);
        self.record_delivery(&delivery);
        Some(delivery)
    }

    pub fn deliver_all(&mut self) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        while let Some(delivery) = self.deliver_next() {
            deliveries.push(delivery);
        }
        deliveries
    }

    pub fn drop_next(&mut self) -> Option<Delivery> {
        let delivery = self.network.drop_next().map(Delivery::Dropped)?;
        self.record_delivery(&delivery);
        Some(delivery)
    }

    pub fn drop_where(&mut self, predicate: impl FnMut(&Envelope) -> bool) -> Option<Delivery> {
        let delivery = self.network.drop_where(predicate).map(Delivery::Dropped)?;
        self.record_delivery(&delivery);
        Some(delivery)
    }

    pub fn trace(&self) -> HarnessTrace {
        HarnessTrace {
            deliveries: self.deliveries.clone(),
            nodes: self
                .nodes
                .values()
                .map(|node| NodeCounters {
                    id: node.id,
                    role: node.role,
                    generation: node.generation,
                    running: node.running,
                    inbox_len: node.inbox.len(),
                })
                .collect(),
            pending: self.network.pending_len(),
            dropped: self.network.dropped().len(),
        }
    }

    pub fn stop_node(&mut self, id: NodeId) -> bool {
        let Some(node) = self.nodes.get_mut(&id) else {
            return false;
        };
        node.running = false;
        true
    }

    pub fn restart_node(&mut self, id: NodeId) -> bool {
        let Some(node) = self.nodes.get_mut(&id) else {
            return false;
        };
        node.generation += 1;
        node.running = true;
        node.inbox.clear();
        true
    }

    fn deliver_envelope(&mut self, envelope: Envelope) -> Delivery {
        let Some(node) = self.nodes.get_mut(&envelope.to) else {
            return Delivery::NoSuchNode(envelope);
        };
        if !node.running {
            return Delivery::NodeStopped(envelope);
        }
        node.inbox.push(envelope.clone());
        Delivery::Delivered(envelope)
    }

    fn record_delivery(&mut self, delivery: &Delivery) {
        match delivery {
            Delivery::Delivered(_) => self.deliveries.delivered += 1,
            Delivery::Dropped(_) => self.deliveries.dropped += 1,
            Delivery::NoSuchNode(_) => self.deliveries.no_such_node += 1,
            Delivery::NodeStopped(_) => self.deliveries.node_stopped += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice() -> NodeId {
        NodeId(1)
    }

    fn bob() -> NodeId {
        NodeId(2)
    }

    fn carol() -> NodeId {
        NodeId(3)
    }

    #[test]
    fn delivers_messages_in_enqueue_order() {
        let mut harness = Harness::new();
        harness.add_node(alice(), NodeRole::Client);
        harness.add_node(bob(), NodeRole::Replica);

        harness.send(alice(), bob(), b"create account".to_vec());
        harness.send(alice(), bob(), b"rename account".to_vec());
        harness.send(bob(), alice(), b"ack rename".to_vec());

        let deliveries = harness.deliver_all();

        assert_eq!(deliveries.len(), 3);
        assert!(matches!(deliveries[0], Delivery::Delivered(_)));
        assert!(matches!(deliveries[1], Delivery::Delivered(_)));
        assert!(matches!(deliveries[2], Delivery::Delivered(_)));
        assert_eq!(
            harness
                .node(bob())
                .expect("bob exists")
                .inbox()
                .iter()
                .map(|envelope| envelope.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"create account".as_slice(), b"rename account".as_slice()]
        );
        assert_eq!(
            harness
                .node(alice())
                .expect("alice exists")
                .inbox()
                .iter()
                .map(|envelope| envelope.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"ack rename".as_slice()]
        );
    }

    #[test]
    fn drops_selected_message_without_reordering_the_rest() {
        let mut harness = Harness::new();
        harness.add_node(alice(), NodeRole::Client);
        harness.add_node(bob(), NodeRole::Replica);

        let first = harness.send(alice(), bob(), b"first mutation".to_vec());
        let second = harness.send(alice(), bob(), b"second mutation".to_vec());
        let third = harness.send(alice(), bob(), b"third mutation".to_vec());

        assert_eq!(
            harness.drop_where(|envelope| envelope.seq == second.seq),
            Some(Delivery::Dropped(second.clone()))
        );
        assert_eq!(harness.network().dropped(), &[second]);

        let deliveries = harness.deliver_all();

        assert_eq!(
            deliveries,
            vec![
                Delivery::Delivered(first.clone()),
                Delivery::Delivered(third.clone())
            ]
        );
        assert_eq!(
            harness
                .node(bob())
                .expect("bob exists")
                .inbox()
                .iter()
                .map(|envelope| envelope.seq)
                .collect::<Vec<_>>(),
            vec![first.seq, third.seq]
        );
    }

    #[test]
    fn restart_brings_node_back_with_new_generation_and_empty_inbox() {
        let mut harness = Harness::new();
        harness.add_node(alice(), NodeRole::Client);
        harness.add_node(bob(), NodeRole::Replica);

        harness.send(alice(), bob(), b"before stop".to_vec());
        assert_eq!(
            harness.deliver_next(),
            Some(Delivery::Delivered(Envelope {
                seq: 0,
                from: alice(),
                to: bob(),
                payload: b"before stop".to_vec(),
            }))
        );
        assert_eq!(harness.node(bob()).expect("bob exists").inbox().len(), 1);

        assert!(harness.stop_node(bob()));
        let while_stopped = harness.send(alice(), bob(), b"while stopped".to_vec());
        assert_eq!(
            harness.deliver_next(),
            Some(Delivery::NodeStopped(while_stopped))
        );

        assert!(harness.restart_node(bob()));
        assert_eq!(harness.node(bob()).expect("bob exists").generation, 1);
        assert!(harness.node(bob()).expect("bob exists").running);
        assert!(harness.node(bob()).expect("bob exists").inbox().is_empty());

        let after_restart = harness.send(carol(), bob(), b"after restart".to_vec());
        assert_eq!(
            harness.deliver_next(),
            Some(Delivery::Delivered(after_restart.clone()))
        );
        assert_eq!(
            harness.node(bob()).expect("bob exists").inbox(),
            &[after_restart]
        );
    }

    #[test]
    fn trace_counts_deliveries_and_nodes() {
        let mut harness = Harness::new();
        harness.add_node(alice(), NodeRole::Client);
        harness.add_node(bob(), NodeRole::Replica);
        harness.add_node(carol(), NodeRole::Coordinator);

        harness.stop_node(carol());
        harness.send(alice(), bob(), b"create invoice".to_vec());
        harness.send(alice(), carol(), b"sync invoice".to_vec());
        harness.send(bob(), NodeId(99), b"notify missing device".to_vec());
        harness.send(alice(), bob(), b"discard stale cursor".to_vec());

        assert!(matches!(
            harness.deliver_next(),
            Some(Delivery::Delivered(_))
        ));
        assert!(matches!(
            harness.deliver_next(),
            Some(Delivery::NodeStopped(_))
        ));
        assert!(matches!(
            harness.deliver_next(),
            Some(Delivery::NoSuchNode(_))
        ));
        assert!(matches!(harness.drop_next(), Some(Delivery::Dropped(_))));

        assert_eq!(
            harness.trace(),
            HarnessTrace {
                deliveries: DeliveryCounters {
                    delivered: 1,
                    dropped: 1,
                    no_such_node: 1,
                    node_stopped: 1,
                },
                nodes: vec![
                    NodeCounters {
                        id: alice(),
                        role: NodeRole::Client,
                        generation: 0,
                        running: true,
                        inbox_len: 0,
                    },
                    NodeCounters {
                        id: bob(),
                        role: NodeRole::Replica,
                        generation: 0,
                        running: true,
                        inbox_len: 1,
                    },
                    NodeCounters {
                        id: carol(),
                        role: NodeRole::Coordinator,
                        generation: 0,
                        running: false,
                        inbox_len: 0,
                    },
                ],
                pending: 0,
                dropped: 1,
            }
        );
    }
}
