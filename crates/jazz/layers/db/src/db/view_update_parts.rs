//! Bounded parts for a `ViewUpdate` larger than the routed payload limit.
//!
//! A subscription's update is atomic: the receiver installs its supporting-set
//! transition in one step. The routed per-message payload limit is a transport
//! bound, not a semantic one, so an update that exceeds it is sent as a
//! sequence of bounded parts on its subscription's delivery stream. Every part
//! but the last is a [`SyncMessage::ViewUpdatePart`]; the last is the ordinary
//! [`SyncMessage::ViewUpdate`]. The receiving transport buffers the parts and
//! yields only the reassembled update, so nothing above the transport ever
//! sees a partial one.
//!
//! Each part repeats the update's header (subscription, `settled_through`,
//! transition kind and revisions) and carries a slice of its supporting-row
//! lists and of its version bundles. Only the final part carries the real
//! `peer_payload_inventory`. The split is at row and bundle granularity: one
//! supporting row or one transaction bundle that alone exceeds a part's
//! capacity is refused with [`OversizedViewUpdate`] rather than worked around.

use std::collections::BTreeSet;

use crate::protocol::{
    PeerPayloadInventory, SupportingRow, SupportingRowsUpdate, SyncMessage, VersionBundle,
    VersionCarrier, ViewUpdatePayload,
};

/// Headroom for the list-length prefixes of a part, which the header
/// measurement (taken with empty lists) does not include.
const PART_LENGTH_PREFIX_SLACK: usize = 64;

/// A `ViewUpdate` that cannot be sent as bounded parts. Splitting never divides
/// one supporting row or one transaction bundle, so an indivisible unit larger
/// than a part's capacity is unsupported rather than silently degraded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OversizedViewUpdate {
    /// The update header alone (subscription, inventory, revisions) exceeds
    /// the routed payload limit.
    HeaderExceedsLimit {
        /// Encoded header bytes, including list-prefix headroom.
        header_bytes: usize,
        /// Routed payload limit.
        limit: usize,
    },
    /// One supporting-row reference exceeds a part's capacity.
    SupportingRowExceedsLimit {
        /// Encoded row bytes.
        row_bytes: usize,
        /// Bytes a part has for rows and bundles after its header.
        capacity: usize,
    },
    /// One transaction's version bundle (its row bodies) exceeds a part's
    /// capacity.
    VersionBundleExceedsLimit {
        /// Encoded bundle bytes.
        bundle_bytes: usize,
        /// Bytes a part has for rows and bundles after its header.
        capacity: usize,
    },
    /// A packed version-carrier run is malformed and cannot be divided.
    MalformedCarrier,
}

impl std::fmt::Display for OversizedViewUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HeaderExceedsLimit {
                header_bytes,
                limit,
            } => write!(
                f,
                "unsupported view update: its {header_bytes}-byte header exceeds the \
                 {limit}-byte routed payload limit"
            ),
            Self::SupportingRowExceedsLimit {
                row_bytes,
                capacity,
            } => write!(
                f,
                "unsupported view update: one {row_bytes}-byte supporting row exceeds the \
                 {capacity}-byte part capacity"
            ),
            Self::VersionBundleExceedsLimit {
                bundle_bytes,
                capacity,
            } => write!(
                f,
                "unsupported view update: one {bundle_bytes}-byte transaction bundle exceeds \
                 the {capacity}-byte part capacity"
            ),
            Self::MalformedCarrier => {
                write!(f, "unsupported view update: malformed version-carrier run")
            }
        }
    }
}

impl std::error::Error for OversizedViewUpdate {}

/// The two supporting-row lists of a transition, in declaration order:
/// snapshot rows (and nothing), delta adds and removes, catch-up changed and
/// left. The returned template keeps kind and revisions with empty lists.
fn take_lists(
    mut rows: SupportingRowsUpdate,
) -> (SupportingRowsUpdate, Vec<SupportingRow>, Vec<SupportingRow>) {
    let (first, second) = match &mut rows {
        SupportingRowsUpdate::Snapshot { rows, .. } => (std::mem::take(rows), Vec::new()),
        SupportingRowsUpdate::Delta { adds, removes, .. } => {
            (std::mem::take(adds), std::mem::take(removes))
        }
        SupportingRowsUpdate::CatchUp { changed, left, .. } => {
            (std::mem::take(changed), std::mem::take(left))
        }
    };
    (rows, first, second)
}

/// Fill a template's lists. A snapshot has no second list.
fn with_lists(
    template: &SupportingRowsUpdate,
    first: Vec<SupportingRow>,
    second: Vec<SupportingRow>,
) -> SupportingRowsUpdate {
    match template {
        SupportingRowsUpdate::Snapshot { revision, .. } => {
            debug_assert!(second.is_empty(), "a snapshot has one row list");
            SupportingRowsUpdate::Snapshot {
                revision: *revision,
                rows: first,
            }
        }
        SupportingRowsUpdate::Delta {
            predecessor,
            revision,
            ..
        } => SupportingRowsUpdate::Delta {
            predecessor: *predecessor,
            revision: *revision,
            adds: first,
            removes: second,
        },
        SupportingRowsUpdate::CatchUp {
            predecessor,
            revision,
            ..
        } => SupportingRowsUpdate::CatchUp {
            predecessor: *predecessor,
            revision: *revision,
            changed: first,
            left: second,
        },
    }
}

/// Kind and revisions of a transition, ignoring its lists: every part of one
/// update must agree on it.
fn transition_identity(rows: &SupportingRowsUpdate) -> (u8, [u8; 16], [u8; 16]) {
    match rows {
        SupportingRowsUpdate::Snapshot { revision, .. } => (0, [0; 16], *revision),
        SupportingRowsUpdate::Delta {
            predecessor,
            revision,
            ..
        } => (1, *predecessor, *revision),
        SupportingRowsUpdate::CatchUp {
            predecessor,
            revision,
            ..
        } => (2, *predecessor, *revision),
    }
}

fn encoded_len(message: &SyncMessage) -> usize {
    crate::wire::encoded_sync_message_len(message).unwrap_or(usize::MAX)
}

fn serialized_size<T: serde::Serialize>(value: &T) -> usize {
    postcard::experimental::serialized_size(value).unwrap_or(usize::MAX)
}

#[derive(Default)]
struct PartContents {
    first: Vec<SupportingRow>,
    second: Vec<SupportingRow>,
    bundles: Vec<VersionBundle>,
    bytes: usize,
}

impl PartContents {
    fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty() && self.bundles.is_empty()
    }
}

/// Split `view` into parts whose encodings each fit `limit` bytes.
///
/// Returns the parts in send order: `ViewUpdatePart`s followed by one final
/// `ViewUpdate` that carries the real payload inventory. Rows keep their list
/// order and bundles keep their carrier order, so concatenating the parts
/// reproduces the update. Callers split only an update whose encoding exceeds
/// `limit`; one that already fits comes back as its single final part.
pub fn split_view_update(
    view: ViewUpdatePayload,
    limit: usize,
) -> Result<Vec<SyncMessage>, OversizedViewUpdate> {
    let ViewUpdatePayload {
        subscription,
        settled_through,
        version_carriers,
        peer_payload_inventory,
        supporting_rows,
    } = view;
    let bundles = crate::protocol::expand_version_carriers(&version_carriers)
        .map_err(|_| OversizedViewUpdate::MalformedCarrier)?;
    drop(version_carriers);
    let (template, first, second) = take_lists(supporting_rows);
    let shell = |inventory: PeerPayloadInventory,
                 first: Vec<SupportingRow>,
                 second: Vec<SupportingRow>,
                 carriers: Vec<VersionCarrier>| ViewUpdatePayload {
        subscription,
        settled_through,
        version_carriers: carriers,
        peer_payload_inventory: inventory,
        supporting_rows: with_lists(&template, first, second),
    };
    // The final part's header is the largest (it carries the inventory), so
    // it bounds every part's header.
    let header_bytes = encoded_len(&SyncMessage::ViewUpdate(shell(
        peer_payload_inventory.clone(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )))
    .saturating_add(PART_LENGTH_PREFIX_SLACK);
    if header_bytes >= limit {
        return Err(OversizedViewUpdate::HeaderExceedsLimit {
            header_bytes,
            limit,
        });
    }
    let capacity = limit - header_bytes;

    let mut parts = Vec::<PartContents>::new();
    let mut current = PartContents::default();
    let mut admit = |current: &mut PartContents, bytes: usize| {
        if !current.is_empty() && current.bytes.saturating_add(bytes) > capacity {
            parts.push(std::mem::take(current));
        }
        current.bytes = current.bytes.saturating_add(bytes);
    };
    for row in first {
        let row_bytes = serialized_size(&row);
        if row_bytes > capacity {
            return Err(OversizedViewUpdate::SupportingRowExceedsLimit {
                row_bytes,
                capacity,
            });
        }
        admit(&mut current, row_bytes);
        current.first.push(row);
    }
    for row in second {
        let row_bytes = serialized_size(&row);
        if row_bytes > capacity {
            return Err(OversizedViewUpdate::SupportingRowExceedsLimit {
                row_bytes,
                capacity,
            });
        }
        admit(&mut current, row_bytes);
        current.second.push(row);
    }
    for bundle in bundles {
        // A singleton carrier is the enum tag plus the bundle.
        let bundle_bytes = serialized_size(&bundle).saturating_add(1);
        if bundle_bytes > capacity {
            return Err(OversizedViewUpdate::VersionBundleExceedsLimit {
                bundle_bytes,
                capacity,
            });
        }
        admit(&mut current, bundle_bytes);
        current.bundles.push(bundle);
    }
    parts.push(current);

    let count = parts.len();
    let mut messages = Vec::with_capacity(count);
    for (index, part) in parts.into_iter().enumerate() {
        let last = index + 1 == count;
        let inventory = if last {
            peer_payload_inventory.clone()
        } else {
            PeerPayloadInventory::default()
        };
        let wrap = |payload: ViewUpdatePayload| {
            if last {
                SyncMessage::ViewUpdate(payload)
            } else {
                SyncMessage::ViewUpdatePart(payload)
            }
        };
        // A packed run is usually smaller than its singletons, but its
        // per-body overrides can make it slightly larger; singletons are what
        // the capacity accounting measured, so they always fit.
        let packed = crate::protocol::build_version_carriers_from_singletons(part.bundles.clone())
            .ok()
            .map(|carriers| {
                wrap(shell(
                    inventory.clone(),
                    part.first.clone(),
                    part.second.clone(),
                    carriers,
                ))
            })
            .filter(|message| encoded_len(message) <= limit);
        let message = packed.unwrap_or_else(|| {
            wrap(shell(
                inventory,
                part.first,
                part.second,
                part.bundles
                    .into_iter()
                    .map(VersionCarrier::Bundle)
                    .collect(),
            ))
        });
        debug_assert!(encoded_len(&message) <= limit, "a view-update part fits");
        messages.push(message);
    }
    Ok(messages)
}

/// Reassemble buffered `ViewUpdatePart`s and their final `ViewUpdate`.
///
/// Every part must name the final update's subscription, `settled_through`,
/// transition kind and revisions. Rows and carriers concatenate in arrival
/// order, and the final part's inventory is the update's. A supporting row
/// repeated across parts is rejected, as it would be within one update.
pub fn merge_view_update_parts(
    parts: Vec<ViewUpdatePayload>,
    last: ViewUpdatePayload,
) -> Result<ViewUpdatePayload, String> {
    let identity = transition_identity(&last.supporting_rows);
    let mut first = Vec::new();
    let mut second = Vec::new();
    let mut carriers = Vec::new();
    let ViewUpdatePayload {
        subscription,
        settled_through,
        version_carriers: last_carriers,
        peer_payload_inventory,
        supporting_rows: last_rows,
    } = last;
    for part in parts {
        if part.subscription != subscription
            || part.settled_through != settled_through
            || transition_identity(&part.supporting_rows) != identity
        {
            return Err("view-update part does not match its final update".into());
        }
        let (_, part_first, part_second) = take_lists(part.supporting_rows);
        first.extend(part_first);
        second.extend(part_second);
        carriers.extend(part.version_carriers);
    }
    let (template, last_first, last_second) = take_lists(last_rows);
    first.extend(last_first);
    second.extend(last_second);
    carriers.extend(last_carriers);
    let mut identities = BTreeSet::new();
    if !first
        .iter()
        .chain(&second)
        .all(|row| identities.insert(row))
    {
        return Err("view-update parts repeat a supporting row".into());
    }
    drop(identities);
    Ok(ViewUpdatePayload {
        subscription,
        settled_through,
        version_carriers: carriers,
        peer_payload_inventory,
        supporting_rows: with_lists(&template, first, second),
    })
}
