/// Frozen local V1 tags: Seal=0, Publish=1, Cancel=2, RecordAbsence=3,
/// Status=4, HasAuthenticatedCatalogue=5.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InitializationAction {
    Seal {
        transaction: u64,
    },
    Publish {
        token: String,
    },
    Cancel {
        token: String,
    },
    RecordAbsence {
        transaction: u64,
        table: String,
        row: [u8; 16],
    },
    Status {
        ids: Vec<String>,
    },
    /// Admitted host-cache readiness, not local schema bootstrap. Contention
    /// returns a foreground pending operation; true requires durable capture
    /// publication. Generic relay handles without a cache owner are unsupported.
    HasAuthenticatedCatalogue,
}
