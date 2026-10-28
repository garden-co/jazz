//! Mutable authority policies are separate from immutable structural identities.
use super::*;
use serde::{Deserialize, Serialize};

/// `jazz.sqlite.policy-descriptor.v1`: the authority's active policies, bound to
/// an exact catalogue namespace and revision. The schema UUID excludes policies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyDescriptorV1 {
    pub head: CatalogueHeadV1,
    pub schema: SchemaVersion,
}

pub(super) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_policy_descriptor_v1 (slot INTEGER PRIMARY KEY CHECK(slot=1), value BLOB NOT NULL);";

fn invalid(message: &str) -> Error {
    Error::new(ErrorCode::Protocol, message)
}

impl Inner {
    fn stored_policy_descriptor(&self) -> Result<Option<PolicyDescriptorV1>, Error> {
        let bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT value FROM _jazz_policy_descriptor_v1 WHERE slot=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        bytes
            .map(|bytes| {
                let (descriptor, rest) = postcard::take_from_bytes::<PolicyDescriptorV1>(&bytes)
                    .map_err(|e| {
                        Error::new(
                            ErrorCode::Storage,
                            format!("invalid policy-descriptor-v1: {e}"),
                        )
                    })?;
                if !rest.is_empty() {
                    return Err(Error::new(
                        ErrorCode::Storage,
                        "trailing policy-descriptor-v1 bytes",
                    ));
                }
                self.validate_policy_descriptor(&descriptor)
                    .map_err(|e| Error::new(ErrorCode::Storage, e.message))?;
                Ok(descriptor)
            })
            .transpose()
    }

    fn validate_policy_descriptor(&self, descriptor: &PolicyDescriptorV1) -> Result<(), Error> {
        if descriptor.head.root == [0; 32]
            || descriptor.head.current != descriptor.schema.id
            || descriptor.schema.id != descriptor.schema.schema.version_id()
        {
            return Err(invalid("policy descriptor identity mismatch"));
        }
        let structural = self
            .catalogue_schema(descriptor.schema.id)?
            .ok_or_else(|| invalid("policy descriptor lacks accepted schema identities"))?;
        if !structural
            .schema
            .schema
            .has_same_lineage_structure(&descriptor.schema.schema)
        {
            return Err(invalid(
                "policy descriptor differs from accepted schema structure",
            ));
        }
        Ok(())
    }

    /// Called within the authority catalogue publication savepoint.
    pub(super) fn record_policy_descriptor(
        &self,
        descriptor: &PolicyDescriptorV1,
    ) -> Result<(), Error> {
        self.validate_policy_descriptor(descriptor)?;
        if self.catalogue_head()?.as_ref() != Some(&descriptor.head) {
            return Err(invalid(
                "policy descriptor does not match accepted catalogue head",
            ));
        }
        if let Some(previous) = self.stored_policy_descriptor()? {
            if previous.head.root != descriptor.head.root
                || previous.head.revision > descriptor.head.revision
                || previous.head.through > descriptor.head.through
            {
                return Err(invalid(
                    "policy descriptor moved to another namespace or backwards",
                ));
            }
            if previous.head.revision == descriptor.head.revision
                && (previous.schema.id != descriptor.schema.id
                    || previous
                        .schema
                        .schema
                        .with_permissions_from(&descriptor.schema.schema)
                        != previous.schema.schema)
            {
                return Err(invalid("conflicting policies at one catalogue revision"));
            }
        }
        let bytes = postcard::to_allocvec(descriptor)
            .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?;
        self.conn.execute(
            "INSERT INTO _jazz_policy_descriptor_v1(slot,value) VALUES(1,?1) ON CONFLICT(slot) DO UPDATE SET value=excluded.value",
            [bytes],
        ).map_err(storage)?;
        Ok(())
    }
}
