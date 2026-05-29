use crate::schema::SchemaDef;
use crate::sync::{Bundle, BUNDLE_PROTOCOL_VERSION};
use crate::Result;
use std::collections::BTreeSet;

pub(crate) struct BundleApplyPlan {
    policy_tables: BTreeSet<String>,
    touched_tables: BTreeSet<String>,
}

impl BundleApplyPlan {
    pub(crate) fn validate(
        schema: &SchemaDef,
        bundle: &Bundle,
        check_policy_fingerprint: bool,
    ) -> Result<Self> {
        if bundle.protocol_version != BUNDLE_PROTOCOL_VERSION {
            return Err(crate::Error::new(format!(
                "unsupported bundle protocol version {}",
                bundle.protocol_version
            )));
        }
        let local_schema_fingerprint = schema.compatibility_fingerprint();
        if bundle.schema_fingerprint != "legacy"
            && bundle.schema_fingerprint != local_schema_fingerprint
        {
            return Err(crate::Error::new("incompatible schema fingerprint"));
        }

        let policy_tables = bundle_policy_tables(bundle);
        if check_policy_fingerprint {
            for table_name in &policy_tables {
                schema.table_def(table_name)?;
            }
            if !policy_tables.is_empty() {
                let local_policy_fingerprint =
                    schema.policy_fingerprint_for_tables(policy_tables.iter());
                if bundle.policy_fingerprint != "legacy"
                    && bundle.policy_fingerprint != local_policy_fingerprint
                {
                    return Err(crate::Error::new("incompatible policy fingerprint"));
                }
            }
        }

        Ok(Self {
            policy_tables,
            touched_tables: bundle_touched_tables(bundle),
        })
    }

    pub(crate) fn touched_tables(&self) -> &BTreeSet<String> {
        &self.touched_tables
    }

    #[allow(dead_code)]
    pub(crate) fn policy_tables(&self) -> &BTreeSet<String> {
        &self.policy_tables
    }
}

fn bundle_policy_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}

fn bundle_touched_tables(bundle: &Bundle) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for record in &bundle.history {
        tables.insert(record.table.clone());
    }
    for record in &bundle.reads {
        tables.insert(record.table.clone());
    }
    for query_read in &bundle.query_reads {
        tables.insert(query_read.table.clone());
    }
    tables
}
