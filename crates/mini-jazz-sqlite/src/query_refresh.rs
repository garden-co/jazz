use crate::observed_query::{self, ObservedQuery};
use crate::sync::QueryReadRecord;
use crate::Result;
use std::collections::BTreeMap;

type RecursiveRefreshKey = (String, String, String);

pub(crate) enum QueryRefreshPlan {
    RecursiveRefs {
        table: String,
        field: String,
        root_ids: Vec<String>,
    },
    Single(QueryReadRecord),
}

pub(crate) fn plan_refreshes(
    current_branch_id: &str,
    reads: &[QueryReadRecord],
) -> Result<Vec<QueryRefreshPlan>> {
    let mut recursive_groups: BTreeMap<RecursiveRefreshKey, Vec<String>> = BTreeMap::new();
    let mut singles = Vec::new();

    for read in reads {
        if read.branch_id != current_branch_id {
            singles.push(QueryRefreshPlan::Single(read.clone()));
            continue;
        }
        match observed_query::decode(read)? {
            ObservedQuery::RecursiveRefs { root_id } => {
                recursive_groups
                    .entry((
                        read.table.clone(),
                        read.field.clone(),
                        read.branch_id.clone(),
                    ))
                    .or_default()
                    .push(root_id);
            }
            ObservedQuery::Built { .. } | ObservedQuery::Absent => {
                singles.push(QueryRefreshPlan::Single(read.clone()));
            }
        }
    }

    let mut plans = Vec::new();
    plans.extend(
        recursive_groups
            .into_iter()
            .map(
                |((table, field, _branch), root_ids)| QueryRefreshPlan::RecursiveRefs {
                    table,
                    field,
                    root_ids,
                },
            ),
    );
    plans.extend(singles);
    Ok(plans)
}
