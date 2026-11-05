//! Bounded UUID-array membership at candidate and accepted cuts.
use super::*;

impl Ctx<'_> {
    pub(super) fn candidate_array_source(
        &mut self,
        table: &Table,
        include_created: bool,
        column: &str,
        value: &str,
    ) -> Result<String, Error> {
        if include_created || !table.branch_by.is_empty() {
            return Err(unsupported(
                "candidate-created or branched UUID-array permission dependencies",
            ));
        }
        let index = crate::array_index::name(table, column).ok_or_else(|| {
            unsupported("array permission dependencies without a leading declared UUID-array index")
        })?;
        let index = quote(&index);
        let current = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        let c = self.alias("array_current");
        let h = self.alias("array_history");
        let m = self.alias("array_member");
        let projection = |alias: &str, historical: bool| {
            let mut fields = vec![if historical {
                "NULL".to_owned()
            } else {
                format!("{alias}.rid")
            }];
            fields.extend(["id", "_b", "_seq", "_del"].map(|name| format!("{alias}.{name}")));
            fields.extend(
                table
                    .stored()
                    .map(|column| format!("{alias}.{}", quote(&column.stored))),
            );
            fields.join(", ")
        };
        let current_arm = |guard: &str| {
            format!(
                "SELECT {} FROM {index} {m} CROSS JOIN {current} {c} WHERE {m}.member={value} AND {m}.kind=0 AND {c}.id={m}.id AND {c}._b={m}.branch AND {c}._seq={m}.seq AND {guard}",
                projection(&c, false)
            )
        };
        let scope = self.permission_evidence.ok_or_else(|| {
            unsupported("array permission evidence without an authorization scope")
        })?;
        let arms = match scope {
            PermissionEvidence::Candidate(scope) => {
                let cut = self.bind(Sql::Integer(scope.accepted as i64));
                let publication = self.bind(Sql::Integer(scope.publication as i64));
                if scope.post {
                    let table_key = self.bind(Sql::Text(table.stored.as_bytes().to_vec()));
                    vec![current_arm(&format!(
                        "({c}._seq<={cut} OR {c}._seq={publication}) AND NOT EXISTS (SELECT 1 FROM _jazz_candidate_created_v1 x WHERE x.t={table_key} AND x.id={c}.id)"
                    ))]
                } else {
                    // Only this unit's modified rows can supersede its accepted
                    // cut. Drive history from that bounded publication index.
                    let sequence = quote(&format!("{}__by_seq", table.stored));
                    vec![
                        current_arm(&format!("{c}._seq<={cut}")),
                        format!(
                            "SELECT {} FROM {current} {c} INDEXED BY {sequence} CROSS JOIN {history} {h} WHERE {c}._seq={publication} AND {h}.id={c}.id AND {h}._b={c}._b AND {h}._seq=(SELECT MAX(prior._seq) FROM {history} prior WHERE prior.id={c}.id AND prior._b={c}._b AND prior._seq<={cut}) AND EXISTS (SELECT 1 FROM {index} {m} WHERE {m}.member={value} AND {m}.kind=1 AND {m}.id={h}.id AND {m}.branch={h}._b AND {m}.seq={h}._seq)",
                            projection(&h, true)
                        ),
                    ]
                }
            }
            PermissionEvidence::Accepted { through, own } => {
                let cut = self.bind(Sql::Integer(through as i64));
                let own = self.bind(Sql::Integer(own as i64));
                vec![
                    current_arm(&format!("({c}._seq<={cut} OR {c}._seq={own})")),
                    format!(
                        "SELECT {} FROM {index} {m} CROSS JOIN {history} {h} WHERE {m}.member={value} AND {m}.kind=1 AND {h}.id={m}.id AND {h}._b={m}.branch AND {h}._seq={m}.seq AND {h}._seq<={cut} AND NOT EXISTS (SELECT 1 FROM {current} {c} WHERE {c}.id={h}.id AND {c}._b={h}._b AND ({c}._seq<={cut} OR {c}._seq={own})) AND {h}._seq=(SELECT MAX(prior._seq) FROM {history} prior WHERE prior.id={h}.id AND prior._b={h}._b AND prior._seq<={cut})",
                        projection(&h, true)
                    ),
                ]
            }
        };
        Ok(format!("({})", arms.join(" UNION ALL ")))
    }
}
