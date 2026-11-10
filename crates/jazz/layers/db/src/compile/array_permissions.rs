//! Bounded UUID-array membership at candidate cuts.
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
                "CAST(NULL AS INTEGER)".to_owned()
            } else {
                format!("{alias}.rid")
            }];
            fields.extend(
                ["id", "_branch", "_clock", "_writer", "_state"]
                    .map(|name| format!("{alias}.{name}")),
            );
            fields.extend(
                table
                    .stored()
                    .map(|column| format!("{alias}.{}", quote(&column.stored))),
            );
            fields.join(", ")
        };
        let current_arm = |guard: &str| {
            format!(
                "SELECT {} FROM {index} {m} CROSS JOIN {current} {c} WHERE {m}.member={value} AND {m}.kind=0 AND {c}.id={m}.id AND {c}._branch={m}.branch AND {c}._clock={m}.clock AND {c}._writer={m}.writer AND {guard}",
                projection(&c, false)
            )
        };
        let scope = self.permission_evidence.ok_or_else(|| {
            unsupported("array permission evidence without an authorization scope")
        })?;
        let arms = match scope {
            PermissionEvidence::Candidate(scope) => {
                let cut = self.bind(Sql::Integer(scope.accepted.t()));
                let publication = self.candidate_publication(&c, scope.publication);
                if scope.post {
                    let table_key = self.bind(Sql::Text(table.stored.as_bytes().to_vec()));
                    vec![current_arm(&format!(
                        "({c}._clock<={cut} OR ({publication})) AND NOT EXISTS (SELECT 1 FROM _jazz_candidate_created_v1 x WHERE x.t={table_key} AND x.id={c}.id)"
                    ))]
                } else {
                    // Only this unit's modified rows can supersede its accepted
                    // cut. Drive history from that bounded publication index.
                    let clock_index = quote(&format!("{}__by_clock", table.stored));
                    vec![
                        current_arm(&format!("{c}._clock<={cut}")),
                        format!(
                            "SELECT {} FROM {current} {c} INDEXED BY {clock_index} CROSS JOIN {history} {h} WHERE ({publication}) AND {h}.id={c}.id AND {h}._branch={c}._branch AND ({h}._clock,{h}._writer)=(SELECT prior._clock,prior._writer FROM {history} prior WHERE prior.id={c}.id AND prior._branch={c}._branch AND prior._clock<={cut} AND (prior._state & {STATE_AS_SENT})=0 ORDER BY prior._clock DESC,prior._writer DESC LIMIT 1) AND EXISTS (SELECT 1 FROM {index} {m} WHERE {m}.member={value} AND {m}.kind=1 AND {m}.id={h}.id AND {m}.branch={h}._branch AND {m}.clock={h}._clock AND {m}.writer={h}._writer)",
                            projection(&h, true)
                        ),
                    ]
                }
            }
        };
        Ok(format!("({})", arms.join(" UNION ALL ")))
    }
}
