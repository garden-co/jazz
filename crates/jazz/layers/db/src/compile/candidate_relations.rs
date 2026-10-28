//! Keep candidate relation lookups in the same SQL scope as their bound key.
//!
//! A derived UNION source cannot refer to a sibling join or recursive frontier
//! in SQLite. Distribute accepted current/history alternatives over the complete
//! SELECT instead. Every physical source retains its own indexed key predicate.
use super::*;

const MAX_RELATION_ARMS: usize = 64;

impl Ctx<'_> {
    /// Compile indexed dependency walks, including recursive seed/step sources.
    /// The caller supplies one captured root and meters every resulting query.
    pub(crate) fn candidate_relation_inputs(
        mut self,
        relation: &RelationExpr,
        outer: &Table,
        root: &[(String, Sql)],
        session: &Session,
    ) -> Result<(Vec<(String, String)>, Vec<Sql>), Error> {
        self.candidate_inputs = Some(Vec::new());
        let scope = RelScope {
            outer: Some((outer, "candidate_root")),
            frontier: None,
            session,
        };
        let rel = self.relation(relation, &scope)?;
        self.rel_selects(&rel, "1", None, &[])?;
        let columns = root
            .iter()
            .map(|(name, value)| {
                let value = self.bind(value.clone());
                format!("{value} AS {}", quote(name))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut ctes = vec![format!("candidate_root AS (SELECT {columns})")];
        ctes.extend(rel.ctes);
        let prefix = format!("WITH RECURSIVE {} ", ctes.join(", "));
        let inputs = self.candidate_inputs.take().expect("input collection");
        Ok((
            inputs
                .into_iter()
                .map(|(table, sql)| (table, format!("{prefix}{sql}")))
                .collect(),
            self.params,
        ))
    }

    pub(super) fn rel_bound(
        &self,
        rel: &Rel,
        column: &RelationColumnRef,
        value: String,
        dependency: Option<String>,
    ) -> Option<RelBound> {
        self.permission_evidence?;
        let src = self.resolve_src(rel, column).ok()?;
        let table = self.schema.table(src.table.as_ref()?).ok()?;
        let uuid = column.column == "id"
            || table.column(&column.column).is_some_and(|c| {
                let ty = match &c.ty {
                    ValueType::Nullable(inner) => inner.as_ref(),
                    ty => ty,
                };
                matches!(ty, ValueType::Uuid)
            });
        (uuid
            && (column.column == "id"
                || table
                    .indexes
                    .iter()
                    .any(|ix| ix.first() == Some(&column.column))))
        .then(|| RelBound {
            alias: src.alias.clone(),
            column: column.column.clone(),
            value,
            dependency,
        })
    }

    pub(super) fn rel_selects(
        &mut self,
        rel: &Rel,
        select: &str,
        prefix: Option<&str>,
        extra: &[String],
    ) -> Result<Vec<String>, Error> {
        let mut wheres = rel.wheres.clone();
        wheres.extend_from_slice(extra);
        let Some(evidence) = self.permission_evidence else {
            let from = match prefix {
                Some(p) => format!("{p}, {}", rel.sql_from_clause()),
                None => rel.sql_from_clause(),
            };
            return Ok(vec![format!(
                "SELECT {select} FROM {from} WHERE {}",
                and(&wheres)
            )]);
        };
        // A table becomes available only after its key's source. CROSS JOIN
        // fixes that order, including the sequence-driven historical arm.
        let input_prefix = self.candidate_inputs.as_ref().map(|_| match prefix {
            Some(prefix) => format!("candidate_root CROSS JOIN {prefix}"),
            None => "candidate_root".into(),
        });
        let prefix = input_prefix.as_deref().or(prefix);
        let mut remaining: Vec<_> = rel.from.iter().collect();
        let mut available = BTreeSet::new();
        let mut arms = vec![(
            prefix.map(str::to_owned).into_iter().collect::<Vec<_>>(),
            wheres,
        )];
        while !remaining.is_empty() {
            let choice = remaining.iter().enumerate().find_map(|(i, src)| {
                if src.table.is_none() {
                    return Some((i, None));
                }
                rel.bounds
                    .iter()
                    .find(|b| {
                        b.alias == src.alias
                            && b.dependency.as_ref().is_none_or(|d| available.contains(d))
                    })
                    .map(|b| (i, Some(b)))
            });
            let (i, bound) = choice.ok_or_else(|| unsupported(format!(
                "candidate relation source `{}` without a mandatory indexed UUID equality to a bound input", remaining[0].scope
            )))?;
            let src = remaining.remove(i);
            let alternatives = match bound {
                None => vec![(format!("{} AS {}", src.sql, src.alias), String::from("1"))],
                Some(bound) => self.rel_candidate_arms(src, bound, evidence)?,
            };
            if arms.len().saturating_mul(alternatives.len()) > MAX_RELATION_ARMS {
                return Err(unsupported(
                    "candidate relation exceeds its bounded SQL alternative limit",
                ));
            }
            let mut expanded = Vec::new();
            for (from, wheres) in arms {
                for (source, condition) in &alternatives {
                    let mut from = from.clone();
                    from.push(source.clone());
                    let mut wheres = wheres.clone();
                    wheres.push(condition.clone());
                    expanded.push((from, wheres));
                }
            }
            arms = expanded;
            available.insert(src.alias.clone());
        }
        if let Some(inputs) = &mut self.candidate_inputs {
            for src in &rel.from {
                let Some(table) = &src.table else { continue };
                let table = self.schema.table(table)?;
                for (from, wheres) in &arms {
                    inputs.push((
                        table.stored.clone(),
                        format!(
                            "SELECT {}.* FROM {} WHERE {}",
                            src.alias,
                            from.join(" CROSS JOIN "),
                            and(wheres),
                        ),
                    ));
                }
            }
        }
        Ok(arms
            .into_iter()
            .map(|(from, wheres)| {
                format!(
                    "SELECT {select} FROM {} WHERE {}",
                    from.join(" CROSS JOIN "),
                    and(&wheres)
                )
            })
            .collect())
    }

    fn rel_candidate_arms(
        &mut self,
        src: &Src,
        bound: &RelBound,
        evidence: PermissionEvidence,
    ) -> Result<Vec<(String, String)>, Error> {
        let table = self
            .schema
            .table(src.table.as_ref().expect("physical source"))?;
        let PermissionEvidence::Candidate(evidence) = evidence;
        if self.view.is_some() && !table.branch_by.is_empty() {
            return Err(unsupported(
                "branch-view relation dependencies in candidate authorization units",
            ));
        }
        let lookup = if bound.column == "id" {
            String::new()
        } else {
            let index = table
                .indexes
                .iter()
                .find(|ix| ix.first() == Some(&bound.column))
                .filter(|_| table.branch_by.is_empty())
                .ok_or_else(|| {
                    unsupported("candidate relation correlations without a leading declared index")
                })?;
            format!(
                " INDEXED BY {}",
                quote(&format!(
                    "{}__ix({})",
                    table.stored,
                    table.stored_index(index).join(",")
                ))
            )
        };
        let alias = &src.alias;
        let current = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        let key = quote(table.stored_name(&bound.column));
        let value = &bound.value;
        let accepted = self.bind(Sql::Integer(evidence.accepted as i64));
        let publication = self.bind(Sql::Integer(evidence.publication as i64));
        let current_from = format!("{current} AS {alias}{lookup}");
        if evidence.post {
            let stored = self.bind(Sql::Text(table.stored.as_bytes().to_vec()));
            return Ok(vec![(
                current_from,
                format!(
                    "{alias}.{key}={value} AND ({alias}._seq<={accepted} OR {alias}._seq={publication}) AND NOT EXISTS (SELECT 1 FROM _jazz_candidate_created_v1 x WHERE x.t={stored} AND x.id={alias}.id)"
                ),
            )]);
        }
        let changed = self.alias("rel_changed");
        let seq_index = quote(&format!("{}__by_seq", table.stored));
        Ok(vec![
            (
                current_from,
                format!("{alias}.{key}={value} AND {alias}._seq<={accepted}"),
            ),
            (
                format!(
                    "{current} AS {changed} INDEXED BY {seq_index} CROSS JOIN {history} AS {alias}"
                ),
                format!(
                    "{changed}._seq={publication} AND {alias}.id={changed}.id AND {alias}._b={changed}._b AND {alias}.{key}={value} AND {alias}._seq=(SELECT MAX(prior._seq) FROM {history} AS prior WHERE prior.id={changed}.id AND prior._b={changed}._b AND prior._seq<={accepted})"
                ),
            ),
        ])
    }
}
