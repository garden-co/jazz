//! Retained UNION ALL: arm-local policies/windows, then global order/window.

use super::*;

impl Ctx<'_> {
    pub(super) fn compile_union(
        mut self,
        query: &Query,
        include_deleted: bool,
    ) -> Result<Compiled, Error> {
        use crate::query::{RelationQuery, compose_union_arm_path, relation_union_parts};
        crate::db::normalize_relation(query, self.schema)?;
        let relation = query.relation.as_ref().expect("retained relation");
        let mut parts = relation_union_parts(&relation.rel)
            .ok_or_else(|| unsupported("this retained relation shape on SQLite"))?;
        // Validate labels, output table and supported wrapper order before SQL.
        let resolved = crate::query::relation_query_to_query(relation)
            .map_err(|e| query_error(e.to_string()))?;
        if resolved.table != query.table {
            return Err(query_error("union output table differs from its envelope"));
        }
        if crate::schema::history::history_source_table(&query.table).is_some() {
            return Err(unsupported("union arms over history sources"));
        }
        if query.select.is_some() || !query.array_subqueries.is_empty() {
            return Err(unsupported("union selections or nested array output"));
        }
        crate::query::relation_union_leaf_projections(relation)
            .map_err(|e| query_error(e.to_string()))?;
        let table = self.table(&query.table)?;
        let projection = projection(table, None)?;
        let columns: Vec<String> = ["id", "_seq", "_del"]
            .into_iter()
            .map(str::to_owned)
            .chain(
                projection
                    .iter()
                    .map(|(name, _)| table.stored_name(name).to_owned()),
            )
            .collect();
        let mut arm_name = "_arm".to_owned();
        while columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(&arm_name))
        {
            arm_name.push('_');
        }
        let arm_column = quote(&arm_name);
        let mut ctes = Vec::new();
        let mut plain = Vec::new();
        let mut ranked = Vec::new();
        let mut union_arms = Vec::new();
        let mut any_ranked = false;
        // Slots are only SQL-local lookups. Semantic identity is always the
        // declared label, and tie order is independent of sibling declaration.
        parts.inputs.sort_by(|a, b| a.label.cmp(&b.label));
        for (index, arm) in parts.inputs.into_iter().enumerate() {
            let mut input = Query::from(query.table.clone());
            input.relation = Some(RelationQuery { rel: arm.input });
            let normalized = crate::db::normalize_relation(&input, self.schema)?;
            let input = normalized.as_ref().unwrap_or(&input);
            // Every arm uses the same context: claims, policies, schema view,
            // accepted cut and large-value preprocessing all remain in force.
            let compiled = self.clone().compile_query(input, include_deleted)?;
            if compiled.aggregate || compiled.joined != 0 || compiled.projection != projection {
                return Err(unsupported("union arms with different row projections"));
            }
            let label = if compiled.union_arms.is_empty() {
                let slot = union_arms.len();
                union_arms.push(arm.label);
                slot.to_string()
            } else {
                let mut cases = Vec::new();
                for (child, label) in compiled.union_arms.iter().enumerate() {
                    let slot = union_arms.len();
                    union_arms.push(compose_union_arm_path(&arm.label, label));
                    cases.push(format!("WHEN {child} THEN {slot}"));
                }
                format!("CASE {arm_column} {} END", cases.join(" "))
            };
            let mut names = columns.clone();
            if !compiled.union_arms.is_empty() {
                names.push(arm_name.clone());
            }
            let names = names
                .iter()
                .map(|s| quote(s))
                .collect::<Vec<_>>()
                .join(", ");
            let output = columns
                .iter()
                .map(|s| quote(s))
                .collect::<Vec<_>>()
                .join(", ");
            ctes.push(format!("u{index}({names}) AS ({})", compiled.sql));
            plain.push(format!(
                "SELECT {output}, {label} AS {arm_column} FROM u{index}"
            ));
            ranked.push(format!(
                "u{index}({names}) AS ({})",
                compiled.ranked_sql.as_ref().unwrap_or(&compiled.sql)
            ));
            any_ranked |= compiled.ranked_sql.is_some();
            self.params = compiled.params;
            self.tables = compiled.tables;
            self.preps = compiled.preps;
        }
        let order = parts
            .order_by
            .unwrap_or_default()
            .into_iter()
            .map(|term| {
                if term
                    .column
                    .scope
                    .as_deref()
                    .is_some_and(|scope| scope != query.table)
                {
                    return Err(query_error("union order must name its output table"));
                }
                if term.column.column != "id"
                    && !projection
                        .iter()
                        .any(|(name, _)| name == &term.column.column)
                {
                    return Err(unsupported("union ordering by unprojected metadata"));
                }
                Ok(crate::query::OrderBy {
                    column: term.column.column,
                    direction: term.direction,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let (order, ranked_order) = self.order_sql(table, "t", &order)?;
        any_ranked |= ranked_order.is_some();
        let limit = self.bind(Sql::Integer(
            parts
                .limit
                .map(i64::try_from)
                .transpose()
                .map_err(|_| query_error("union limit exceeds SQLite's range"))?
                .unwrap_or(-1),
        ));
        let offset = self.bind(Sql::Integer(
            i64::try_from(parts.offset.unwrap_or(0))
                .map_err(|_| query_error("union offset exceeds SQLite's range"))?,
        ));
        let compound = plain.join(" UNION ALL ");
        let statement = |ctes: &[String], order: &str| {
            format!(
                "WITH {} SELECT t.* FROM ({compound}) AS t ORDER BY {order}, t.{arm_column} ASC LIMIT {limit} OFFSET {offset}",
                ctes.join(", ")
            )
        };
        Ok(Compiled {
            sql: statement(&ctes, &order),
            ranked_sql: any_ranked
                .then(|| statement(&ranked, ranked_order.as_deref().unwrap_or(&order))),
            params: self.params,
            projection,
            tables: self.tables,
            aggregate: false,
            joined: 0,
            union_arms,
            preps: self.preps,
        })
    }
}
