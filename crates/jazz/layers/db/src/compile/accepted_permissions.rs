//! Indexed permission dependencies at an arbitrary authority cut. A current
//! foreign key may have moved since that cut, so history has its own UUID index.
use super::*;

impl Ctx<'_> {
    pub(super) fn accepted_permission_arms(
        &mut self,
        table: &Table,
        column: &str,
        value: &str,
        alias: &str,
        through: u64,
        own: u64,
    ) -> Result<Vec<(String, String)>, Error> {
        if !table.branch_by.is_empty() {
            return Err(unsupported("branched accepted permission dependencies"));
        }
        let (current_index, history_index) = if column == "id" {
            (String::new(), String::new())
        } else {
            let index = table
                .indexes
                .iter()
                .find(|index| index.first().is_some_and(|first| first == column))
                .ok_or_else(|| {
                    unsupported(
                        "accepted permission dependencies without a leading declared UUID index",
                    )
                })?;
            let history = table.permission_history_index_name(column).ok_or_else(|| {
                unsupported(
                    "accepted permission dependencies without a leading declared UUID index",
                )
            })?;
            (
                format!(
                    " INDEXED BY {}",
                    quote(&format!(
                        "{}__ix({})",
                        table.stored,
                        table.stored_index(index).join(",")
                    ))
                ),
                format!(" INDEXED BY {}", quote(&history)),
            )
        };
        let current = quote(&table.stored);
        let history = quote(&format!("{}__h", table.stored));
        let key = quote(table.stored_name(column));
        let cut = self.bind(Sql::Integer(through as i64));
        let own = self.bind(Sql::Integer(own as i64));
        let newer = self.alias("permission_newer");
        let current_at_cut = format!(
            "NOT EXISTS (SELECT 1 FROM {current} {newer} WHERE {newer}.id={alias}.id AND {newer}._b={alias}._b AND ({newer}._seq<={cut} OR {newer}._seq={own}))"
        );
        Ok(vec![
            (
                format!("{current} AS {alias}{current_index}"),
                format!("{alias}.{key}={value} AND ({alias}._seq<={cut} OR {alias}._seq={own})"),
            ),
            (
                format!("{history} AS {alias}{history_index}"),
                format!(
                    "{alias}.{key}={value} AND {alias}._seq<={cut} AND {current_at_cut} AND {alias}._seq=(SELECT MAX({newer}._seq) FROM {history} {newer} WHERE {newer}.id={alias}.id AND {newer}._b={alias}._b AND {newer}._seq<={cut})"
                ),
            ),
        ])
    }
}
