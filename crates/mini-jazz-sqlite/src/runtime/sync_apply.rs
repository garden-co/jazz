use super::*;

impl Runtime {
    pub fn apply_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        self.apply_bundle_inner(bundle, true).map(|_| ())
    }

    pub fn profile_apply_bundle(&mut self, bundle: &Bundle) -> Result<ApplyBundleProfile> {
        self.apply_bundle_inner(bundle, true)
    }

    fn apply_bundle_inner(
        &mut self,
        bundle: &Bundle,
        check_policy_fingerprint: bool,
    ) -> Result<ApplyBundleProfile> {
        let total_started = ProfileTimer::start();
        let validation_started = ProfileTimer::start();
        let apply_plan = BundleApplyPlan::validate(&self.schema, bundle, check_policy_fingerprint)?;
        let validation_ms = validation_started.elapsed_ms();
        let schema = self.schema.clone();
        let repair_user = self.policy_user().to_owned();
        let repair_bypass_policy = self.bypasses_policy();
        let begin_tx_started = ProfileTimer::start();
        let db = self.conn.transaction()?;
        let begin_tx_ms = begin_tx_started.elapsed_ms();

        let branches_started = ProfileTimer::start();
        let branch_nums_by_id = apply_branch_records(&db, bundle)?;
        let branches_ms = branches_started.elapsed_ms();

        let table_nums_by_name = crate::schema::table_nums(&db)?;

        let txs_started = ProfileTimer::start();
        let applied_txs = apply_tx_records(&db, bundle)?;
        let txs_ms = txs_started.elapsed_ms();

        let reads_started = ProfileTimer::start();
        let mut apply_caches = ApplyCaches::default();
        apply_read_records(
            &db,
            bundle,
            &applied_txs,
            &table_nums_by_name,
            &mut apply_caches,
        )?;
        let reads_ms = reads_started.elapsed_ms();

        let rejected_cleanup_started = ProfileTimer::start();
        if bundle
            .txs
            .iter()
            .any(|tx| tx.outcome == tx::OUTCOME_REJECTED)
        {
            for table_name in apply_plan.touched_tables() {
                schema.table_def(table_name)?;
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE visible_tx_num IN (
                           SELECT tx_num FROM jazz_tx WHERE outcome = ?
                         )",
                        crate::schema::current_table(table_name)
                    ),
                    params![tx::OUTCOME_REJECTED],
                )?;
            }
        }
        let rejected_cleanup_ms = rejected_cleanup_started.elapsed_ms();

        let query_reads_started = ProfileTimer::start();
        apply_query_read_records(&db, bundle)?;
        let query_reads_ms = query_reads_started.elapsed_ms();

        let history_started = ProfileTimer::start();
        let mut history_context = ApplyHistoryContext {
            schema: &schema,
            db: &db,
            local_node_num: self.node_num,
            tx_nums_by_id: &applied_txs.tx_nums_by_id,
            tx_info_by_num: &applied_txs.tx_info_by_num,
            branch_nums_by_id: &branch_nums_by_id,
            table_nums_by_name: &table_nums_by_name,
            apply_caches: &mut apply_caches,
        };
        for record in &bundle.history {
            Self::apply_history_record(&mut history_context, record)?;
        }
        let history_ms = history_started.elapsed_ms();

        let query_scope_repair_started = ProfileTimer::start();
        for query_read in &bundle.query_reads {
            Self::apply_query_scope_repair(
                &schema,
                &db,
                query_read,
                &repair_user,
                repair_bypass_policy,
            )?;
        }
        let query_scope_repair_ms = query_scope_repair_started.elapsed_ms();

        let commit_started = ProfileTimer::start();
        db.commit()?;
        let commit_ms = commit_started.elapsed_ms();

        let revalidate_started = ProfileTimer::start();
        self.revalidate_awaiting_dependencies()?;
        let revalidate_awaiting_ms = revalidate_started.elapsed_ms();

        Ok(ApplyBundleProfile {
            total_ms: total_started.elapsed_ms(),
            validation_ms,
            begin_tx_ms,
            branches_ms,
            txs_ms,
            reads_ms,
            rejected_cleanup_ms,
            query_reads_ms,
            history_ms,
            query_scope_repair_ms,
            commit_ms,
            revalidate_awaiting_ms,
            branch_rows: bundle.branches.len(),
            tx_rows: bundle.txs.len(),
            read_rows: bundle.reads.len(),
            query_read_rows: bundle.query_reads.len(),
            history_rows: bundle.history.len(),
        })
    }

    pub fn apply_untrusted_bundle(&mut self, bundle: &Bundle) -> Result<()> {
        self.apply_untrusted_bundle_with_auth_user(bundle, None)
    }

    pub fn apply_untrusted_bundle_as_user(&mut self, bundle: &Bundle, user: &str) -> Result<()> {
        self.apply_untrusted_bundle_with_auth_user(bundle, Some(user))
    }

    pub fn stage_exclusive_bundle_for_forwarding(&mut self, bundle: &Bundle) -> Result<()> {
        for tx_record in &bundle.txs {
            if tx_record.conflict_mode == tx::MODE_EXCLUSIVE
                && tx_record.outcome == tx::OUTCOME_PENDING
                && tx_record.auth_user.is_none()
            {
                return Err(crate::Error::new(format!(
                    "exclusive transaction {} is missing forwarded auth user",
                    tx_record.tx_id
                )));
            }
        }
        self.apply_bundle_inner(bundle, false)?;
        projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        Ok(())
    }

    fn apply_untrusted_bundle_with_auth_user(
        &mut self,
        bundle: &Bundle,
        connection_auth_user: Option<&str>,
    ) -> Result<()> {
        let stale_exclusive_tx_ids =
            read_set::stale_exclusive_tx_ids_in_bundle(&self.conn, bundle)?;
        let forwarded_auth_users = bundle
            .txs
            .iter()
            .filter(|tx| tx.conflict_mode == tx::MODE_EXCLUSIVE)
            .filter_map(|tx| {
                tx.auth_user
                    .as_deref()
                    .map(|user| (tx.tx_id.as_str(), user))
            })
            .collect::<BTreeMap<_, _>>();
        self.apply_bundle_inner(bundle, false)?;
        let mut rejected = BTreeSet::new();
        let mut exclusive_to_accept = BTreeSet::new();
        for tx_id in stale_exclusive_tx_ids {
            self.reject_transaction_with_detail(
                &tx_id,
                "stale_read_set",
                json!({
                    "reason": "exclusive_read_dependency_changed",
                }),
            )?;
            rejected.insert(tx_id);
        }
        for record in &bundle.history {
            if rejected.contains(&record.tx_id) {
                continue;
            }
            let tx_num = tx::tx_num(&self.conn, &record.tx_id)?;
            if tx_outcome(&self.conn, tx_num)? != tx::OUTCOME_PENDING {
                continue;
            }
            let conflict_mode = tx_conflict_mode(&self.conn, tx_num)?;
            if conflict_mode == tx::MODE_EXCLUSIVE {
                if !forwarded_auth_users.contains_key(record.tx_id.as_str()) {
                    self.reject_transaction_with_detail(
                        &record.tx_id,
                        "policy_denied",
                        json!({
                            "reason": "missing_auth_user",
                        }),
                    )?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
                if read_set::tx_read_set_is_stale(&self.conn, tx_num, &record.branch_id)? {
                    self.reject_transaction_with_detail(
                        &record.tx_id,
                        "stale_read_set",
                        json!({
                            "reason": "exclusive_read_dependency_changed",
                        }),
                    )?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
            }
            let table = self.schema.table_def(&record.table)?;
            let row_num = ensure_row_id(&self.conn, &record.table, &record.row_id)?;
            let auth_user = if conflict_mode == tx::MODE_EXCLUSIVE {
                forwarded_auth_users.get(record.tx_id.as_str()).copied()
            } else {
                connection_auth_user
            };
            if auth_user.is_none() {
                self.reject_transaction_with_detail(
                    &record.tx_id,
                    "policy_denied",
                    json!({
                        "reason": "missing_auth_user",
                    }),
                )?;
                rejected.insert(record.tx_id.clone());
                continue;
            }
            let auth_user = auth_user.expect("auth user checked above");
            let allowed = write_allowed_for_history_record(
                &self.conn,
                &self.schema,
                table,
                row_num,
                record,
                Some(auth_user),
            )?;
            if !allowed {
                let detail =
                    policy_denial_detail_for_history_record(&self.conn, table, record, tx_num)?;
                if is_policy_dependency_unavailable(&detail) {
                    if conflict_mode == tx::MODE_EXCLUSIVE {
                        self.reject_transaction_with_detail(
                            &record.tx_id,
                            "policy_denied",
                            detail,
                        )?;
                        rejected.insert(record.tx_id.clone());
                        continue;
                    }
                    mark_transaction_awaiting_dependency(&self.conn, tx_num, auth_user, &detail)?;
                    remove_current_for_awaiting_dependency(&self.conn, record, row_num)?;
                    rejected.insert(record.tx_id.clone());
                    continue;
                }
                self.reject_transaction_with_detail(&record.tx_id, "policy_denied", detail)?;
                rejected.insert(record.tx_id.clone());
            } else {
                clear_transaction_awaiting_dependency(&self.conn, tx_num)?;
                if conflict_mode == tx::MODE_EXCLUSIVE {
                    exclusive_to_accept.insert(record.tx_id.clone());
                }
            }
        }
        let mut accepted_exclusive = false;
        for tx_id in exclusive_to_accept {
            let tx_num = tx::tx_num(&self.conn, &tx_id)?;
            if !rejected.contains(&tx_id) && tx_outcome(&self.conn, tx_num)? == tx::OUTCOME_PENDING
            {
                tx::accept_global(&self.conn, &tx_id, next_global_epoch(&self.conn)?)?;
                accepted_exclusive = true;
            }
        }
        if !rejected.is_empty() || accepted_exclusive {
            projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        }
        self.revalidate_awaiting_dependencies()?;
        Ok(())
    }

    fn revalidate_awaiting_dependencies(&mut self) -> Result<()> {
        let awaiting = awaiting_dependency_transactions(&self.conn)?;
        let mut changed = false;
        for awaiting in awaiting {
            if tx_outcome(&self.conn, awaiting.tx_num)? != tx::OUTCOME_PENDING {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                changed = true;
                continue;
            }
            let records =
                history_records_for_tx(&self.conn, &self.schema, awaiting.tx_num, &awaiting.tx_id)?;
            if records.is_empty() {
                continue;
            }
            let mut still_waiting = None;
            let mut denied = None;
            for record in records {
                let table = self.schema.table_def(&record.table)?;
                let row_num = row_num(&self.conn, &record.row_id)?;
                let allowed = write_allowed_for_history_record(
                    &self.conn,
                    &self.schema,
                    table,
                    row_num,
                    &record,
                    Some(awaiting.auth_user.as_str()),
                )?;
                if !allowed {
                    let detail = policy_denial_detail_for_history_record(
                        &self.conn,
                        table,
                        &record,
                        awaiting.tx_num,
                    )?;
                    if is_policy_dependency_unavailable(&detail) {
                        still_waiting = Some(detail);
                    } else {
                        denied = Some(detail);
                    }
                    break;
                }
            }
            if let Some(detail) = denied {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                tx::reject_with_detail_json(
                    &self.conn,
                    &awaiting.tx_id,
                    "policy_denied",
                    &serde_json::to_string(&detail)
                        .map_err(|err| crate::Error::new(err.to_string()))?,
                )?;
                changed = true;
            } else if let Some(detail) = still_waiting {
                mark_transaction_awaiting_dependency(
                    &self.conn,
                    awaiting.tx_num,
                    &awaiting.auth_user,
                    &detail,
                )?;
            } else {
                clear_transaction_awaiting_dependency(&self.conn, awaiting.tx_num)?;
                if tx_conflict_mode(&self.conn, awaiting.tx_num)? == tx::MODE_MERGEABLE {
                    tx::accept_edge(&self.conn, &awaiting.tx_id, now_ms())?;
                }
                changed = true;
            }
        }
        if changed {
            projection::rebuild(&self.conn, &self.schema, self.node_num)?;
        }
        Ok(())
    }
    fn apply_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Query-scope repair keeps a receiver's current projection from retaining
        // rows that used to satisfy an observed query but are no longer covered by
        // the refresh bundle.
        //
        // Export side:
        //
        //   +------------------+      +---------------------+
        //   | current results  | ---> | visible row history |
        //   +------------------+      +---------------------+
        //            |                         ^
        //            v                         |
        //   +------------------+      +---------------------+
        //   | repair row nums  | ---> | old matching rows   |
        //   +------------------+      +---------------------+
        //
        // Apply side:
        //
        //   +-------------------+      +----------------------+
        //   | local current row | ---> | still has matching   |
        //   | matches read      |      | history in bundle?   |
        //   +-------------------+      +----------------------+
        //             | yes                     | no
        //             v                         v
        //        keep current             delete current
        //
        // `apply_bundle` runs this both before and after applying incoming
        // history. The first pass can remove stale rows using repair history
        // already present locally; the second pass observes repair history that
        // arrived in the bundle and catches page-boundary changes after current
        // projection updates.
        if query_read.op == "absent" {
            let table = schema.table_def(&query_read.table)?;
            if query_read.field != "id"
                && !table
                    .fields
                    .iter()
                    .any(|field| field.name == query_read.field)
            {
                return Err(crate::Error::new(format!(
                    "unknown query field {}",
                    query_read.field
                )));
            }
            return Ok(());
        }
        if query_read.op == "recursive_refs" {
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            if !matches!(field.kind, FieldKind::Ref { .. }) {
                return Err(crate::Error::new(format!(
                    "recursive refs expects ref field {}",
                    query_read.field
                )));
            }
            if !query_read.value.is_string() {
                return Err(crate::Error::new("recursive refs expects root id string"));
            }
            return Ok(());
        }
        if query_read.op == "query" {
            let query = built_query_from_read(query_read)?;
            return Self::apply_built_query_scope_repair(
                schema,
                db,
                query_read,
                &query,
                user,
                bypass_policy,
            );
        }
        if query_read.op == "eq_top_created_at_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top created query expects eq value"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top created query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            if matches!(query_read.field.as_str(), "id" | "$createdBy") {
                let branch_num = branch::checkout(db, &query_read.branch_id)?;
                let observed_row_nums = observed_ids_from_query_value(&query_read.value)?
                    .into_iter()
                    .map(|row_id| row_num(db, &row_id))
                    .collect::<Result<Vec<_>>>()?;
                let observed_filter = if observed_row_nums.is_empty() {
                    String::new()
                } else {
                    format!(
                        "AND row_num NOT IN ({})",
                        sql_placeholders(observed_row_nums.len())
                    )
                };
                let mut params = vec![rusqlite::types::Value::Integer(branch_num)];
                let predicate_sql = if query_read.field == "id" {
                    let row_id = value
                        .as_str()
                        .ok_or_else(|| crate::Error::new("id equality expects a string value"))?;
                    params.push(rusqlite::types::Value::Integer(ensure_row_id(
                        db,
                        &query_read.table,
                        row_id,
                    )?));
                    "row_num = ?".to_owned()
                } else {
                    let user_id = value.as_str().ok_or_else(|| {
                        crate::Error::new("$createdBy equality expects a string value")
                    })?;
                    let Ok(user_num) = users::user_num(db, user_id) else {
                        return Ok(());
                    };
                    params.push(rusqlite::types::Value::Integer(user_num));
                    "j_created_by = ?".to_owned()
                };
                params.extend(
                    observed_row_nums
                        .into_iter()
                        .map(rusqlite::types::Value::Integer),
                );
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND is_deleted = 0
                           AND {predicate_sql}
                           {observed_filter}",
                        crate::schema::current_table(&query_read.table),
                    ),
                    params_from_iter(params.iter()),
                )?;
                return Ok(());
            }
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.j_created_at DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "eq_top_field_desc" {
            let value = query_read
                .value
                .get("eq")
                .ok_or_else(|| crate::Error::new("top field query expects eq value"))?;
            let order_field_name = query_read
                .value
                .get("order_field")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| crate::Error::new("top field query expects order_field"))?;
            let limit = query_read
                .value
                .get("limit")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::Error::new("top field query expects numeric limit"))?;
            let table = schema.table_def(&query_read.table)?;
            let field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == query_read.field)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown query field {}", query_read.field))
                })?;
            let order_field = table
                .fields
                .iter()
                .find(|candidate| candidate.name == order_field_name)
                .ok_or_else(|| {
                    crate::Error::new(format!("unknown order field {order_field_name}"))
                })?;
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            let predicate_column =
                crate::schema::quote_ident(&crate::schema::storage_column(field));
            let order_column =
                crate::schema::quote_ident(&crate::schema::storage_column(order_field));
            let predicate_sql = query_predicate::sql(field, &predicate_column, "eq")?;
            let predicate_value = query_predicate::value(field, "eq", value, db)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND is_deleted = 0
                       AND {predicate_sql}
                       AND row_num NOT IN (
                         SELECT current.row_num
                         FROM {current_table} current
                         JOIN jazz_tx tx ON tx.tx_num = current.visible_tx_num
                         WHERE current.j_branch_num = ?
                           AND current.is_deleted = 0
                           AND tx.outcome != ?
                           AND {current_predicate_sql}
                         ORDER BY current.{order_column} DESC, current.row_num
                         LIMIT ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    current_table = crate::schema::current_table(&query_read.table),
                    current_predicate_sql =
                        query_predicate::sql(field, &format!("current.{predicate_column}"), "eq")?,
                ),
                params![
                    branch_num,
                    predicate_value.clone(),
                    branch_num,
                    tx::OUTCOME_REJECTED,
                    predicate_value,
                    limit as i64
                ],
            )?;
            return Ok(());
        }
        if query_read.op == "in" && query_read.field != "id" {
            for value in query_read
                .value
                .as_array()
                .ok_or_else(|| crate::Error::new("in predicate expects an array value"))?
            {
                let eq_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: query_read.table.clone(),
                    field: query_read.field.clone(),
                    op: "eq".to_owned(),
                    value: value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &eq_read, user, bypass_policy)?;
            }
            return Ok(());
        }
        if query_read.field == "id" {
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            if query_read.op == "ne" {
                let excluded_id = query_read
                    .value
                    .as_str()
                    .ok_or_else(|| crate::Error::new("id inequality expects a string value"))?;
                db.execute(
                    &format!(
                        "DELETE FROM {current_table}
                         WHERE j_branch_num = ?
                           AND row_num != (SELECT row_num FROM jazz_row_id WHERE row_id = ?)
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_row_id ids ON ids.row_num = h.row_num
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE ids.row_id != ?
                               AND h.j_branch_num = ?
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        current_table = crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                    ),
                    params![
                        branch_num,
                        excluded_id,
                        excluded_id,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
                return Ok(());
            }
            let row_ids = id_predicate_values(&query_read.op, &query_read.value)?;
            for row_id in row_ids {
                let row_num = ensure_row_id(db, &query_read.table, &row_id)?;
                db.execute(
                    &format!(
                        "DELETE FROM {}
                         WHERE j_branch_num = ?
                           AND row_num = ?
                           AND row_num NOT IN (
                             SELECT h.row_num
                             FROM {history_table} h
                             JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                             WHERE h.row_num = ?
                               AND h.j_branch_num = ?
                               AND h.op != 3
                               AND tx.outcome != ?
                           )",
                        crate::schema::current_table(&query_read.table),
                        history_table = crate::schema::history_table(&query_read.table),
                    ),
                    params![
                        branch_num,
                        row_num,
                        row_num,
                        branch_num,
                        tx::OUTCOME_REJECTED
                    ],
                )?;
            }
            return Ok(());
        }
        if query_read.field == "$createdBy" {
            let Some(created_by) = query_read.value.as_str() else {
                return Err(crate::Error::new(
                    "$createdBy predicate expects a string value",
                ));
            };
            let created_by_num = users::ensure_user(db, created_by)?;
            let created_by_sql = match query_read.op.as_str() {
                "eq" => "j_created_by = ?",
                "ne" => "j_created_by != ?",
                op => {
                    return Err(crate::Error::new(format!(
                        "unsupported $createdBy predicate op {op}"
                    )));
                }
            };
            let history_created_by_sql = match query_read.op.as_str() {
                "eq" => "h.j_created_by = ?",
                "ne" => "h.j_created_by != ?",
                _ => unreachable!("validated above"),
            };
            let branch_num = branch::checkout(db, &query_read.branch_id)?;
            db.execute(
                &format!(
                    "DELETE FROM {}
                     WHERE j_branch_num = ?
                       AND {created_by_sql}
                       AND row_num NOT IN (
                         SELECT h.row_num
                         FROM {history_table} h
                         JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                         WHERE h.j_branch_num = ?
                           AND {history_created_by_sql}
                           AND h.op != 3
                           AND tx.outcome != ?
                       )",
                    crate::schema::current_table(&query_read.table),
                    history_table = crate::schema::history_table(&query_read.table),
                ),
                params![
                    branch_num,
                    created_by_num,
                    branch_num,
                    created_by_num,
                    tx::OUTCOME_REJECTED
                ],
            )?;
            return Ok(());
        }
        let table = schema.table_def(&query_read.table)?;
        let field = table
            .fields
            .iter()
            .find(|candidate| candidate.name == query_read.field)
            .ok_or_else(|| {
                crate::Error::new(format!("unknown query field {}", query_read.field))
            })?;
        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let predicate_column = crate::schema::quote_ident(&crate::schema::storage_column(field));
        let predicate_sql = query_predicate::sql(field, &predicate_column, &query_read.op)?;
        let predicate_value = query_predicate::value(field, &query_read.op, &query_read.value, db)?;
        db.execute(
            &format!(
                "DELETE FROM {}
                 WHERE j_branch_num = ?
                   AND is_deleted = 0
                   AND {predicate_sql}
                   AND row_num NOT IN (
                     SELECT ids.row_num
                     FROM jazz_row_id ids
                     JOIN {history_table} h ON h.row_num = ids.row_num
                     JOIN jazz_tx tx ON tx.tx_num = h.tx_num
                     WHERE h.j_branch_num = ?
                       AND h.op != 3
                       AND tx.outcome != ?
                       AND {history_predicate_sql}
                   )",
                crate::schema::current_table(&query_read.table),
                history_table = crate::schema::history_table(&query_read.table),
                history_predicate_sql =
                    query_predicate::sql(field, &format!("h.{predicate_column}"), &query_read.op)?,
            ),
            params![
                branch_num,
                predicate_value.clone(),
                branch_num,
                tx::OUTCOME_REJECTED,
                predicate_value
            ],
        )?;
        Ok(())
    }

    fn apply_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        // Built queries are recorded as one opaque query-read value so they can
        // be replayed exactly after reconnect:
        //
        //   jazz_query_read
        //   field="$query", op="query", value=<BuiltQuery JSON>
        //
        // Repair keeps the old narrow fast path for simple predicates, then
        // falls back to a generic SQL-lowered pass for the wider built-query
        // language:
        //
        //   +-----------------------------+
        //   | BuiltQuery descriptor       |
        //   +-----------------------------+
        //        | one predicate only
        //        v
        //   +-----------------------------+      +--------------------------+
        //   | QueryReadRecord predicate   | ---> | apply_query_scope_repair |
        //   +-----------------------------+      +--------------------------+
        //
        //        | every other SQL-lowered shape
        //        v
        //   +-----------------------------+
        //   | generic SQL-lowered repair  |
        //   +-----------------------------+
        match built_query_repair_scope(built_query)? {
            BuiltQueryRepairScope::Predicate(condition) => {
                let predicate_read = QueryReadRecord {
                    branch_id: query_read.branch_id.clone(),
                    table: built_query.table.clone(),
                    field: condition.column.clone(),
                    op: condition.op.as_str().to_owned(),
                    value: condition.value.clone(),
                };
                Self::apply_query_scope_repair(schema, db, &predicate_read, user, bypass_policy)
            }
            BuiltQueryRepairScope::Generic => Self::apply_generic_built_query_scope_repair(
                schema,
                db,
                query_read,
                built_query,
                user,
                bypass_policy,
            ),
        }
    }

    fn apply_generic_built_query_scope_repair(
        schema: &SchemaDef,
        db: &Connection,
        query_read: &QueryReadRecord,
        built_query: &BuiltQuery,
        user: &str,
        bypass_policy: bool,
    ) -> Result<()> {
        if built_query.limit.is_none() && built_query.offset.unwrap_or(0) == 0 {
            return Ok(());
        }

        let branch_num = branch::checkout(db, &query_read.branch_id)?;
        let context = query::QueryContext {
            conn: db,
            schema,
            branch_num,
            user,
            bypass_policy,
            read_tier: ReadTier::Local,
        };
        let mut scope_query = built_query.clone();
        scope_query.limit = None;
        scope_query.offset = None;
        let scope_row_nums = context
            .read_rows_for_built_query(&scope_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        if scope_row_nums.is_empty() {
            return Ok(());
        }

        let keep_query = built_query_repair_keep_query(built_query)?;
        let keep_row_nums = context
            .read_rows_for_built_query(&keep_query)?
            .iter()
            .map(|row| row_num(db, &row.id))
            .collect::<Result<Vec<_>>>()?;
        delete_current_rows_outside_keep_set(
            db,
            &built_query.table,
            branch_num,
            &scope_row_nums,
            &keep_row_nums,
        )
    }

    fn apply_history_record(
        context: &mut ApplyHistoryContext<'_>,
        record: &HistoryRecord,
    ) -> Result<()> {
        let table = context.schema.table_def(&record.table)?;
        let row_num = context.apply_caches.ensure_row_id_with_status(
            context.db,
            &record.table,
            &record.row_id,
        )?;
        if row_id_used_by_other_table(context.db, context.schema, &record.table, row_num)? {
            return Err(crate::Error::new(format!(
                "row id {} is already used by another table",
                record.row_id
            )));
        }
        let tx_num = context
            .tx_nums_by_id
            .get(&record.tx_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx::tx_num(context.db, &record.tx_id))?;
        let branch_num = context
            .branch_nums_by_id
            .get(&record.branch_id)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| branch::ensure(context.db, &record.branch_id, None, now_ms()))?;
        let tx_info = context
            .tx_info_by_num
            .get(&tx_num)
            .copied()
            .map(Ok)
            .unwrap_or_else(|| tx_apply_info(context.db, tx_num))?;
        let outcome = tx_info.outcome;
        let history_exists = history_record_exists(context.db, &record.table, row_num, tx_num)?;
        if history_exists
            && current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
                .is_some_and(|current_tx_num| current_tx_num == tx_num)
        {
            return Ok(());
        }

        let mut columns = vec![
            "row_num".to_owned(),
            "tx_num".to_owned(),
            "j_branch_num".to_owned(),
            "op".to_owned(),
        ];
        let mut values = vec![
            rusqlite::types::Value::Integer(row_num),
            rusqlite::types::Value::Integer(tx_num),
            rusqlite::types::Value::Integer(branch_num),
            rusqlite::types::Value::Integer(record.op),
        ];
        for field in &table.fields {
            let value = record
                .values
                .get(&field.name)
                .or_else(|| record.values.get(&field.storage_name))
                .ok_or_else(|| crate::Error::new(format!("missing field {}", field.name)))?;
            columns.push(crate::schema::quote_ident(&crate::schema::storage_column(
                field,
            )));
            values.push(crate::schema::field_sql_value(
                field,
                value,
                |ref_table, row_id| {
                    context
                        .apply_caches
                        .ensure_row_id(context.db, ref_table, row_id)
                },
            )?);
        }
        columns.extend([
            "j_created_at".to_owned(),
            "j_updated_at".to_owned(),
            "j_created_by".to_owned(),
            "j_updated_by".to_owned(),
        ]);
        let created_by_num = context
            .apply_caches
            .ensure_user(context.db, &record.created_by)?;
        let updated_by_num = context
            .apply_caches
            .ensure_user(context.db, &record.updated_by)?;
        values.extend([
            rusqlite::types::Value::Integer(record.created_at),
            rusqlite::types::Value::Integer(record.updated_at),
            rusqlite::types::Value::Integer(created_by_num),
            rusqlite::types::Value::Integer(updated_by_num),
        ]);
        if !history_exists {
            insert_dynamic(
                context.db,
                &crate::schema::history_table(&record.table),
                &columns,
                &values,
            )?;
        }
        let table_num = context
            .table_nums_by_name
            .get(&record.table)
            .copied()
            .ok_or_else(|| crate::Error::new("history record references missing table"))?;
        if !history_exists {
            record_tx_write_num(context.db, tx_num, table_num, row_num, record.op)?;
        }
        if outcome == tx::OUTCOME_PENDING && tx_info.conflict_mode == tx::MODE_EXCLUSIVE {
            return Ok(());
        }
        if tx_info.outcome == tx::OUTCOME_PENDING
            && tx_info.node_num != context.local_node_num
            && durable_version_exists_for_row(context.db, &record.table, row_num, branch_num)?
        {
            return Ok(());
        }
        if outcome != tx::OUTCOME_REJECTED {
            if let Some(current_tx_num) =
                current_visible_tx_num(context.db, &record.table, row_num, branch_num)?
            {
                if let Some(is_newer) =
                    tx_is_newer_than_current_fast_path(context.db, tx_num, current_tx_num)?
                {
                    if !is_newer {
                        return Ok(());
                    }
                } else if !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )? {
                    return Ok(());
                }
            } else if !context.apply_caches.row_created_in_apply(row_num)
                && !is_newest_version_for_current(
                    context.db,
                    &record.table,
                    row_num,
                    branch_num,
                    tx_num,
                )?
            {
                return Ok(());
            }
        }
        if outcome != tx::OUTCOME_REJECTED && record.op == 3 {
            context.db.execute(
                &format!(
                    "DELETE FROM {} WHERE row_num = ? AND j_branch_num = ?",
                    crate::schema::current_table(&record.table)
                ),
                params![row_num, branch_num],
            )?;
            if branch_num != 1 {
                let mut current_columns = vec![
                    "row_num".to_owned(),
                    "j_branch_num".to_owned(),
                    "visible_tx_num".to_owned(),
                    "is_deleted".to_owned(),
                ];
                let mut current_values = vec![
                    rusqlite::types::Value::Integer(row_num),
                    rusqlite::types::Value::Integer(branch_num),
                    rusqlite::types::Value::Integer(tx_num),
                    rusqlite::types::Value::Integer(1),
                ];
                current_columns.extend(columns.iter().skip(4).cloned());
                current_values.extend(values.iter().skip(4).cloned());
                insert_dynamic(
                    context.db,
                    &crate::schema::current_table(&record.table),
                    &current_columns,
                    &current_values,
                )?;
            }
        } else if outcome != tx::OUTCOME_REJECTED {
            let mut current_columns = vec![
                "row_num".to_owned(),
                "j_branch_num".to_owned(),
                "visible_tx_num".to_owned(),
                "is_deleted".to_owned(),
            ];
            let mut current_values = vec![
                rusqlite::types::Value::Integer(row_num),
                rusqlite::types::Value::Integer(branch_num),
                rusqlite::types::Value::Integer(tx_num),
                rusqlite::types::Value::Integer(0),
            ];
            current_columns.extend(columns.iter().skip(4).cloned());
            current_values.extend(values.iter().skip(4).cloned());
            insert_dynamic(
                context.db,
                &crate::schema::current_table(&record.table),
                &current_columns,
                &current_values,
            )?;
        }
        Ok(())
    }
}
