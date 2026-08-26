//! Self-contained Jamazon Warehouse fixture. It mirrors the app's operational
//! tables and query shapes without importing the web application.

use std::collections::BTreeMap;

use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, InsertOptions, PreparedQuery, UpdateOptions, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

pub struct Fixture {
    db: BenchDb,
    warehouse: RowUuid,
    district: RowUuid,
    customer: RowUuid,
    item: RowUuid,
    stock: RowUuid,
    warehouse_districts: PreparedQuery,
    district_customers: PreparedQuery,
    stock_on_hand: PreparedQuery,
    district_next_order_number: PreparedQuery,
    complete_orders_for_receipt: PreparedQuery,
    all_order_lines: PreparedQuery,
    all_payments: PreparedQuery,
    pending_orders: PreparedQuery,
    low_stock_candidates: PreparedQuery,
}

impl Fixture {
    pub fn new(order_count: usize) -> Self {
        let schema = schema();
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = block_on(Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x6a; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .expect("open fixture");
        let warehouse = row(1, 0);
        let district = row(2, 0);
        let other_district = row(2, 1);
        let customer = row(3, 0);
        let item = row(4, 0);
        let stock = row(5, 0);
        let adequate_item = row(4, 1);
        let adequate_stock = row(5, 1);
        let low_item = row(4, 2);
        let low_stock = row(5, 2);
        insert(
            &db,
            "warehouses",
            warehouse,
            [
                ("name", Value::String("East".into())),
                ("region", Value::String("demo".into())),
                ("operator_id", Value::String("operator".into())),
            ],
        );
        insert(
            &db,
            "items",
            adequate_item,
            [
                ("sku", Value::String("JAM-002".into())),
                ("name", Value::String("Cable".into())),
                ("unit_price_cents", Value::I32(1_000)),
                ("operator_id", Value::String("operator".into())),
            ],
        );
        insert(
            &db,
            "stock",
            adequate_stock,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("item_id", Value::Uuid(adequate_item.0)),
                ("on_hand", Value::I32(20)),
                ("reorder_level", Value::I32(5)),
            ],
        );
        insert(
            &db,
            "items",
            low_item,
            [
                ("sku", Value::String("JAM-003".into())),
                ("name", Value::String("Picks".into())),
                ("unit_price_cents", Value::I32(500)),
                ("operator_id", Value::String("operator".into())),
            ],
        );
        insert(
            &db,
            "stock",
            low_stock,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("item_id", Value::Uuid(low_item.0)),
                ("on_hand", Value::I32(2)),
                ("reorder_level", Value::I32(4)),
            ],
        );
        insert(
            &db,
            "districts",
            district,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("name", Value::String("A".into())),
                ("next_order_number", Value::I32(order_count as i32)),
            ],
        );
        insert(
            &db,
            "districts",
            other_district,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("name", Value::String("B".into())),
                ("next_order_number", Value::I32(0)),
            ],
        );
        insert(
            &db,
            "customers",
            customer,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("district_id", Value::Uuid(district.0)),
                ("name", Value::String("Demo buyer".into())),
                ("balance_cents", Value::I32(0)),
            ],
        );
        insert(
            &db,
            "items",
            item,
            [
                ("sku", Value::String("JAM-001".into())),
                ("name", Value::String("Jazzmaster strings".into())),
                ("unit_price_cents", Value::I32(2_500)),
                ("operator_id", Value::String("operator".into())),
            ],
        );
        insert(
            &db,
            "stock",
            stock,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("item_id", Value::Uuid(item.0)),
                ("on_hand", Value::I32(10)),
                ("reorder_level", Value::I32(12)),
            ],
        );
        for n in 0..order_count {
            insert(
                &db,
                "orders",
                row(6, n),
                [
                    ("warehouse_id", Value::Uuid(warehouse.0)),
                    ("district_id", Value::Uuid(district.0)),
                    ("customer_id", Value::Uuid(customer.0)),
                    ("order_number", Value::I32(n as i32)),
                    (
                        "status",
                        Value::String(if n % 3 == 0 { "pending" } else { "delivered" }.into()),
                    ),
                    ("total_cents", Value::I32(10_000)),
                    ("idempotency_key", Value::String(format!("seed-{n}"))),
                ],
            );
        }
        // A pending row in the same warehouse but a different district makes
        // the production district scope observable in benchmark receipts.
        insert(
            &db,
            "orders",
            row(6, order_count),
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("district_id", Value::Uuid(other_district.0)),
                ("customer_id", Value::Uuid(customer.0)),
                ("order_number", Value::I32(-1)),
                ("status", Value::String("pending".into())),
                ("total_cents", Value::I32(1)),
                (
                    "idempotency_key",
                    Value::String("other-district-distractor".into()),
                ),
            ],
        );
        let warehouse_districts = db
            .prepare_query(
                &Query::from("districts")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .order_by("name", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare warehouse districts");
        let district_customers = db
            .prepare_query(
                &Query::from("customers")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .filter(eq(col("district_id"), lit(district.0)))
                    .order_by("name", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare district customers");
        let pending_orders = db
            .prepare_query(
                &Query::from("orders")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .filter(eq(col("district_id"), lit(district.0)))
                    .filter(eq(col("status"), lit("pending")))
                    .order_by("order_number", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare pending orders");
        let stock_on_hand = db
            .prepare_query(
                &Query::from("stock")
                    .select(["on_hand"])
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .filter(eq(col("item_id"), lit(item.0)))
                    .limit(1),
            )
            .expect("prepare stock on hand");
        let district_next_order_number = db
            .prepare_query(
                &Query::from("districts")
                    .select(["next_order_number"])
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .filter(eq(col("name"), lit("A")))
                    .order_by("name", OrderDirection::Asc)
                    .limit(1),
            )
            .expect("prepare district next order number");
        // This complete-state read is a fixture-only receipt used to prove
        // transaction atomicity. It is deliberately not the application
        // console query: the public TS API exposes an ordered, bounded page.
        let complete_orders_for_receipt = db
            .prepare_query(
                &Query::from("orders")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .filter(eq(col("district_id"), lit(district.0)))
                    .order_by("order_number", OrderDirection::Asc),
            )
            .expect("prepare all orders");
        let all_order_lines = db
            .prepare_query(&Query::from("order_lines"))
            .expect("prepare all order lines");
        let all_payments = db
            .prepare_query(&Query::from("payments"))
            .expect("prepare all payments");
        // Jazz does not yet lower `on_hand < reorder_level` field-to-field
        // predicates (the capability report says exactly that). Keep the
        // complete candidate read honest and apply that predicate here until
        // #1864 adds the missing lowering. Do not limit before this filter:
        // correctness wins over the eventual indexed scaling path.
        let low_stock_candidates = db
            .prepare_query(
                &Query::from("stock")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .order_by("on_hand", OrderDirection::Asc),
            )
            .expect("prepare low-stock candidates");
        Self {
            db,
            warehouse,
            district,
            customer,
            item,
            stock,
            warehouse_districts,
            district_customers,
            stock_on_hand,
            district_next_order_number,
            complete_orders_for_receipt,
            all_order_lines,
            all_payments,
            pending_orders,
            low_stock_candidates,
        }
    }
    pub fn warehouse_district_count(&self) -> usize {
        self.db
            .read(&self.warehouse_districts)
            .expect("warehouse districts")
            .len()
    }
    pub fn district_customer_count(&self) -> usize {
        self.db
            .read(&self.district_customers)
            .expect("district customers")
            .len()
    }
    pub fn pending_order_count(&self) -> usize {
        self.db
            .read(&self.pending_orders)
            .expect("pending orders")
            .len()
    }

    /// Fixture receipt for the console's bounded, ordered pending-order page.
    pub fn pending_order_numbers_for_receipt(&self) -> Vec<i32> {
        self.db
            .read(&self.pending_orders)
            .expect("pending orders")
            .into_iter()
            .map(|order| match order.cell_at(3) {
                Some(Value::I32(number)) => number,
                other => panic!("invalid pending order number: {other:?}"),
            })
            .collect()
    }
    pub fn low_stock_count(&self) -> usize {
        self.db
            .read(&self.low_stock_candidates)
            .expect("low-stock candidates")
            .into_iter()
            .filter(|stock| {
                matches!(
                    (stock.cell_at(2), stock.cell_at(3)),
                    (Some(Value::I32(on_hand)), Some(Value::I32(reorder_level))) if on_hand < reorder_level
                )
            })
            .count()
    }
    pub fn purchase(
        &self,
        request_key: &str,
        quantity: i32,
    ) -> Result<PurchaseReceipt, &'static str> {
        block_on(async {
            let tx = self.db.exclusive_tx().await.map_err(|_| "open purchase")?;
            let idempotency_lookup = self
                .db
                .prepare_query(
                    &Query::from("orders")
                        .filter(eq(col("warehouse_id"), lit(self.warehouse.0)))
                        .filter(eq(col("idempotency_key"), lit(request_key)))
                        .limit(1),
                )
                .map_err(|_| "prepare idempotency lookup")?;
            let prior_orders = tx
                .all_prepared(&idempotency_lookup)
                .await
                .map_err(|_| "read orders")?;
            if let Some(order) = prior_orders.into_iter().find(|order| {
                matches!(order.cell_at(6), Some(Value::String(ref key)) if key == request_key)
            }) {
                return Ok(PurchaseReceipt {
                    order_number: match order.cell_at(3) {
                        Some(Value::I32(order_number)) => order_number,
                        _ => return Err("invalid existing order"),
                    },
                    total_cents: match order.cell_at(5) {
                        Some(Value::I32(total_cents)) => total_cents,
                        _ => return Err("invalid existing order"),
                    },
                });
            }
            let stock = tx
                .read("stock", self.stock)
                .await
                .map_err(|_| "read stock")?
                .ok_or("missing stock")?;
            let on_hand = match stock.get("on_hand") {
                Some(Value::I32(on_hand)) => *on_hand,
                _ => return Err("invalid stock"),
            };
            if quantity <= 0 || quantity > on_hand {
                return Err("insufficient stock");
            }
            let district = tx
                .read("districts", self.district)
                .await
                .map_err(|_| "read district")?
                .ok_or("missing district")?;
            let order_number = match district.get("next_order_number") {
                Some(Value::I32(order_number)) => *order_number,
                _ => return Err("invalid district"),
            };
            let order_number_usize =
                usize::try_from(order_number).map_err(|_| "invalid order number")?;
            let order = row(7, order_number_usize);
            let order_line = row(8, order_number_usize);
            let payment = row(9, order_number_usize);
            let amount_cents = quantity.checked_mul(2_500).ok_or("total overflow")?;
            let next_order_number = order_number
                .checked_add(1)
                .ok_or("order counter overflow")?;
            let customer = tx
                .read("customers", self.customer)
                .await
                .map_err(|_| "read customer")?
                .ok_or("missing customer")?;
            let balance = match customer.get("balance_cents") {
                Some(Value::I32(balance)) => *balance,
                _ => return Err("invalid customer"),
            };
            let next_balance = balance
                .checked_sub(amount_cents)
                .ok_or("balance overflow")?;
            tx.update(
                "stock",
                self.stock,
                BTreeMap::from([("on_hand".to_owned(), Value::I32(on_hand - quantity))]),
                UpdateOptions::default(),
            )
            .await
            .map_err(|_| "stage stock")?;
            tx.update(
                "districts",
                self.district,
                BTreeMap::from([(
                    "next_order_number".to_owned(),
                    Value::I32(next_order_number),
                )]),
                UpdateOptions::default(),
            )
            .await
            .map_err(|_| "stage district")?;
            tx.update(
                "customers",
                self.customer,
                BTreeMap::from([("balance_cents".to_owned(), Value::I32(next_balance))]),
                UpdateOptions::default(),
            )
            .await
            .map_err(|_| "stage customer")?;
            tx.insert(
                "orders",
                BTreeMap::from([
                    ("warehouse_id".to_owned(), Value::Uuid(self.warehouse.0)),
                    ("district_id".to_owned(), Value::Uuid(self.district.0)),
                    ("customer_id".to_owned(), Value::Uuid(self.customer.0)),
                    ("order_number".to_owned(), Value::I32(order_number)),
                    ("status".to_owned(), Value::String("pending".into())),
                    ("total_cents".to_owned(), Value::I32(amount_cents)),
                    (
                        "idempotency_key".to_owned(),
                        Value::String(request_key.into()),
                    ),
                ]),
                InsertOptions {
                    row_id: Some(order),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "stage order")?;
            tx.insert(
                "order_lines",
                BTreeMap::from([
                    ("warehouse_id".to_owned(), Value::Uuid(self.warehouse.0)),
                    ("order_id".to_owned(), Value::Uuid(order.0)),
                    ("item_id".to_owned(), Value::Uuid(self.item.0)),
                    ("quantity".to_owned(), Value::I32(quantity)),
                    ("amount_cents".to_owned(), Value::I32(amount_cents)),
                ]),
                InsertOptions {
                    row_id: Some(order_line),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "stage order line")?;
            tx.insert(
                "payments",
                BTreeMap::from([
                    ("warehouse_id".to_owned(), Value::Uuid(self.warehouse.0)),
                    ("customer_id".to_owned(), Value::Uuid(self.customer.0)),
                    (
                        "order_id".to_owned(),
                        Value::Nullable(Some(Box::new(Value::Uuid(order.0)))),
                    ),
                    ("amount_cents".to_owned(), Value::I32(amount_cents)),
                    (
                        "idempotency_key".to_owned(),
                        Value::String(request_key.into()),
                    ),
                ]),
                InsertOptions {
                    row_id: Some(payment),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "stage payment")?;
            tx.commit().await.map_err(|_| "commit purchase")?;
            Ok(PurchaseReceipt {
                order_number,
                total_cents: amount_cents,
            })
        })
    }
    pub fn stock_on_hand(&self) -> i32 {
        let stock = self
            .db
            .read(&self.stock_on_hand)
            .expect("read stock")
            .pop()
            .expect("stock exists");
        match stock.cell_at(0) {
            Some(Value::I32(on_hand)) => on_hand,
            other => panic!("invalid stock value: {other:?}"),
        }
    }
    pub fn complete_order_count_for_receipt(&self) -> usize {
        self.db
            .read(&self.complete_orders_for_receipt)
            .expect("complete orders")
            .len()
    }
    pub fn order_line_count(&self) -> usize {
        self.db
            .read(&self.all_order_lines)
            .expect("order lines")
            .len()
    }
    pub fn payment_count(&self) -> usize {
        self.db.read(&self.all_payments).expect("payments").len()
    }
    pub fn purchase_artifacts(&self) -> PurchaseArtifacts {
        let line = self
            .db
            .read(&self.all_order_lines)
            .expect("order lines")
            .pop()
            .expect("order line exists");
        let payment = self
            .db
            .read(&self.all_payments)
            .expect("payments")
            .pop()
            .expect("payment exists");
        let order_id = match line.cell_at(1) {
            Some(Value::Uuid(id)) => id,
            other => panic!("invalid order-line relation: {other:?}"),
        };
        PurchaseArtifacts {
            line_quantity: match line.cell_at(3) {
                Some(Value::I32(value)) => value,
                other => panic!("invalid line quantity: {other:?}"),
            },
            line_amount_cents: match line.cell_at(4) {
                Some(Value::I32(value)) => value,
                other => panic!("invalid line amount: {other:?}"),
            },
            payment_amount_cents: match payment.cell_at(3) {
                Some(Value::I32(value)) => value,
                other => panic!("invalid payment amount: {other:?}"),
            },
            line_references_item: line.cell_at(2) == Some(Value::Uuid(self.item.0)),
            payment_references_customer: payment.cell_at(1) == Some(Value::Uuid(self.customer.0)),
            payment_references_order: matches!(payment.cell_at(2), Some(Value::Nullable(Some(value))) if *value == Value::Uuid(order_id)),
        }
    }
    pub fn customer_balance(&self) -> i32 {
        let customer = block_on(self.db.exclusive_tx()).expect("open customer read");
        let cells = block_on(customer.read("customers", self.customer))
            .expect("read customer")
            .expect("customer exists");
        match cells.get("balance_cents") {
            Some(Value::I32(balance)) => *balance,
            other => panic!("invalid customer value: {other:?}"),
        }
    }
    pub fn set_stock_on_hand_for_test(&self, on_hand: i32) {
        block_on(async {
            let tx = self.db.exclusive_tx().await.expect("open stock update");
            tx.update(
                "stock",
                self.stock,
                BTreeMap::from([("on_hand".to_owned(), Value::I32(on_hand))]),
                UpdateOptions::default(),
            )
            .await
            .expect("stage stock update");
            tx.commit().await.expect("commit stock update");
        });
    }
    pub fn district_next_order_number(&self) -> i32 {
        let district = self
            .db
            .read(&self.district_next_order_number)
            .expect("read district")
            .pop()
            .expect("district exists");
        match district.cell_at(0) {
            Some(Value::I32(order_number)) => order_number,
            other => panic!("invalid district value: {other:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PurchaseReceipt {
    pub order_number: i32,
    pub total_cents: i32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PurchaseArtifacts {
    pub line_quantity: i32,
    pub line_amount_cents: i32,
    pub payment_amount_cents: i32,
    pub line_references_item: bool,
    pub payment_references_customer: bool,
    pub payment_references_order: bool,
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("warehouses")
                    .column("name", ColumnType::Text)
                    .column("region", ColumnType::Text)
                    .column("operator_id", ColumnType::Text)
                    .index_only(["operator_id"]),
            )
            .table(
                TableSchemaBuilder::new("districts")
                    .fk_column("warehouse_id", "warehouses")
                    .column("name", ColumnType::Text)
                    .column("next_order_number", ColumnType::Integer)
                    .index_only(["warehouse_id", "name"]),
            )
            .table(
                TableSchemaBuilder::new("items")
                    .column("sku", ColumnType::Text)
                    .column("name", ColumnType::Text)
                    .column("unit_price_cents", ColumnType::Integer)
                    .column("operator_id", ColumnType::Text)
                    .index_only(["operator_id"]),
            )
            .table(
                TableSchemaBuilder::new("stock")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("item_id", "items")
                    .column("on_hand", ColumnType::Integer)
                    .column("reorder_level", ColumnType::Integer)
                    .index_only(["warehouse_id", "item_id", "on_hand"]),
            )
            .table(
                TableSchemaBuilder::new("customers")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("district_id", "districts")
                    .column("name", ColumnType::Text)
                    .column("balance_cents", ColumnType::Integer)
                    .index_only(["warehouse_id", "district_id", "name"]),
            )
            .table(
                TableSchemaBuilder::new("orders")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("district_id", "districts")
                    .fk_column("customer_id", "customers")
                    .column("order_number", ColumnType::Integer)
                    .column("status", ColumnType::Text)
                    .column("total_cents", ColumnType::Integer)
                    .column("idempotency_key", ColumnType::Text)
                    .index_only([
                        "warehouse_id",
                        "idempotency_key",
                        "district_id",
                        "status",
                        "order_number",
                    ]),
            )
            .table(
                TableSchemaBuilder::new("order_lines")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("order_id", "orders")
                    .fk_column("item_id", "items")
                    .column("quantity", ColumnType::Integer)
                    .column("amount_cents", ColumnType::Integer)
                    .index_only(["warehouse_id", "order_id"]),
            )
            .table(
                TableSchemaBuilder::new("payments")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("customer_id", "customers")
                    .nullable_fk_column("order_id", "orders")
                    .column("amount_cents", ColumnType::Integer)
                    .column("idempotency_key", ColumnType::Text)
                    .index_only(["warehouse_id", "order_id"]),
            )
            .table(
                TableSchemaBuilder::new("deliveries")
                    .fk_column("warehouse_id", "warehouses")
                    .fk_column("district_id", "districts")
                    .fk_column("order_id", "orders")
                    .column("status", ColumnType::Text),
            )
            .build(),
    )
    .expect("warehouse schema")
}

fn row(kind: u8, n: usize) -> RowUuid {
    let mut id = [0; 16];
    id[0] = kind;
    id[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(id)
}
fn insert<const N: usize>(db: &BenchDb, table: &str, id: RowUuid, cells: [(&str, Value); N]) {
    let write = block_on(
        db.insert(
            table,
            cells
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect::<BTreeMap<_, _>>(),
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ),
    )
    .expect("seed row");
    block_on(write.wait(DurabilityTier::Local)).expect("local seed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_indexes_match_the_operational_query_shapes() {
        let warehouse_schema = schema();
        for (table_name, columns) in [
            ("warehouses", &["operator_id"][..]),
            ("districts", &["warehouse_id", "name"][..]),
            ("stock", &["warehouse_id", "item_id", "on_hand"][..]),
            ("customers", &["warehouse_id", "district_id", "name"][..]),
            (
                "orders",
                &[
                    "warehouse_id",
                    "district_id",
                    "status",
                    "order_number",
                    "idempotency_key",
                ][..],
            ),
            ("order_lines", &["warehouse_id", "order_id"][..]),
            ("payments", &["warehouse_id", "order_id"][..]),
        ] {
            let table = warehouse_schema
                .tables()
                .iter()
                .find(|table| table.name == table_name)
                .unwrap_or_else(|| panic!("missing {table_name} table"));
            for column in columns {
                assert!(
                    table.indexed_columns.contains(*column),
                    "{table_name} index must retain {column}"
                );
            }
        }
    }
}
