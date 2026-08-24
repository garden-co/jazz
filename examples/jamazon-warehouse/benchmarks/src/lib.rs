//! Self-contained Jamazon Warehouse fixture. It mirrors the app's operational
//! tables and query shapes without importing the web application.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, block_on};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

type BenchDb = Db<MemoryStorage>;

pub struct Fixture {
    db: BenchDb,
    pending_orders: PreparedQuery,
    low_stock: PreparedQuery,
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
                author: AuthorId::SYSTEM,
            },
        )))
        .expect("open fixture");
        let warehouse = row(1, 0);
        let district = row(2, 0);
        let customer = row(3, 0);
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
            "customers",
            customer,
            [
                ("warehouse_id", Value::Uuid(warehouse.0)),
                ("district_id", Value::Uuid(district.0)),
                ("name", Value::String("Demo buyer".into())),
                ("balance_cents", Value::I32(0)),
            ],
        );
        for n in 0..order_count {
            insert(
                &db,
                "orders",
                row(4, n),
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
        let pending_orders = db
            .prepare_query(
                &Query::from("orders")
                    .filter(eq(col("district_id"), lit(district.0)))
                    .filter(eq(col("status"), lit("pending")))
                    .order_by("order_number", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare pending orders");
        let low_stock = db
            .prepare_query(
                &Query::from("stock")
                    .filter(eq(col("warehouse_id"), lit(warehouse.0)))
                    .order_by("on_hand", OrderDirection::Asc)
                    .limit(20),
            )
            .expect("prepare low-stock");
        Self {
            db,
            pending_orders,
            low_stock,
        }
    }
    pub fn pending_order_count(&self) -> usize {
        self.db
            .read(&self.pending_orders)
            .expect("pending orders")
            .len()
    }
    pub fn low_stock_count(&self) -> usize {
        self.db.read(&self.low_stock).expect("low stock").len()
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("warehouses")
                    .column("name", ColumnType::Text)
                    .column("region", ColumnType::Text)
                    .column("operator_id", ColumnType::Text),
            )
            .table(
                TableSchemaBuilder::new("districts")
                    .fk_column("warehouse_id", "warehouses")
                    .column("name", ColumnType::Text)
                    .column("next_order_number", ColumnType::Integer)
                    .index_only(["warehouse_id"]),
            )
            .table(
                TableSchemaBuilder::new("items")
                    .column("sku", ColumnType::Text)
                    .column("name", ColumnType::Text)
                    .column("unit_price_cents", ColumnType::Integer),
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
                    .index_only(["warehouse_id", "district_id"]),
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
                    .index_only(["district_id", "status", "order_number", "idempotency_key"]),
            )
            .table(
                TableSchemaBuilder::new("order_lines")
                    .fk_column("order_id", "orders")
                    .fk_column("item_id", "items")
                    .column("quantity", ColumnType::Integer)
                    .column("amount_cents", ColumnType::Integer),
            )
            .table(
                TableSchemaBuilder::new("payments")
                    .fk_column("customer_id", "customers")
                    .fk_column("order_id", "orders")
                    .column("amount_cents", ColumnType::Integer)
                    .column("idempotency_key", ColumnType::Text),
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
        db.insert_with_id(
            table,
            id,
            cells
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect::<BTreeMap<_, _>>(),
        ),
    )
    .expect("seed row");
    block_on(write.wait(DurabilityTier::Local)).expect("local seed");
}
