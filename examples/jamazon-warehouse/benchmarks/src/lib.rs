//! Self-contained Jamazon Warehouse fixture. It mirrors the app's operational
//! tables and query shapes without importing the web application.

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ExclusiveTxOps, PreparedQuery, block_on};
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
    warehouse: RowUuid,
    district: RowUuid,
    customer: RowUuid,
    stock: RowUuid,
    warehouse_districts: PreparedQuery,
    district_customers: PreparedQuery,
    stock_on_hand: PreparedQuery,
    district_next_order_number: PreparedQuery,
    all_orders: PreparedQuery,
    all_order_lines: PreparedQuery,
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
        let item = row(4, 0);
        let stock = row(5, 0);
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
        insert(
            &db,
            "items",
            item,
            [
                ("sku", Value::String("JAM-001".into())),
                ("name", Value::String("Jazzmaster strings".into())),
                ("unit_price_cents", Value::I32(2_500)),
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
                    .limit(1),
            )
            .expect("prepare district next order number");
        let all_orders = db
            .prepare_query(&Query::from("orders").filter(eq(col("warehouse_id"), lit(warehouse.0))))
            .expect("prepare all orders");
        let all_order_lines = db
            .prepare_query(&Query::from("order_lines"))
            .expect("prepare all order lines");
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
            warehouse,
            district,
            customer,
            stock,
            warehouse_districts,
            district_customers,
            stock_on_hand,
            district_next_order_number,
            all_orders,
            all_order_lines,
            pending_orders,
            low_stock,
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
    pub fn low_stock_count(&self) -> usize {
        self.db.read(&self.low_stock).expect("low stock").len()
    }
    pub fn purchase(&self, quantity: i32) -> Result<(), &'static str> {
        block_on(async {
            let tx = self.db.exclusive_tx().await.map_err(|_| "open purchase")?;
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
            let order = row(7, order_number as usize);
            let order_line = row(8, order_number as usize);
            let amount_cents = quantity * 2_500;
            tx.update(
                "stock",
                self.stock,
                BTreeMap::from([("on_hand".to_owned(), Value::I32(on_hand - quantity))]),
            )
            .await
            .map_err(|_| "stage stock")?;
            tx.update(
                "districts",
                self.district,
                BTreeMap::from([("next_order_number".to_owned(), Value::I32(order_number + 1))]),
            )
            .await
            .map_err(|_| "stage district")?;
            tx.update(
                "customers",
                self.customer,
                BTreeMap::from([("balance_cents".to_owned(), Value::I32(-amount_cents))]),
            )
            .await
            .map_err(|_| "stage customer")?;
            tx.insert_with_id(
                "orders",
                order,
                BTreeMap::from([
                    ("warehouse_id".to_owned(), Value::Uuid(self.warehouse.0)),
                    ("district_id".to_owned(), Value::Uuid(self.district.0)),
                    ("customer_id".to_owned(), Value::Uuid(self.customer.0)),
                    ("order_number".to_owned(), Value::I32(order_number)),
                    ("status".to_owned(), Value::String("pending".into())),
                    ("total_cents".to_owned(), Value::I32(amount_cents)),
                    (
                        "idempotency_key".to_owned(),
                        Value::String(format!("purchase-{order_number}")),
                    ),
                ]),
            )
            .await
            .map_err(|_| "stage order")?;
            tx.insert_with_id(
                "order_lines",
                order_line,
                BTreeMap::from([
                    ("order_id".to_owned(), Value::Uuid(order.0)),
                    ("item_id".to_owned(), Value::Uuid(row(4, 0).0)),
                    ("quantity".to_owned(), Value::I32(quantity)),
                    ("amount_cents".to_owned(), Value::I32(amount_cents)),
                ]),
            )
            .await
            .map_err(|_| "stage order line")?;
            tx.commit().await.map_err(|_| "commit purchase")?;
            Ok(())
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
    pub fn order_count(&self) -> usize {
        self.db.read(&self.all_orders).expect("orders").len()
    }
    pub fn order_line_count(&self) -> usize {
        self.db
            .read(&self.all_order_lines)
            .expect("order lines")
            .len()
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
