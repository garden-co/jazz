import type { Db, ExclusiveWriteResult } from "jazz-tools";
import { app } from "../schema.js";

export interface WarehouseScope {
  warehouseId: string;
  districtId: string;
}

export interface PurchaseRequest extends WarehouseScope {
  customerId: string;
  itemId: string;
  quantity: number;
  idempotencyKey: string;
}

export interface PurchaseReceipt {
  orderId: string;
  orderNumber: number;
  totalCents: number;
}

/**
 * The bounded operational reads used by the warehouse console. Keep these
 * together with checkout so browser topology tests and a future UI exercise
 * the same access paths rather than generic table scans.
 */
export function warehouseQueries({ warehouseId, districtId }: WarehouseScope) {
  return {
    districts: app.districts.where({ warehouse_id: warehouseId }).orderBy("name", "asc").limit(20),
    customers: app.customers
      .where({ warehouse_id: warehouseId, district_id: districtId })
      .orderBy("name", "asc")
      .limit(20),
    pendingOrders: app.orders
      .where({ warehouse_id: warehouseId, district_id: districtId, status: "pending" })
      .orderBy("order_number", "asc")
      .limit(20),
    allOrders: app.orders
      .where({ warehouse_id: warehouseId, district_id: districtId })
      .orderBy("order_number", "asc"),
  };
}

/**
 * Stage one TPC-C-shaped purchase in an exclusive transaction. The authority
 * validates the stock, district counter, customer balance, order, line, and
 * payment as one unit. Repeating an already accepted request key returns the
 * original receipt without decrementing stock a second time.
 */
export async function purchase(
  db: Db,
  request: PurchaseRequest,
): Promise<ExclusiveWriteResult<PurchaseReceipt>> {
  // Create the client before beginning an exclusive transaction. This is also
  // the app's minimal connected preflight; an exclusive checkout is not an
  // offline cart operation.
  await db.all(app.warehouses.where({ id: request.warehouseId }).limit(1), { tier: "edge" });

  return db.exclusiveTransaction(async (tx) => {
    const existing = await tx.one(
      app.orders
        .where({ warehouse_id: request.warehouseId, idempotency_key: request.idempotencyKey })
        .limit(1),
    );
    if (existing) {
      return {
        orderId: existing.id,
        orderNumber: existing.order_number,
        totalCents: existing.total_cents,
      };
    }

    const [stock, district, customer, item] = await Promise.all([
      tx.one(
        app.stock.where({ warehouse_id: request.warehouseId, item_id: request.itemId }).limit(1),
      ),
      tx.one(app.districts.where({ id: request.districtId }).limit(1)),
      tx.one(app.customers.where({ id: request.customerId }).limit(1)),
      tx.one(app.items.where({ id: request.itemId }).limit(1)),
    ]);
    if (!stock || !district || !customer || !item) throw new Error("checkout rows are missing");
    if (!Number.isSafeInteger(request.quantity) || request.quantity <= 0) {
      throw new Error("quantity must be a positive integer");
    }
    if (request.quantity > stock.on_hand) throw new Error("insufficient stock");

    const totalCents = request.quantity * item.unit_price_cents;
    if (!Number.isSafeInteger(totalCents)) throw new Error("total exceeds safe integer range");
    const nextBalance = customer.balance_cents - totalCents;
    const nextOrderNumber = district.next_order_number + 1;
    if (!Number.isSafeInteger(nextBalance) || !Number.isSafeInteger(nextOrderNumber)) {
      throw new Error("checkout counter exceeds safe integer range");
    }

    tx.update(app.stock, stock.id, { on_hand: stock.on_hand - request.quantity });
    tx.update(app.districts, district.id, { next_order_number: nextOrderNumber });
    tx.update(app.customers, customer.id, { balance_cents: nextBalance });
    const order = tx.insert(app.orders, {
      warehouse_id: request.warehouseId,
      district_id: request.districtId,
      customer_id: request.customerId,
      order_number: district.next_order_number,
      status: "pending",
      total_cents: totalCents,
      idempotency_key: request.idempotencyKey,
    });
    tx.insert(app.order_lines, {
      order_id: order.id,
      item_id: request.itemId,
      quantity: request.quantity,
      amount_cents: totalCents,
    });
    tx.insert(app.payments, {
      customer_id: request.customerId,
      order_id: order.id,
      amount_cents: totalCents,
      idempotency_key: request.idempotencyKey,
    });
    return { orderId: order.id, orderNumber: order.order_number, totalCents };
  });
}
