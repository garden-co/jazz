import type { Db } from "jazz-tools";
import { app, REORDER_LEVEL_CAP } from "../schema.js";

export interface WarehouseScope {
  warehouseId: string;
  districtId: string;
}

export interface PurchaseLine {
  itemId: string;
  quantity: number;
}

export interface PurchaseRequest extends WarehouseScope {
  customerId: string;
  lines: PurchaseLine[];
  /** One key per checkout attempt. Resubmitting the same key is always safe. */
  idempotencyKey: string;
}

export interface ReceiptLine {
  lineNumber: number;
  itemId: string;
  quantity: number;
  amountCents: number;
}

export interface PurchaseReceipt {
  orderId: string;
  orderNumber: number;
  totalCents: number;
  lines: ReceiptLine[];
}

/** Largest page any console read asks for. Every console read is bounded. */
export const PAGE_SIZE = 20;
/** Dashboard counters read at most this many rows and show "500+" above it. */
export const COUNT_CAP = 500;
/** TPC-C allows 5 to 15 lines; the console accepts up to 15. */
export const MAX_ORDER_LINES = 15;

/** Thrown when a checkout asks for more than a warehouse holds. */
export class InsufficientStockError extends Error {
  constructor(
    readonly itemId: string,
    readonly onHand: number,
    readonly requested: number,
  ) {
    super(`insufficient stock for item ${itemId}: ${onHand} on hand, ${requested} requested`);
    this.name = "InsufficientStockError";
  }
}

/**
 * The bounded operational reads used by the warehouse console. Keep these
 * together with checkout so the browser topology tests and the UI use the
 * same bounded reads. Complete-state convergence reads belong in the
 * topology receipt, not in this public console API.
 */
export function warehouseQueries({ warehouseId, districtId }: WarehouseScope) {
  return {
    districts: consoleQueries.districtsOf(warehouseId),
    customers: app.customers
      .where({ warehouse_id: warehouseId, district_id: districtId })
      .orderBy("name", "asc")
      .limit(PAGE_SIZE),
    /** The delivery queue: oldest pending order first, one bounded page. */
    pendingOrders: app.orders
      .where({ warehouse_id: warehouseId, district_id: districtId, status: "pending" })
      .orderBy("order_number", "asc")
      .limit(PAGE_SIZE),
    orders: app.orders
      .where({ warehouse_id: warehouseId, district_id: districtId })
      .orderBy("order_number", "asc")
      .limit(PAGE_SIZE),
  };
}

export const consoleQueries = {
  warehouses: app.warehouses.orderBy("name", "asc").limit(PAGE_SIZE),
  items: app.items.orderBy("sku", "asc").limit(100),
  districtsOf: (warehouseId: string) =>
    app.districts.where({ warehouse_id: warehouseId }).orderBy("name", "asc").limit(PAGE_SIZE),
  membershipsOf: (accountId: string) =>
    app.warehouse_operators.where({ account_id: accountId }).limit(PAGE_SIZE),
  operatorsOf: (warehouseId: string) =>
    app.warehouse_operators
      .where({ warehouse_id: warehouseId })
      .orderBy("name", "asc")
      .limit(PAGE_SIZE),
  /** Orders entered since `sinceMs`, capped at {@link COUNT_CAP}. */
  ordersSince: (warehouseId: string, sinceMs: number) =>
    app.orders.where({ warehouse_id: warehouseId, $createdAt: { gte: sinceMs } }).limit(COUNT_CAP),
  pendingInWarehouse: (warehouseId: string) =>
    app.orders.where({ warehouse_id: warehouseId, status: "pending" }).limit(COUNT_CAP),
  pendingQueue: (scope: WarehouseScope) =>
    warehouseQueries(scope).pendingOrders.include({ customer: true, order_linesViaOrder: true }),
  orderByNumber: ({ warehouseId, districtId }: WarehouseScope, orderNumber: number) =>
    app.orders
      .where({ warehouse_id: warehouseId, district_id: districtId, order_number: orderNumber })
      .include({ customer: true })
      .limit(1),
  recentOrdersOf: (customerId: string) =>
    app.orders
      .where({ customer_id: customerId })
      .orderBy("order_number", "desc")
      .include({ customer: true })
      .limit(5),
  linesOf: (orderId: string) =>
    app.order_lines
      .where({ order_id: orderId })
      .orderBy("line_number", "asc")
      .include({ item: true })
      .limit(MAX_ORDER_LINES),
  deliveriesOf: (warehouseId: string, orderId: string) =>
    app.deliveries.where({ warehouse_id: warehouseId, order_id: orderId }).limit(5),
  stockOf: (warehouseId: string) =>
    app.stock.where({ warehouse_id: warehouseId }).include({ item: true }).limit(100),
  /**
   * Candidates for the stock-level report. Jazz cannot yet compare two
   * columns in a query (#1864), so the report reads the indexed range
   * `on_hand < REORDER_LEVEL_CAP` and keeps rows with `on_hand < reorder_level`.
   * The permissions cap every reorder level at REORDER_LEVEL_CAP, which makes
   * this candidate set complete up to the page bound.
   */
  stockLevelCandidates: (warehouseId: string) =>
    app.stock
      .where({ warehouse_id: warehouseId, on_hand: { lt: REORDER_LEVEL_CAP } })
      .orderBy("on_hand", "asc")
      .include({ item: true })
      .limit(COUNT_CAP),
};

export function belowReorderLevel<Row extends { on_hand: number; reorder_level: number }>(
  rows: readonly Row[],
): Row[] {
  return rows.filter((row) => row.on_hand < row.reorder_level);
}

/** Start of the viewer's local day, used for "orders today". */
export function startOfToday(now = new Date()): number {
  return new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
}

/**
 * Stage one TPC-C "new order" in an exclusive transaction. The authority
 * validates the stock rows, district counter, customer balance, order, lines
 * and payment as one unit, so two operators racing for the last units see
 * exactly one success; the other retries, re-reads stock and fails with
 * {@link InsufficientStockError}. Repeating an already accepted request key
 * returns the original receipt without decrementing stock a second time.
 */
export async function purchase(db: Db, request: PurchaseRequest): Promise<PurchaseReceipt> {
  const lines = normalizeLines(request.lines);
  // Create the client before beginning an exclusive transaction. This is also
  // the app's minimal connected preflight; an exclusive checkout is not an
  // offline cart operation.
  await db.all(app.warehouses.where({ id: request.warehouseId }).limit(1), { tier: "global" });

  return await retryExclusive(
    db,
    async () => {
      const write = await db.exclusiveTransaction(async (tx) => {
        const existing = await tx.one(
          app.orders
            .where({ warehouse_id: request.warehouseId, idempotency_key: request.idempotencyKey })
            .limit(1),
        );
        if (existing) {
          const existingLines = await tx.all(
            app.order_lines
              .where({ order_id: existing.id })
              .orderBy("line_number", "asc")
              .limit(MAX_ORDER_LINES),
          );
          return {
            orderId: existing.id,
            orderNumber: existing.order_number,
            totalCents: existing.total_cents,
            lines: existingLines.map((line) => ({
              lineNumber: line.line_number,
              itemId: line.item_id,
              quantity: line.quantity,
              amountCents: line.amount_cents,
            })),
          };
        }

        const [district, customer] = await Promise.all([
          tx.one(app.districts.where({ id: request.districtId }).limit(1)),
          tx.one(app.customers.where({ id: request.customerId }).limit(1)),
        ]);
        if (!district || !customer) throw new Error("checkout rows are missing");
        // #1898: every row a checkout touches belongs to one warehouse. The
        // permissions enforce this at the authority; checking here as well
        // gives the operator a clear message instead of a generic rejection.
        if (
          district.warehouse_id !== request.warehouseId ||
          customer.warehouse_id !== request.warehouseId ||
          customer.district_id !== request.districtId
        ) {
          throw new Error("checkout rows belong to different warehouses or districts");
        }

        const stocked = await Promise.all(
          lines.map(async (line) => {
            const [stock, item] = await Promise.all([
              tx.one(
                app.stock
                  .where({ warehouse_id: request.warehouseId, item_id: line.itemId })
                  .limit(1),
              ),
              tx.one(app.items.where({ id: line.itemId }).limit(1)),
            ]);
            if (!stock || !item) throw new Error("item is not stocked in this warehouse");
            if (line.quantity > stock.on_hand) {
              throw new InsufficientStockError(item.id, stock.on_hand, line.quantity);
            }
            const amountCents = line.quantity * item.unit_price_cents;
            if (!Number.isSafeInteger(amountCents)) throw new Error("line exceeds safe range");
            return { ...line, stock, amountCents };
          }),
        );

        const totalCents = stocked.reduce((sum, line) => sum + line.amountCents, 0);
        const nextBalance = customer.balance_cents - totalCents;
        const nextOrderNumber = district.next_order_number + 1;
        if (
          !Number.isSafeInteger(totalCents) ||
          !Number.isSafeInteger(nextBalance) ||
          !Number.isSafeInteger(nextOrderNumber)
        ) {
          throw new Error("checkout counter exceeds safe integer range");
        }

        for (const line of stocked) {
          tx.update(app.stock, line.stock.id, { on_hand: line.stock.on_hand - line.quantity });
        }
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
        const receiptLines = stocked.map((line, index) => {
          tx.insert(app.order_lines, {
            warehouse_id: request.warehouseId,
            order_id: order.id,
            line_number: index + 1,
            item_id: line.itemId,
            quantity: line.quantity,
            amount_cents: line.amountCents,
          });
          return {
            lineNumber: index + 1,
            itemId: line.itemId,
            quantity: line.quantity,
            amountCents: line.amountCents,
          };
        });
        // The payment row is the idempotent hand-off to an external charge.
        tx.insert(app.payments, {
          warehouse_id: request.warehouseId,
          customer_id: request.customerId,
          order_id: order.id,
          amount_cents: totalCents,
          idempotency_key: request.idempotencyKey,
        });
        return {
          orderId: order.id,
          orderNumber: order.order_number,
          totalCents,
          lines: receiptLines,
        };
      });
      return await write.wait();
    },
    // Before a retry, bring the stock rows this checkout reads up to date so
    // the next attempt sees the competing order's decrement.
    () =>
      db.all(app.stock.where({ warehouse_id: request.warehouseId }).limit(100), {
        tier: "global",
      }),
  );
}

export interface DeliveredOrder {
  districtId: string;
  orderId: string;
  orderNumber: number;
}

/**
 * TPC-C "delivery": deliver the oldest pending order of every district in one
 * exclusive batch. Two operators pressing the button at once never deliver
 * the same order twice; the loser retries against the new queue heads.
 */
export async function deliverBatch(db: Db, warehouseId: string): Promise<DeliveredOrder[]> {
  await db.all(app.warehouses.where({ id: warehouseId }).limit(1), { tier: "global" });
  return await retryExclusive(db, async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const districts = await tx.all(consoleQueries.districtsOf(warehouseId));
      const heads = await Promise.all(
        districts.map((district) =>
          tx.one(warehouseQueries({ warehouseId, districtId: district.id }).pendingOrders.limit(1)),
        ),
      );
      const delivered: DeliveredOrder[] = [];
      for (const order of heads) {
        if (!order) continue;
        tx.update(app.orders, order.id, { status: "delivered" });
        tx.insert(app.deliveries, {
          warehouse_id: warehouseId,
          district_id: order.district_id,
          order_id: order.id,
          status: "delivered",
        });
        delivered.push({
          districtId: order.district_id,
          orderId: order.id,
          orderNumber: order.order_number,
        });
      }
      return delivered;
    });
    return await write.wait();
  });
}

export interface PaymentRequest extends WarehouseScope {
  customerId: string;
  amountCents: number;
  idempotencyKey: string;
}

/** TPC-C "payment": credit a customer's balance once per request key. */
export async function recordPayment(
  db: Db,
  request: PaymentRequest,
): Promise<{ paymentId: string; balanceCents: number }> {
  if (!Number.isSafeInteger(request.amountCents) || request.amountCents <= 0) {
    throw new Error("payment must be a positive amount");
  }
  await db.all(app.warehouses.where({ id: request.warehouseId }).limit(1), { tier: "global" });
  return await retryExclusive(db, async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const [existing, customer] = await Promise.all([
        tx.one(
          app.payments
            .where({ warehouse_id: request.warehouseId, idempotency_key: request.idempotencyKey })
            .limit(1),
        ),
        tx.one(app.customers.where({ id: request.customerId }).limit(1)),
      ]);
      if (!customer) throw new Error("customer is missing");
      if (existing) return { paymentId: existing.id, balanceCents: customer.balance_cents };
      if (
        customer.warehouse_id !== request.warehouseId ||
        customer.district_id !== request.districtId
      ) {
        throw new Error("customer belongs to another warehouse or district");
      }
      const balanceCents = customer.balance_cents + request.amountCents;
      if (!Number.isSafeInteger(balanceCents)) throw new Error("balance exceeds safe range");
      tx.update(app.customers, customer.id, { balance_cents: balanceCents });
      const payment = tx.insert(app.payments, {
        warehouse_id: request.warehouseId,
        customer_id: customer.id,
        amount_cents: request.amountCents,
        idempotency_key: request.idempotencyKey,
      });
      return { paymentId: payment.id, balanceCents };
    });
    return await write.wait();
  });
}

/** Receive units into one stock row. Exclusive, so concurrent receipts add up. */
export async function restock(db: Db, stockId: string, quantity: number): Promise<number> {
  if (!Number.isSafeInteger(quantity) || quantity <= 0) {
    throw new Error("quantity must be a positive integer");
  }
  return await retryExclusive(db, async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const stock = await tx.one(app.stock.where({ id: stockId }).limit(1));
      if (!stock) throw new Error("stock row is missing");
      const onHand = stock.on_hand + quantity;
      tx.update(app.stock, stock.id, { on_hand: onHand });
      return onHand;
    });
    return await write.wait();
  });
}

function normalizeLines(lines: readonly PurchaseLine[]): PurchaseLine[] {
  if (lines.length === 0) throw new Error("an order needs at least one line");
  const merged = new Map<string, number>();
  for (const line of lines) {
    if (!Number.isSafeInteger(line.quantity) || line.quantity <= 0) {
      throw new Error("quantity must be a positive integer");
    }
    merged.set(line.itemId, (merged.get(line.itemId) ?? 0) + line.quantity);
  }
  if (merged.size > MAX_ORDER_LINES) {
    throw new Error(`an order has at most ${MAX_ORDER_LINES} lines`);
  }
  return [...merged].map(([itemId, quantity]) => ({ itemId, quantity }));
}

const MAX_ATTEMPTS = 8;

/**
 * Re-run an exclusive transaction the authority rejected because a
 * concurrent write changed what it read. Every other error is final.
 */
async function retryExclusive<T>(
  db: Db,
  attempt: () => Promise<T>,
  beforeRetry?: () => Promise<unknown>,
): Promise<T> {
  for (let tries = 1; ; tries++) {
    try {
      return await attempt();
    } catch (error) {
      if (!isExclusiveConflict(error) || tries >= MAX_ATTEMPTS) throw error;
      await (beforeRetry ? beforeRetry() : db.all(app.warehouses.limit(1), { tier: "global" }));
    }
  }
}

function isExclusiveConflict(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /exclusive_conflict|transaction_conflict|cascade_rejected/.test(message);
}
