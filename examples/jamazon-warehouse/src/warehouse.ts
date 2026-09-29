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

/**
 * Order lifecycle. A checkout reserves a `draft` order, then places it as
 * `pending` (in the delivery queue); delivery makes it `delivered`. A draft
 * whose second phase was rejected is `cancelled`, with its stock returned.
 */
export const ORDER_STATUS = {
  draft: "draft",
  pending: "pending",
  delivered: "delivered",
  cancelled: "cancelled",
} as const;

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
      .where({ warehouse_id: warehouseId, district_id: districtId, status: ORDER_STATUS.pending })
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
    app.orders.where({ warehouse_id: warehouseId, status: ORDER_STATUS.pending }).limit(COUNT_CAP),
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

/** Thrown when a request key belongs to a checkout that was cancelled. */
export class CheckoutCancelledError extends Error {
  constructor(readonly orderNumber: number) {
    super(`order ${orderNumber} was cancelled; start a new order`);
    this.name = "CheckoutCancelledError";
  }
}

interface DraftOrder {
  id: string;
  order_number: number;
  total_cents: number;
  customer_id: string;
}

/**
 * TPC-C "new order", in two exclusive phases.
 *
 * 1. Reserve: stock for every line, the district's order counter, the
 *    customer's balance and a `draft` order change together or not at all.
 *    Two operators racing for the last units see exactly one reservation; the
 *    other retries, re-reads stock and fails with {@link InsufficientStockError}.
 * 2. Place: the order's lines and payment hand-off are written against the
 *    now-committed order, and the order becomes `pending`.
 *
 * Checkout is two-phase because permission `exists` checks only see committed
 * rows (INV-RLS-9): the policies that prove a line or payment belongs to an
 * order of its own warehouse can't see an order staged in the same commit.
 *
 * Repeating a request key is always safe: a placed order returns its original
 * receipt, and a draft left by an interrupted checkout is placed. If the
 * authority rejects the second phase, the draft is cancelled and its stock
 * and balance returned; if that can't be confirmed either, the draft stays
 * visible and resubmitting the key finishes it.
 */
export async function purchase(db: Db, request: PurchaseRequest): Promise<PurchaseReceipt> {
  const lines = normalizeLines(request.lines);
  // Create the client before beginning an exclusive transaction. This is also
  // the app's minimal connected preflight; an exclusive checkout is not an
  // offline cart operation.
  await db.all(app.warehouses.where({ id: request.warehouseId }).limit(1), { tier: "global" });

  const reserved = await reserveOrder(db, request, lines);
  if ("receipt" in reserved) return reserved.receipt;
  try {
    return await placeOrder(db, request, reserved.draft, lines);
  } catch (error) {
    if (isDefinitiveRejection(error)) {
      await cancelDraft(db, request.warehouseId, reserved.draft.id, lines).catch(() => {
        // The draft stays visible; resubmitting the request key places it.
      });
    }
    throw error;
  }
}

async function reserveOrder(
  db: Db,
  request: PurchaseRequest,
  lines: PurchaseLine[],
): Promise<{ receipt: PurchaseReceipt } | { draft: DraftOrder }> {
  return await retryExclusive(
    db,
    async () => {
      const write = await db.exclusiveTransaction(
        async (tx): Promise<{ receipt: PurchaseReceipt } | { draft: DraftOrder }> => {
          const existing = await tx.one(
            app.orders
              .where({ warehouse_id: request.warehouseId, idempotency_key: request.idempotencyKey })
              .limit(1),
          );
          if (existing) {
            if (existing.status === ORDER_STATUS.cancelled) {
              throw new CheckoutCancelledError(existing.order_number);
            }
            if (existing.status === ORDER_STATUS.draft) return { draft: existing };
            const storedLines = await tx.all(
              app.order_lines
                .where({ order_id: existing.id })
                .orderBy("line_number", "asc")
                .limit(MAX_ORDER_LINES),
            );
            return { receipt: storedReceipt(existing, storedLines) };
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
              return { ...line, stock, amountCents: lineAmount(line, item.unit_price_cents) };
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
          const draft = tx.insert(app.orders, {
            warehouse_id: request.warehouseId,
            district_id: request.districtId,
            customer_id: request.customerId,
            order_number: district.next_order_number,
            status: ORDER_STATUS.draft,
            total_cents: totalCents,
            idempotency_key: request.idempotencyKey,
          });
          return { draft };
        },
      );
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

async function placeOrder(
  db: Db,
  request: PurchaseRequest,
  draft: DraftOrder,
  lines: PurchaseLine[],
): Promise<PurchaseReceipt> {
  return await retryExclusive(db, async () => {
    const write = await db.exclusiveTransaction(async (tx): Promise<PurchaseReceipt> => {
      const order = await tx.one(app.orders.where({ id: draft.id }).limit(1));
      if (!order) throw new Error("the reserved order is missing");
      if (order.status === ORDER_STATUS.cancelled) {
        throw new CheckoutCancelledError(order.order_number);
      }
      if (order.status !== ORDER_STATUS.draft) {
        // A concurrent submit of the same request placed it first.
        const storedLines = await tx.all(
          app.order_lines
            .where({ order_id: order.id })
            .orderBy("line_number", "asc")
            .limit(MAX_ORDER_LINES),
        );
        return storedReceipt(order, storedLines);
      }

      const priced = await Promise.all(
        lines.map(async (line, index) => {
          const item = await tx.one(app.items.where({ id: line.itemId }).limit(1));
          if (!item) throw new Error("item is missing");
          return {
            lineNumber: index + 1,
            itemId: line.itemId,
            quantity: line.quantity,
            amountCents: lineAmount(line, item.unit_price_cents),
          };
        }),
      );
      const totalCents = priced.reduce((sum, line) => sum + line.amountCents, 0);
      if (totalCents !== order.total_cents) {
        throw new Error("this request no longer matches its reserved order");
      }
      for (const line of priced) {
        tx.insert(app.order_lines, {
          warehouse_id: request.warehouseId,
          order_id: order.id,
          line_number: line.lineNumber,
          item_id: line.itemId,
          quantity: line.quantity,
          amount_cents: line.amountCents,
        });
      }
      // The payment row is the idempotent hand-off to an external charge.
      tx.insert(app.payments, {
        warehouse_id: request.warehouseId,
        customer_id: order.customer_id,
        order_id: order.id,
        amount_cents: totalCents,
        idempotency_key: request.idempotencyKey,
      });
      tx.update(app.orders, order.id, { status: ORDER_STATUS.pending });
      return {
        orderId: order.id,
        orderNumber: order.order_number,
        totalCents,
        lines: priced,
      };
    });
    return await write.wait();
  });
}

/** Return a rejected draft's stock and balance, and mark it cancelled. */
async function cancelDraft(
  db: Db,
  warehouseId: string,
  orderId: string,
  lines: PurchaseLine[],
): Promise<void> {
  await retryExclusive(db, async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const order = await tx.one(app.orders.where({ id: orderId }).limit(1));
      if (!order || order.status !== ORDER_STATUS.draft) return;
      const [customer, stock] = await Promise.all([
        tx.one(app.customers.where({ id: order.customer_id }).limit(1)),
        Promise.all(
          lines.map((line) =>
            tx.one(app.stock.where({ warehouse_id: warehouseId, item_id: line.itemId }).limit(1)),
          ),
        ),
      ]);
      lines.forEach((line, index) => {
        const row = stock[index];
        if (row) tx.update(app.stock, row.id, { on_hand: row.on_hand + line.quantity });
      });
      if (customer) {
        tx.update(app.customers, customer.id, {
          balance_cents: customer.balance_cents + order.total_cents,
        });
      }
      tx.update(app.orders, order.id, { status: ORDER_STATUS.cancelled });
    });
    await write.wait();
  });
}

function storedReceipt(
  order: { id: string; order_number: number; total_cents: number },
  lines: readonly {
    line_number: number;
    item_id: string;
    quantity: number;
    amount_cents: number;
  }[],
): PurchaseReceipt {
  return {
    orderId: order.id,
    orderNumber: order.order_number,
    totalCents: order.total_cents,
    lines: lines.map((line) => ({
      lineNumber: line.line_number,
      itemId: line.item_id,
      quantity: line.quantity,
      amountCents: line.amount_cents,
    })),
  };
}

function lineAmount(line: PurchaseLine, unitPriceCents: number): number {
  const amountCents = line.quantity * unitPriceCents;
  if (!Number.isSafeInteger(amountCents)) throw new Error("line exceeds safe range");
  return amountCents;
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
        tx.update(app.orders, order.id, { status: ORDER_STATUS.delivered });
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

/** The authority refused the write outright; retrying the same write can't help. */
function isDefinitiveRejection(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /permission_denied|AuthorizationDenied|write_rejected|Write rejected/.test(message);
}

function isExclusiveConflict(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /exclusive_conflict|transaction_conflict|cascade_rejected/.test(message);
}
