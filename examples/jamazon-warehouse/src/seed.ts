import type { Db } from "jazz-tools";
import { app } from "../schema.js";

/**
 * The deterministic "small" seed profile: two warehouses, three districts
 * each, four customers per district, a sixteen-item catalogue, stock for every
 * item in both warehouses and a few pending and delivered orders per
 * district. Every id is derived from a stable name, so every run seeds the
 * same rows and re-running the bootstrap is a no-op.
 */

/** Deterministic RFC 4122-shaped UUID for a seed row name (FNV-1a based). */
export function seedId(name: string): string {
  const words = [0x811c9dc5, 0x01000193, 0x050c5d1f, 0x1b873593].map((offset) => {
    let hash = offset >>> 0;
    for (const char of `jamazon-seed:${name}`) {
      hash ^= char.codePointAt(0)!;
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash.toString(16).padStart(8, "0");
  });
  const hex = words.join("").split("");
  hex[12] = "5";
  hex[16] = ((parseInt(hex[16]!, 16) & 0x3) | 0x8).toString(16);
  const h = hex.join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/**
 * Seed rows are owned by this placeholder manager account. No one signs in as
 * it; operators get their authority from `warehouse_operators` rows.
 */
export const SEED_MANAGER = seedId("manager");

export const SEED_WAREHOUSES = [
  { id: seedId("warehouse:east"), name: "East instruments", region: "east" },
  { id: seedId("warehouse:west"), name: "West instruments", region: "west" },
] as const;

const DISTRICT_NAMES = ["Central", "Harbour", "Uptown"] as const;
const CUSTOMER_NAMES = [
  "Ada Okafor",
  "Bruno Silva",
  "Chen Wei",
  "Dara Byrne",
  "Elif Aydın",
  "Farah Haddad",
  "Gus Lindqvist",
  "Hana Sato",
  "Ines Moreau",
  "Jonas Weber",
  "Kemi Adeyemi",
  "Luca Romano",
] as const;

export const SEED_ITEMS = [
  { sku: "JAM-001", name: "Electric guitar strings, 10–46", unit_price_cents: 899 },
  { sku: "JAM-002", name: "Drumsticks 5A, pair", unit_price_cents: 1_299 },
  { sku: "JAM-003", name: "Guitar picks, pack of 12", unit_price_cents: 499 },
  { sku: "JAM-004", name: "Trigger capo", unit_price_cents: 1_999 },
  { sku: "JAM-005", name: "Clip-on tuner", unit_price_cents: 2_499 },
  { sku: "JAM-006", name: "Alto sax reeds, box of 10", unit_price_cents: 3_199 },
  { sku: "JAM-007", name: "Valve oil", unit_price_cents: 799 },
  { sku: "JAM-008", name: "Violin rosin", unit_price_cents: 1_099 },
  { sku: "JAM-009", name: "Instrument cable, 3 m", unit_price_cents: 2_299 },
  { sku: "JAM-010", name: "Boom microphone stand", unit_price_cents: 4_999 },
  { sku: "JAM-011", name: "XLR cable, 6 m", unit_price_cents: 2_799 },
  { sku: "JAM-012", name: "Snare drum head, 14 in", unit_price_cents: 3_499 },
  { sku: "JAM-013", name: "Keyboard sustain pedal", unit_price_cents: 2_999 },
  { sku: "JAM-014", name: "Studio headphones", unit_price_cents: 9_900 },
  { sku: "JAM-015", name: "Gig bag, electric guitar", unit_price_cents: 5_900 },
  { sku: "JAM-016", name: "Vintage tube amp", unit_price_cents: 89_900 },
].map((item) => ({ ...item, id: seedId(`item:${item.sku}`) }));

/** The last item is scarce on purpose: two operators ordering 4 each contend for 5 units. */
export const CONTENDED_SKU = "JAM-016";

const FIRST_ORDER_NUMBER = 3001;

export function seedDistrictId(warehouseIndex: number, districtIndex: number) {
  return seedId(`district:${warehouseIndex}:${districtIndex}`);
}

/** Build every seed row. Pure, so tests can assert the profile's shape. */
export function seedRows() {
  const warehouses = SEED_WAREHOUSES.map((warehouse) => ({
    id: warehouse.id,
    name: warehouse.name,
    region: warehouse.region,
    operator_id: SEED_MANAGER,
  }));
  const items = SEED_ITEMS.map((item) => ({ ...item, operator_id: SEED_MANAGER }));
  const districts: Array<{
    id: string;
    warehouse_id: string;
    name: string;
    next_order_number: number;
  }> = [];
  const customers: Array<{
    id: string;
    warehouse_id: string;
    district_id: string;
    name: string;
    balance_cents: number;
  }> = [];
  const stock: Array<{
    id: string;
    warehouse_id: string;
    item_id: string;
    on_hand: number;
    reorder_level: number;
  }> = [];
  const orders: Array<{
    id: string;
    warehouse_id: string;
    district_id: string;
    customer_id: string;
    order_number: number;
    status: string;
    total_cents: number;
    idempotency_key: string;
  }> = [];
  const orderLines: Array<{
    id: string;
    warehouse_id: string;
    order_id: string;
    line_number: number;
    item_id: string;
    quantity: number;
    amount_cents: number;
  }> = [];
  const payments: Array<{
    id: string;
    warehouse_id: string;
    customer_id: string;
    order_id: string;
    amount_cents: number;
    idempotency_key: string;
  }> = [];
  const deliveries: Array<{
    id: string;
    warehouse_id: string;
    district_id: string;
    order_id: string;
    status: string;
  }> = [];

  SEED_WAREHOUSES.forEach((warehouse, w) => {
    const onHand = new Map<string, number>();
    SEED_ITEMS.forEach((item, i) => {
      // Every fifth item runs low; the contended item holds exactly five.
      const low = i % 5 === 3;
      const start = item.sku === CONTENDED_SKU ? 5 : low ? 12 + w : 60 + ((i * 7 + w * 11) % 40);
      onHand.set(item.id, start);
    });

    DISTRICT_NAMES.forEach((districtName, d) => {
      const districtId = seedDistrictId(w, d);
      const districtCustomers = [0, 1, 2, 3].map((c) => {
        const name = CUSTOMER_NAMES[(w * 6 + d * 4 + c) % CUSTOMER_NAMES.length]!;
        return {
          id: seedId(`customer:${w}:${d}:${c}`),
          warehouse_id: warehouse.id,
          district_id: districtId,
          name,
          balance_cents: 0,
        };
      });

      // Five orders per district: the first two delivered, three pending.
      let orderNumber = FIRST_ORDER_NUMBER;
      for (let o = 0; o < 5; o++, orderNumber++) {
        const customer = districtCustomers[o % districtCustomers.length]!;
        const orderId = seedId(`order:${w}:${d}:${o}`);
        const key = `seed-${w}-${d}-${o}`;
        let total = 0;
        const lineCount = 1 + ((o + d) % 3);
        for (let l = 0; l < lineCount; l++) {
          // Seed orders never touch the contended item, so it stays at five.
          const item = SEED_ITEMS[(o * 3 + l * 5 + d + w) % (SEED_ITEMS.length - 1)]!;
          const quantity = 1 + ((o + l) % 3);
          const amount = quantity * item.unit_price_cents;
          total += amount;
          onHand.set(item.id, onHand.get(item.id)! - quantity);
          orderLines.push({
            id: seedId(`line:${w}:${d}:${o}:${l}`),
            warehouse_id: warehouse.id,
            order_id: orderId,
            line_number: l + 1,
            item_id: item.id,
            quantity,
            amount_cents: amount,
          });
        }
        const delivered = o < 2;
        orders.push({
          id: orderId,
          warehouse_id: warehouse.id,
          district_id: districtId,
          customer_id: customer.id,
          order_number: orderNumber,
          status: delivered ? "delivered" : "pending",
          total_cents: total,
          idempotency_key: key,
        });
        payments.push({
          id: seedId(`payment:${w}:${d}:${o}`),
          warehouse_id: warehouse.id,
          customer_id: customer.id,
          order_id: orderId,
          amount_cents: total,
          idempotency_key: key,
        });
        if (delivered) {
          deliveries.push({
            id: seedId(`delivery:${w}:${d}:${o}`),
            warehouse_id: warehouse.id,
            district_id: districtId,
            order_id: orderId,
            status: "delivered",
          });
        }
        customer.balance_cents -= total;
      }

      districts.push({
        id: districtId,
        warehouse_id: warehouse.id,
        name: districtName,
        next_order_number: orderNumber,
      });
      customers.push(...districtCustomers);
    });

    SEED_ITEMS.forEach((item, i) => {
      stock.push({
        id: seedId(`stock:${w}:${item.sku}`),
        warehouse_id: warehouse.id,
        item_id: item.id,
        on_hand: onHand.get(item.id)!,
        reorder_level: item.sku === CONTENDED_SKU ? 2 : 10 + (i % 3) * 5,
      });
    });
  });

  return {
    warehouses,
    items,
    districts,
    customers,
    stock,
    orders,
    orderLines,
    payments,
    deliveries,
  };
}

/**
 * Insert the seed profile once. Runs with backend authority in the trusted
 * bootstrap route; the exclusive transaction makes concurrent first requests
 * converge on a single seed.
 */
export async function ensureSeed(db: Db): Promise<"seeded" | "present"> {
  const rows = seedRows();
  return await retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const existing = await tx.one(app.warehouses.where({ id: rows.warehouses[0]!.id }).limit(1));
      if (existing) return "present" as const;
      const insertAll = <T extends { id: string }>(
        table: Parameters<typeof tx.insert>[0],
        list: T[],
      ) => {
        for (const { id, ...data } of list) tx.insert(table, data as never, { id });
      };
      insertAll(app.warehouses, rows.warehouses);
      insertAll(app.items, rows.items);
      insertAll(app.districts, rows.districts);
      insertAll(app.customers, rows.customers);
      insertAll(app.stock, rows.stock);
      insertAll(app.orders, rows.orders);
      insertAll(app.order_lines, rows.orderLines);
      insertAll(app.payments, rows.payments);
      insertAll(app.deliveries, rows.deliveries);
      return "seeded" as const;
    });
    return await write.wait();
  });
}

/**
 * Staff an account on a warehouse. An operator belongs to one warehouse, so an
 * existing membership wins over the requested warehouse.
 */
export async function ensureOperator(
  db: Db,
  { accountId, name, warehouseId }: { accountId: string; name: string; warehouseId: string },
): Promise<{ warehouseId: string }> {
  return await retryOnConflict(async () => {
    const write = await db.exclusiveTransaction(async (tx) => {
      const existing = await tx.one(
        app.warehouse_operators.where({ account_id: accountId }).limit(1),
      );
      if (existing) return { warehouseId: existing.warehouse_id };
      const warehouse = await tx.one(app.warehouses.where({ id: warehouseId }).limit(1));
      if (!warehouse) throw new Error("unknown warehouse");
      tx.insert(app.warehouse_operators, {
        warehouse_id: warehouseId,
        account_id: accountId,
        name,
      });
      return { warehouseId };
    });
    return await write.wait();
  });
}

async function retryOnConflict<T>(attempt: () => Promise<T>): Promise<T> {
  for (let tries = 1; ; tries++) {
    try {
      return await attempt();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (tries >= 8 || !/exclusive_conflict|transaction_conflict|cascade_rejected/.test(message)) {
        throw error;
      }
    }
  }
}
