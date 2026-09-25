import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/jamazon-warehouse/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database";
const store = (orders: string) =>
  `One warehouse with two districts, one customer and three stocked items; ${orders} existing orders in the operator's district (a third of them pending).`;

export const jamazonBenchmarks: BenchmarkMetadata[] = [
  {
    name: "jamazon_checkout_100",
    title: "Jamazon checkout · 100 orders",
    description:
      "Place 100 orders one after another. Each checkout is one exclusive transaction that checks the request key, reads stock, the district's order counter and the customer's balance, then updates all three and inserts the order, its line and its payment.",
    fixture: `${store("1,000")} A fresh store per measured run.`,
    storage,
    includes: [
      "Idempotency lookup",
      "Transaction reads, three updates and three inserts",
      "Commit of each checkout",
    ],
    excludes: ["Schema compilation, database opening and seeding", "External payment effects"],
    work: {
      count: 100,
      unit: "checkouts/s",
      explanation: "100 sequential checkout transactions per run.",
    },
    source,
  },
  {
    name: "jamazon_checkout_retry",
    title: "Jamazon checkout retry",
    description:
      "Retry a checkout with a request key that already committed. The transaction finds the original order and returns its receipt without writing anything.",
    fixture: `${store("1,000")} The retried checkout committed once before measurement.`,
    storage,
    includes: [
      "Opening the exclusive transaction",
      "Indexed idempotency lookup",
      "Returning the receipt",
    ],
    excludes: ["The original checkout", "Seeding"],
    work: { count: 1, unit: "retries/s", explanation: "One retried checkout." },
    source,
  },
  {
    name: "jamazon_pending_orders_10k",
    title: "Jamazon pending orders · 10,000-order history",
    description:
      "Open the console's first page: the district's 20 oldest pending orders, ordered by order number.",
    fixture: store("10,000"),
    storage,
    includes: ["Prepared, ordered and limited indexed query", "Materializing 20 rows"],
    excludes: ["Seeding", "Query preparation", "Rendering"],
    work: { count: 1, unit: "page loads/s", explanation: "One 20-row console page." },
    source,
  },
];
