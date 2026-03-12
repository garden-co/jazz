import { col, table } from "jazz-tools";

table("products", {
  name: col.string(),
  brand: col.string(),
  category: col.string(),
  description: col.string(),
  image_url: col.string(),
  price_cents: col.int(),
  rating: col.float(),
  in_stock: col.int(),
});

table("cart_items", {
  owner_id: col.string(),
  product: col.ref("products"),
  quantity: col.int(),
});

table("orders", {
  owner_id: col.string(),
  created_at: col.timestamp(),
  total_cents: col.int(),
  item_count: col.int(),
});

table("order_items", {
  order: col.ref("orders"),
  product: col.ref("products"),
  quantity: col.int(),
  unit_price_cents: col.int(),
});
