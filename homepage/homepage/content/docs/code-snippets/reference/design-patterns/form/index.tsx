import { co, z } from "jazz-tools";

const Order = co.map({
  name: z.string(),
});

const order = Order.create({ name: "" });

// #region Basic
<input
  type="text"
  value={order.name}
  onChange={(e) => order.$jazz.set("name", e.target.value)}
/>;
// #endregion
