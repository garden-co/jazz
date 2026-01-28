import { co, z } from "jazz-tools";
import { TextInput } from "react-native";
const Order = co.map({
  name: z.string(),
});

const order = Order.create({ name: "" });

export function OrderForm() {
  /* prettier-ignore */
  return (
<>
{/* #region Basic */}
<input
  type="text"
  value={order.name}
  onChange={(evt) => order.$jazz.set("name", evt.target.value)}
/>
{/* #endregion */}
</>
);
}
export function OrderFormRN() {
  /* prettier-ignore */
  return (
<>
{/* #region BasicRN */}
<TextInput
  onChangeText={(v) => order.$jazz.set("name", v)}
/>
{/* #endregion */}
</>
);
}
