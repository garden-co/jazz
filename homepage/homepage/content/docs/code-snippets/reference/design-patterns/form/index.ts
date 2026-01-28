import { co, ID, z } from "jazz-tools";
const Order = co.map({
  name: z.string(),
});

const order = Order.create({ name: "" });

// #region Basic
const input = Object.assign(document.createElement("input"), {
  type: "text",
  value: order.name,
  onchange: (evt: InputEvent) => {
    const target = evt.currentTarget as HTMLInputElement;
    order.$jazz.set("name", target.value);
  },
});
// #endregion

// #region OrderForm
import { BubbleTeaOrder, JazzAccount, PartialBubbleTeaOrder } from "./schema";
const OrderForm = ({
  order,
  onSave,
}: {
  order:
    | co.loaded<typeof BubbleTeaOrder>
    | co.loaded<typeof PartialBubbleTeaOrder>;
  onSave?: (evt: Event) => void;
}) => {
  const form = Object.assign(document.createElement("form"), {
    type: "text",
    value: order.name,
    onsubmit: onSave || ((evt) => evt.preventDefault()),
  });
  const label = Object.assign(document.createElement("label"), {
    htmlFor: "name",
    textContent: "Name:",
  });
  const input = Object.assign(document.createElement("input"), {
    type: "text",
    value: order.name,
    onchange: (evt: InputEvent) => {
      const target = evt.currentTarget as HTMLInputElement;
      order.$jazz.set("name", target.value);
    },
  });
  form.append(label, input);
  return form;
};
// #endregion

// #region EditForm
const EditForm = async (orderId: ID<typeof BubbleTeaOrder>) => {
  const order = await BubbleTeaOrder.load(orderId);
  if (!order.$isLoaded) return;
  OrderForm({ order });
};
// #endregion

// #region EditFormWithSave
const EditFormWithSave = async (orderId: ID<typeof BubbleTeaOrder>) => {
  const owner = co.group().create();
  const order = await BubbleTeaOrder.load(orderId, {
    resolve: {
      addOns: { $each: true, $onError: "catch" },
      instructions: true,
    },
    unstable_branch: {
      name: "edit-order",
      owner,
    },
  });
  function handleSave(e: Event) {
    e.preventDefault();
    if (!order.$isLoaded) return;

    // Merge the branch back to the original
    order.$jazz.unstable_merge();
    // Navigate away or show success message
  }
  if (!order.$isLoaded) return;
  OrderForm({ order, onSave: handleSave });
};
// #endregion

// #region CreateForm
export async function CreateOrderForm(id: ID<typeof BubbleTeaOrder>) {
  const me = JazzAccount.getMe();
  const {
    root: { orders },
  } = await me.$jazz.ensureLoaded({
    resolve: { root: { orders: true } },
  });
  if (!orders.$isLoaded) {
    throw new Error("Failed to load orders");
  }

  const newOrder = await PartialBubbleTeaOrder.load(id);
  if (!newOrder.$isLoaded || !orders) return;

  const handleSave = (evt: Event) => {
    evt.preventDefault();

    // Convert to real order and add to the list
    // Note: the name field is marked as required in the form, so we can assume that has been set in this case
    // In a more complex form, you would need to validate the partial value before storing it
    orders.$jazz.push(newOrder as co.loaded<typeof BubbleTeaOrder>);
  };

  return OrderForm({ order: newOrder, onSave: handleSave });
}
// #endregion
