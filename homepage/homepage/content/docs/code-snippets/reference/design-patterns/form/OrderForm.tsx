// #region OrderForm
import { co } from "jazz-tools";
import { BubbleTeaOrder, PartialBubbleTeaOrder } from "./schema";

export function OrderForm({
  order,
  onSave,
}: {
  order: BubbleTeaOrder | PartialBubbleTeaOrder;
  onSave?: (e: React.FormEvent<HTMLFormElement>) => void;
}) {
  return (
    <form onSubmit={onSave || ((e) => e.preventDefault())}>
      <label>
        Name
        <input
          type="text"
          value={order.name}
          onChange={(e) => order.$jazz.set("name", e.target.value)}
          required
        />
      </label>

      {onSave && <button type="submit">Submit</button>}
    </form>
  );
}
// #endregion
import { useAccount, useCoState } from "jazz-tools/react";
// #region EditForm
export function EditOrder(props: { id: string }) {
  const order = useCoState(BubbleTeaOrder, props.id);

  if (!order.$isLoaded) return;

  return <OrderForm order={order} />;
}
// #endregion
const JazzAccount = co.account();
// #region CreateOrder
export function CreateOrder(props: { id: string }) {
  const orders = useAccount(JazzAccount, {
    resolve: { root: { orders: true } },
    select: (account) => (account.$isLoaded ? account.root.orders : undefined),
  });

  const newOrder = useCoState(PartialBubbleTeaOrder, props.id);

  if (!newOrder.$isLoaded || !orders) return;

  const handleSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    // Convert to real order and add to the list
    // Note: the name field is marked as required in the form, so we can assume that has been set in this case
    // In a more complex form, you would need to validate the partial value before storing it
    orders.$jazz.push(newOrder as BubbleTeaOrder);
  };

  return <OrderForm order={newOrder} onSave={handleSave} />;
}
// #endregion

// #region EditOrderWithSave
import { Group } from "jazz-tools";
import { useState, useMemo } from "react";

export function EditOrderWithSave(props: { id: string }) {
  // Create a new group for the branch, so that every time we open the edit page,
  // we create a new private branch
  const owner = useMemo(() => Group.create(), []);

  const order = useCoState(BubbleTeaOrder, props.id, {
    resolve: {
      addOns: { $each: true, $onError: "catch" },
      instructions: true,
    },
    unstable_branch: {
      name: "edit-order",
      owner,
    },
  });

  function handleSave(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (!order.$isLoaded) return;

    // Merge the branch back to the original
    order.$jazz.unstable_merge();
    // Navigate away or show success message
  }

  function handleCancel() {
    // Navigate away without saving - the branch will be discarded
  }

  if (!order.$isLoaded) return;

  return <OrderForm order={order} onSave={handleSave} />;
}
// #endregion
