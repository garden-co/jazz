import { useCoState } from "jazz-tools/react";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm, OrderFormData } from "./OrderForm.tsx";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { BubbleTeaOrder } from "./schema.ts";
import { CoPlainText, Loaded } from "jazz-tools";

export type LoadedBubbleTeaOrder = Loaded<
  typeof BubbleTeaOrder,
  { addOns: { $each: true }; instructions: true }
>;

export function EditOrder(props: { id: string }) {
  const order = useCoState(BubbleTeaOrder, props.id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!order) return;

  const onSubmit = (updatedOrder: OrderFormData) => {
    // Apply changes to the original Jazz order
    order.baseTea = updatedOrder.baseTea;
    order.deliveryDate = updatedOrder.deliveryDate;
    order.withMilk = updatedOrder.withMilk;
    order.addOns.applyDiff(updatedOrder.addOns);

    // `applyDiff` requires nested objects to be CoValues as well
    order.instructions ??= CoPlainText.create("");
    if (updatedOrder.instructions) {
      order.instructions.applyDiff(updatedOrder.instructions);
    }
  };

  const originalOrder: OrderFormData = order.toJSON();
  // Convert timestamp to Date
  originalOrder.deliveryDate = new Date(originalOrder.deliveryDate);

  return (
    <>
      <LinkToHome />

      <div>
        <p>Saved order:</p>

        <OrderThumbnail order={order} />
      </div>

      <h1 className="text-lg">
        <strong>Edit your bubble tea order 🧋</strong>
      </h1>

      <OrderForm
        order={originalOrder}
        onSubmit={onSubmit}
        validateOn="change"
      />
    </>
  );
}
