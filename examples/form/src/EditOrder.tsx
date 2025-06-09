import { useCoState } from "jazz-tools/react";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderFormWithSaveButton } from "./OrderFormWithSaveButton.tsx";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { BubbleTeaOrder } from "./schema.ts";

export function EditOrder(props: { id: string }) {
  const order = useCoState(BubbleTeaOrder, props.id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!order) return;

  return (
    <>
      <LinkToHome />

      <OrderThumbnail order={order} />

      <h1 className="text-lg">
        <strong>Edit your bubble tea order 🧋</strong>
      </h1>

      <OrderFormWithSaveButton order={order} />
    </>
  );
}
