import { useCoState } from "jazz-react";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { BubbleTeaOrder } from "./schema.ts";

export function EditOrder(props: { id: string }) {
  const order = useCoState(BubbleTeaOrder, props.id);

  if (!order) return;

  return (
    <>
      <LinkToHome />

      <OrderThumbnail order={order} />

      <h1 className="text-lg">
        <strong>Edit your bubble tea order 🧋</strong>
      </h1>

      <OrderForm order={order} />
    </>
  );
}
