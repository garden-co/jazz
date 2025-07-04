import { useIframeHashRouter } from "hash-slash";
import { CoPlainText } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm, OrderFormData } from "./OrderForm.tsx";
import {
  BubbleTeaOrder,
  JazzAccount,
  ListOfBubbleTeaAddOns,
} from "./schema.ts";

export function CreateOrder() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { orders: true } },
  });
  const router = useIframeHashRouter();

  if (!me?.root) return;

  const addOrder = (draft: OrderFormData) => {
    const newOrder = BubbleTeaOrder.create({
      baseTea: draft.baseTea,
      deliveryDate: draft.deliveryDate,
      withMilk: draft.withMilk,
      addOns: ListOfBubbleTeaAddOns.create(draft.addOns),
      instructions: draft.instructions
        ? CoPlainText.create(draft.instructions)
        : undefined,
    });

    me.root.orders.push(newOrder);

    router.navigate("/");
  };

  const draftOrder: Partial<OrderFormData> = {
    baseTea: "Black",
    addOns: [],
    withMilk: false,
  };

  return (
    <>
      <LinkToHome />

      <h1 className="text-lg">
        <strong>Make a new bubble tea order 🧋</strong>
      </h1>

      <OrderForm order={draftOrder} onSubmit={addOrder} validateOn="submit" />
    </>
  );
}
