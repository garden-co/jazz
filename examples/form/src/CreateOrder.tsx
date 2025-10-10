import { useIframeHashRouter } from "hash-slash";
import { useAccountWithSelector, useCoState } from "jazz-tools/react";
import { useState } from "react";
import { Errors } from "./Errors.tsx";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import {
  BubbleTeaOrder,
  JazzAccount,
  PartialBubbleTeaOrder,
  validatePartialBubbleTeaOrder,
} from "./schema.ts";

export function CreateOrder(props: { id: string }) {
  const orders = useAccountWithSelector(JazzAccount, {
    resolve: { root: { orders: true } },
    select: (account) => {
      if (!account.$isLoaded) {
        return undefined;
      }
      return account.root.orders;
    },
  });
  const router = useIframeHashRouter();
  const [errors, setErrors] = useState<string[]>([]);

  const newOrder = useCoState(PartialBubbleTeaOrder, props.id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!newOrder.$isLoaded) return;

  const handleSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!orders) return;

    const validation = validatePartialBubbleTeaOrder(newOrder);
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    // turn the draft into a real order
    orders.$jazz.push(newOrder as BubbleTeaOrder);

    router.navigate("/");
  };

  return (
    <>
      <LinkToHome />

      <h1 className="text-lg">
        <strong>Make a new bubble tea order 🧋</strong>
      </h1>

      <Errors errors={errors} />
      <OrderForm order={newOrder} onSave={handleSave} />
    </>
  );
}
