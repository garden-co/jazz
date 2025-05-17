import { useIframeHashRouter } from "hash-slash";
import { useAccount, useCoState } from "jazz-react";
import { Loaded } from "jazz-tools";
import { useState } from "react";
import { Errors } from "./Errors.tsx";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import {
  BubbleTeaOrder,
  DraftBubbleTeaOrder,
  JazzAccount,
  ListOfBubbleTeaAddOns,
} from "./schema.ts";

export function CreateOrder() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { draft: true, orders: true } },
  });
  const router = useIframeHashRouter();
  const [errors, setErrors] = useState<string[]>([]);

  if (!me?.root) return;

  const onSave = (draft: Loaded<typeof DraftBubbleTeaOrder>) => {
    // validate if the draft is a valid order
    const validation = DraftBubbleTeaOrder.validate(draft);
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    // turn the draft into a real order
    me.root.orders.push(draft as Loaded<typeof BubbleTeaOrder>);

    // reset the draft
    me.root.draft = DraftBubbleTeaOrder.create({
      addOns: ListOfBubbleTeaAddOns.create([]),
    });

    router.navigate("/");
  };

  return (
    <>
      <LinkToHome />

      <h1 className="text-lg">
        <strong>Make a new bubble tea order 🧋</strong>
      </h1>

      <Errors errors={errors} />

      <CreateOrderForm id={me?.root?.draft.id} onSave={onSave} />
    </>
  );
}

function CreateOrderForm({
  id,
  onSave,
}: {
  id: string;
  onSave: (draft: Loaded<typeof DraftBubbleTeaOrder>) => void;
}) {
  const draft = useCoState(DraftBubbleTeaOrder, id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!draft) return;

  const addOrder = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    onSave(draft);
  };

  return <OrderForm order={draft} onSave={addOrder} />;
}
