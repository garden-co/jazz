import { useIframeHashRouter } from "hash-slash";
import { useAccount, useCoState } from "jazz-tools/react";
import { useState } from "react";
import { Errors } from "./Errors.tsx";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import {
  BubbleTeaOrder,
  DraftBubbleTeaOrder,
  JazzAccount,
  validateDraftOrder,
} from "./schema.ts";

export function CreateOrder() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { draft: true, orders: true } },
  });
  const router = useIframeHashRouter();
  const [errors, setErrors] = useState<string[]>([]);

  if (!me?.root) return;

  const onSave = (draft: DraftBubbleTeaOrder) => {
    const validation = validateDraftOrder(draft);
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    // turn the draft into a real order
    me.root.orders.$jazz.push(draft as BubbleTeaOrder);

    // reset the draft
    me.root.$jazz.set("draft", { addOns: [] });

    router.navigate("/");
  };

  return (
    <>
      <LinkToHome />

      <h1 className="text-lg">
        <strong>Make a new bubble tea order 🧋</strong>
      </h1>

      <Errors errors={errors} />

      <CreateOrderForm id={me?.root?.draft.$jazz.id} onSave={onSave} />
    </>
  );
}

function CreateOrderForm({
  id,
  onSave,
}: {
  id: string;
  onSave: (draft: DraftBubbleTeaOrder) => void;
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
