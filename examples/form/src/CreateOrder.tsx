import { useIframeHashRouter } from "hash-slash";
import { useAccount, useCoState } from "jazz-tools/react";
import { useState } from "react";
import { Errors } from "./Errors.tsx";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import {
  BubbleTeaOrder,
  DraftBubbleTeaOrder,
  getLastDraftId,
  hasChanges,
  JazzAccount,
  validateDraftOrder,
} from "./schema.ts";

export function CreateOrder(props: { id: string }) {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { draft: true, orders: true } },
  });
  const router = useIframeHashRouter();
  const [errors, setErrors] = useState<string[]>([]);

  const draft = useCoState(DraftBubbleTeaOrder, props.id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!draft) return;

  const handleSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!me) return;

    const validation = validateDraftOrder(draft);
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    // turn the draft into a real order
    me.root.orders.$jazz.push(draft as BubbleTeaOrder);

    // reset the draft
    me.root.$jazz.set("draft", undefined);

    router.navigate("/");
  };

  const handleReset = () => {
    if (!me) return;

    if (!hasChanges(draft)) {
      return;
    }

    me.root.$jazz.set("draft", undefined);
    router.navigate("/#/new-order/" + getLastDraftId(me.root));
  };

  return (
    <>
      <LinkToHome />

      <h1 className="text-lg">
        <strong>Make a new bubble tea order 🧋</strong>
      </h1>

      <Errors errors={errors} />

      {draft && (
        <OrderForm order={draft} onSave={handleSave} onReset={handleReset} />
      )}
    </>
  );
}
