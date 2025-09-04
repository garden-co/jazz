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

  const draft = useCoState(DraftBubbleTeaOrder, me?.root.draft.$jazz.id, {
    resolve: { addOns: true, instructions: true },
  });

  if (!me?.root) return;

  const handleCancel = () => {
    me.root.$jazz.set("draft", { addOns: [] });
    router.navigate("/");
  };

  const handleSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!draft) return;

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

      {draft && (
        <OrderForm order={draft} onSave={handleSave} onCancel={handleCancel} />
      )}
    </>
  );
}
