import { CoValueLoadingState } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import { Group } from "jazz-tools";
import { LinkToHome } from "./LinkToHome.tsx";
import { OrderForm } from "./OrderForm.tsx";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { BubbleTeaOrder } from "./schema.ts";
import { useMemo } from "react";
import { useIframeHashRouter } from "hash-slash";

export function EditOrder(props: { id: string }) {
  const router = useIframeHashRouter();

  // Create a new group for the branch, so that every time we open the edit order page, we create a new private branch
  const owner = useMemo(() => Group.create(), []);

  const order = useCoState(BubbleTeaOrder, props.id, {
    resolve: {
      addOns: {
        $each: true,
      },
      instructions: true,
    },
    unstable_branch: {
      name: "edit-order",
      owner,
    },
  });

  if (order.$jazzState !== CoValueLoadingState.LOADED) return;
  const loadedOrder = order;

  function handleSave(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();

    loadedOrder.$jazz.unstable_merge();
    router.navigate("/");
  }

  function handleCancel() {
    router.navigate("/");
  }

  return (
    <>
      <LinkToHome />

      <OrderThumbnail order={order} />

      <h1 className="text-lg">
        <strong>Edit your bubble tea order 🧋</strong>
      </h1>

      <OrderForm order={order} onSave={handleSave} onCancel={handleCancel} />
    </>
  );
}
