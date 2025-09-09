import { useAccount } from "jazz-tools/react";
import { DraftIndicator } from "./DraftIndicator.tsx";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { getLastDraftId, JazzAccount } from "./schema.ts";
import { useIframeHashRouter } from "hash-slash";

export function Orders() {
  const router = useIframeHashRouter();

  const { me } = useAccount(JazzAccount, {
    resolve: {
      root: {
        draft: true,
        orders: {
          $each: {
            addOns: true,
          },
        },
      },
    },
  });

  const orders = me?.root.orders;
  const hasOrders = orders?.length;

  const handleCreateOrder = () => {
    if (!me) return;

    router.navigate(`/#/new-order/${getLastDraftId(me.root)}`);
  };

  return (
    <>
      <section className="space-y-5">
        <div className="space-y-3">
          <h1 className="text-lg pb-2 border-b mb-3 border-stone-200 dark:border-stone-700">
            <strong>Your orders 🧋</strong>
          </h1>

          {hasOrders ? (
            orders.map((order) =>
              order ? (
                <OrderThumbnail key={order.$jazz.id} order={order} />
              ) : null,
            )
          ) : (
            <p>You have no orders yet.</p>
          )}
        </div>
      </section>
      <button
        onClick={handleCreateOrder}
        className="block relative p-3 bg-white border border-stone-200 text-center rounded-md dark:bg-stone-900 dark:border-stone-900"
      >
        <strong>Add new order</strong>
        <DraftIndicator />
      </button>
    </>
  );
}
