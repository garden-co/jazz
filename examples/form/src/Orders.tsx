import { useAccount } from "jazz-tools/react";
import { OrderThumbnail } from "./OrderThumbnail.tsx";
import { JazzAccount, PartialBubbleTeaOrder } from "./schema.ts";
import { useHashRouter } from "hash-slash";

export function Orders() {
  const router = useHashRouter();

  const orders = useAccount(JazzAccount, {
    resolve: {
      root: {
        orders: {
          $each: {
            addOns: true,
            instructions: true,
          },
        },
      },
    },
    select: (me) => {
      if (!me.$isLoaded) {
        return [];
      }
      return me.root.orders;
    },
  });

  const hasOrders = orders.length > 0;
  const createButtonText = hasOrders
    ? "Create a new order"
    : "Create your first order";

  const handleCreateOrder = () => {
    const order = PartialBubbleTeaOrder.create({
      addOns: [],
      instructions: "",
    });
    router.navigate(`/#/new-order/${order.$jazz.id}`);
  };

  return (
    <>
      {hasOrders && (
        <section className="space-y-5">
          <div className="space-y-3">
            <h1 className="text-lg pb-2 border-b mb-3 border-stone-200 dark:border-stone-700">
              <strong>Your orders 🧋</strong>
            </h1>

            {orders.map((order) => (
              <OrderThumbnail key={order.$jazz.id} order={order} />
            ))}
          </div>
        </section>
      )}
      <section className="flex gap-3">
        <button
          onClick={handleCreateOrder}
          className="p-3 bg-white border border-stone-200 text-center rounded-md dark:bg-stone-900 dark:border-stone-900 cursor-pointer"
        >
          <strong>{createButtonText}</strong>
        </button>
      </section>
    </>
  );
}
