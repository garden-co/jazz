import { createFileRoute } from "@tanstack/react-router";
import { useAll, useSession } from "jazz-tools/react";
import { app } from "../../schema/app";

export const Route = createFileRoute("/checkout")({
  component: CheckoutPage,
  ssr: false,
});

function CheckoutPage() {
  const session = useSession();
  const ownerId = session?.user_id;

  const cartItems = useAll(
    app.cart_items.where({ owner_id: ownerId }).include({ product: true }).orderBy("id", "asc"),
  );

  console.log("cartItems", cartItems);

  return (
    <div>
      {cartItems?.map((item) => (
        <div key={item.id}>
          <img src={item.product?.image_url} alt={item.product?.name} />
          <p>{item.product?.name}</p>
          <p>{item.quantity}</p>
        </div>
      ))}

      <pre>{JSON.stringify(cartItems, null, 2)}</pre>
    </div>
  );
}
