import { formatCents } from "@/lib/format";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { Minus, Plus, ShoppingBag, Star, Trash2 } from "lucide-react";
import { app, type Product } from "../../schema/app";

export const Route = createFileRoute("/")({ component: Storefront, ssr: false });

function Storefront() {
  const db = useDb();
  const session = useSession();
  const navigate = useNavigate();
  const ownerId = session?.user_id;

  const products = useAll(app.products.orderBy("name", "asc"));

  const cartItems = useAll(
    app.cart_items.where({ owner_id: ownerId }).include({ product: true }).orderBy("id", "asc"),
  );

  const cartItemsWithoutProduct = useAll(
    app.cart_items.where({ owner_id: ownerId }).orderBy("id", "asc"),
  );

  function addToCart(product: Product) {
    if (!ownerId) return;

    console.log("cartItemsWithoutProduct", { cartItemsWithoutProduct, cartItems });

    const existingWithoutProduct = cartItemsWithoutProduct?.find(
      (item) => item.product?.id === product.id,
    );

    console.log("existingWithoutProduct", existingWithoutProduct);

    const existing = cartItems?.find((item) => item.product?.id === product.id);

    if (existing) {
      db.update(app.cart_items, existing.id, { quantity: existing.quantity + 1 });
      return;
    }
    db.insert(app.cart_items, {
      owner_id: ownerId,
      product: product.id,
      quantity: 1,
    });
  }

  function updateQty(itemId: string, nextQty: number) {
    if (nextQty <= 0) {
      db.delete(app.cart_items, itemId);
      return;
    }
    db.update(app.cart_items, itemId, { quantity: nextQty });
  }

  return (
    <div className="mx-auto grid w-full max-w-7xl gap-6 px-4 py-6 md:grid-cols-[1fr_320px] md:px-6">
      <section className="space-y-4">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {products?.map((product) => (
            <article
              key={product.id}
              className="overflow-hidden rounded-2xl border border-amber-700/30 bg-neutral-900/90 shadow-lg shadow-black/20"
            >
              <img
                src={product.image_url}
                alt={product.name}
                className="h-40 w-full object-cover"
              />
              <div className="space-y-3 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-xs uppercase tracking-wide text-amber-300/70">
                      {product.brand}
                    </p>
                    <h2 className="text-base font-semibold text-amber-100">{product.name}</h2>
                    <p className="mt-1 text-xs text-amber-100/70">{product.description}</p>
                  </div>
                  <p className="text-lg font-bold text-amber-50">
                    {formatCents(product.price_cents)}
                  </p>
                </div>
                <div className="flex items-center justify-between text-xs text-amber-200/80">
                  <span className="inline-flex items-center gap-1">
                    <Star className="h-3 w-3 fill-amber-300 text-amber-300" />
                    {product.rating.toFixed(1)}
                  </span>
                  <span>{product.in_stock} in stock</span>
                </div>
                <button
                  className="w-full rounded-lg bg-amber-500 px-3 py-2 text-sm font-semibold text-neutral-950 transition hover:bg-amber-400"
                  onClick={() => addToCart(product)}
                  disabled={!ownerId}
                >
                  Add to cart
                </button>
              </div>
            </article>
          ))}
        </div>
        {products?.length === 0 ? (
          <p className="rounded-xl border border-amber-700/40 bg-neutral-900/60 px-4 py-3 text-sm text-amber-100/80">
            No products yet. Run <code>pnpm seed</code> in <code>examples/jamazon</code>.
          </p>
        ) : null}
      </section>

      <aside className="space-y-4">
        <section className="rounded-2xl border border-amber-700/40 bg-neutral-950/95 p-4">
          <h3 className="mb-3 flex items-center gap-2 text-lg font-semibold text-amber-100">
            <ShoppingBag className="h-5 w-5" />
          </h3>
          <div className="space-y-3">
            {cartItems?.length === 0 ? (
              <p className="text-sm text-amber-100/60">Your cart is empty.</p>
            ) : (
              cartItems?.map((item) => (
                <div
                  key={item.id}
                  className="rounded-xl border border-amber-700/30 bg-neutral-900 p-3"
                >
                  <p className="text-sm font-medium text-amber-100">
                    {item.product?.name ?? "Unknown item"}
                  </p>
                  <p className="text-xs text-amber-100/60">
                    {formatCents(item.product?.price_cents ?? 0)} each
                  </p>
                  <div className="mt-2 flex items-center justify-between">
                    <div className="flex items-center gap-1">
                      <button
                        className="rounded-md border border-amber-700/40 p-1 text-amber-200 hover:bg-amber-700/20"
                        onClick={() => updateQty(item.id, item.quantity - 1)}
                        aria-label="Decrease quantity"
                      >
                        <Minus className="h-3 w-3" />
                      </button>
                      <span className="min-w-6 text-center text-sm text-amber-100">
                        {item.quantity}
                      </span>
                      <button
                        className="rounded-md border border-amber-700/40 p-1 text-amber-200 hover:bg-amber-700/20"
                        onClick={() => updateQty(item.id, item.quantity + 1)}
                        aria-label="Increase quantity"
                      >
                        <Plus className="h-3 w-3" />
                      </button>
                    </div>
                    <button
                      className="rounded-md border border-red-400/40 p-1 text-red-300 hover:bg-red-500/10"
                      onClick={() => db.delete(app.cart_items, item.id)}
                      aria-label="Remove item"
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
          <div className="mt-4 border-t border-amber-700/30 pt-3">
            <button
              className="mt-3 w-full rounded-lg bg-emerald-500 px-3 py-2 text-sm font-semibold text-emerald-950 transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:bg-emerald-900/50 disabled:text-emerald-100/60"
              onClick={() => navigate({ to: "/checkout" })}
            >
              Checkout
            </button>
          </div>
        </section>
      </aside>
    </div>
  );
}
