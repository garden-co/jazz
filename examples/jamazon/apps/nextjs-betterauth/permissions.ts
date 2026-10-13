import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

/** Largest quantity of one product a cart line may hold. */
export const MAX_LINE_QUANTITY = 20;

export default definePermissions(app, ({ policy, session, allOf, allowedTo }) => {
  // Better Auth rows are owned by the trusted auth route (backend authority).
  for (const table of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    table.allowRead.never();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }

  // The catalogue and stock levels are public. Only the backend writes them:
  // it seeds the catalogue and decrements stock inside checkout.
  for (const table of [policy.categories, policy.products, policy.stock]) {
    table.allowRead.always();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }

  // A cart belongs to exactly one account. Guests have a local-first account
  // too, so the same rule covers a guest cart and a signed-in cart; linking a
  // provider identity keeps the account, and with it the cart.
  const mine = { shopper: session.user.account };
  policy.carts.allowRead.where(mine);
  policy.carts.allowInsert.where(mine);
  policy.carts.allowUpdate.whereOld(mine).whereNew(mine);
  policy.carts.allowDelete.where(mine);

  // Cart lines inherit from their cart. The new row must still point at a
  // cart the shopper owns, so a line cannot be moved into someone else's cart.
  const sensibleQuantity = { quantity: { gte: 0, lte: MAX_LINE_QUANTITY } };
  policy.cartLines.allowRead.where(allowedTo.read("cart"));
  policy.cartLines.allowInsert.where(allOf([allowedTo.update("cart"), sensibleQuantity]));
  policy.cartLines.allowUpdate
    .whereOld(allowedTo.update("cart"))
    .whereNew(allOf([allowedTo.update("cart"), sensibleQuantity]));
  policy.cartLines.allowDelete.where(allowedTo.update("cart"));

  // Orders and everything hanging off them are readable by their shopper and
  // written only by the backend: placing an order, recording a payment and
  // shipping are backend decisions, never client writes.
  policy.orders.allowRead.where(mine);
  for (const table of [policy.orders, policy.orderLines, policy.orderEvents, policy.payments]) {
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }
  policy.orderLines.allowRead.where(allowedTo.read("order"));
  policy.orderEvents.allowRead.where(allowedTo.read("order"));
  policy.payments.allowRead.where(allowedTo.read("order"));
});
