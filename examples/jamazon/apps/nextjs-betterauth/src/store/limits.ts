/**
 * How many of one product a cart line may hold. permissions.ts enforces it
 * on the server; the cart UI caps its inputs at it. It lives here, not in
 * permissions.ts, so client code never imports the permissions module.
 */
export const MAX_LINE_QUANTITY = 20;
