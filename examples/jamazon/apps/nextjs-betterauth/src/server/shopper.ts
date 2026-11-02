import { backend } from "./backend";
import { CheckoutError } from "./orders";

/**
 * Resolve the signed-in shopper behind a store API request from its Better
 * Auth session cookie: the store's Jazz runs `authenticate` and names the
 * account that session's user is assigned to. Guests (local-first accounts)
 * browse and fill carts, but orders need a signed-in account.
 */
export async function requireShopper(request: Request): Promise<{ account: string }> {
  const account = await (await backend()).accountFor(request);
  if (!account) throw new CheckoutError("Sign in to check out.", 401);
  return { account };
}

/** Map domain errors to JSON responses without leaking internals. */
export async function jsonRoute(run: () => Promise<unknown>): Promise<Response> {
  try {
    return Response.json(await run());
  } catch (error) {
    if (error instanceof CheckoutError)
      return Response.json({ error: error.message }, { status: error.status });
    console.error("[jamazon]", error);
    return Response.json({ error: "Something went wrong. Try again." }, { status: 500 });
  }
}
