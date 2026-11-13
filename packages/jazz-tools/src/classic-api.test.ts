import { expect, it } from "vitest";
import {
  assertNoClassicProviderProps,
  co,
  CoFeed,
  JazzReactProvider,
  useAccount,
  useCoState,
} from "./classic-api.js";

function thrown(run: () => unknown): Error & { code?: string } {
  try {
    run();
  } catch (error) {
    return error as Error & { code?: string };
  }
  throw new Error("did not throw");
}

it("names the Jazz 2 replacement for a Classic name that has one", () => {
  const coState = thrown(() => (useCoState as unknown as () => unknown)());
  expect(coState.code).toBe("JAZZ_CLASSIC_API_REMOVED");
  expect(coState.message).toContain(
    "In Jazz 2, start from `useAll(query)` or `useOne(query)` (Svelte: `QuerySubscription`).",
  );
  expect(thrown(() => (useAccount as unknown as () => unknown)()).message).toContain(
    "In Jazz 2, start from the session: `useJazzSession()`",
  );
  expect(thrown(() => (JazzReactProvider as unknown as () => unknown)()).message).toContain(
    'In Jazz 2, start from `<JazzProvider config={{ serverUrl: "/jazz" }}>`.',
  );
  // A namespace member is named by its namespace.
  const map = thrown(() => (co as unknown as { map: unknown }).map);
  expect(map.message).toMatch(/^\[JAZZ_CLASSIC_API_REMOVED\] co\.map belongs to Jazz Classic/);
  expect(map.message).toContain("In Jazz 2, start from tables:");
  // A Classic provider prop by the prop.
  expect(
    thrown(() => assertNoClassicProviderProps("JazzProvider", { sync: { peer: "wss://x" } }))
      .message,
  ).toContain('In Jazz 2, start from `config={{ serverUrl: "/jazz" }}`.');
});

it("says only that it's Classic where Jazz 2 has nothing in its place", () => {
  const feed = thrown(() => (CoFeed as unknown as () => unknown)());
  expect(feed.code).toBe("JAZZ_CLASSIC_API_REMOVED");
  expect(feed.message).not.toContain("In Jazz 2, start from");
  expect(feed.message).toContain("This is not a rename-only migration.");
  // An inherited property name isn't taken for a replacement.
  expect(thrown(() => (co as unknown as { constructor: unknown }).constructor).message).toContain(
    "In Jazz 2, start from tables:",
  );
});
