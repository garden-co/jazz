import { readFile } from "node:fs/promises";
import { expect, test } from "vitest";

// The guide states the rotation retry schedule of jazz-e2ee-client
// (`ROTATION_ATTEMPTS` and `retry_delay` in `src/spaces/delivery.rs`). The pin
// on the guide lives here, not in the crate, so the crate's tests build from
// its published package, which has no `docs/`. It reads the schedule from the
// code itself, so a change to either side fails here until the other follows.
const guide = new URL("../content/docs/e2ee/sharing-and-keys.mdx", import.meta.url);
const delivery = new URL("../../crates/jazz-e2ee-client/src/spaces/delivery.rs", import.meta.url);

const collapse = (text: string) => text.split(/\s+/).filter(Boolean).join(" ");

/** The schedule `delivery.rs` defines: the wait after `failures` failed attempts, and the give-up. */
function schedule(source: string) {
  const attempts = /pub\(crate\) const ROTATION_ATTEMPTS: u32 = (\d+);/.exec(source);
  const delay =
    /pub\(crate\) fn retry_delay\(failures: u32\) -> std::time::Duration \{ std::time::Duration::from_secs\((\d+)u64 << failures\.clamp\((\d+), ROTATION_ATTEMPTS - (\d+)\)\) \}/.exec(
      source,
    );
  if (!attempts || !delay) {
    throw new Error(
      "delivery.rs no longer has the shape this test reads: update the parser with the code",
    );
  }
  const giveUp = Number(attempts[1]);
  const [base, low, below] = delay.slice(1).map(Number);
  const high = giveUp - below;
  const wait = (failures: number) => base * 2 ** Math.min(Math.max(failures, low), high);
  return { giveUp, wait };
}

test("the guide's rotation retry schedule matches the client's", async () => {
  const { giveUp, wait } = schedule(collapse(await readFile(delivery, "utf8")));
  // Waits after failures 1 to giveUp - 1; the giveUp-th failure gives up.
  const waits = Array.from({ length: giveUp - 1 }, (_, i) => wait(i + 1));
  // Offline attempts aren't counted (failures stays 0), and wait as long as
  // a first failure.
  expect(wait(0)).toBe(wait(1));

  const text = collapse(await readFile(guide, "utf8"));
  const [first, ...rest] = waits;
  const last = rest.pop();
  expect(text).toContain(
    `it tries again after ${first} seconds, then ${rest.join(", ")} and ${last}, and gives up after ${giveUp} failed attempts in a row`,
  );
  expect(text).toContain(
    `Failures while offline aren't counted, and wait ${wait(0)} seconds like a first failure.`,
  );
});
