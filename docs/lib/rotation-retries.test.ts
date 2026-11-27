import { readFile } from "node:fs/promises";
import { expect, test } from "vitest";

// The guide states the rotation retry schedule that jazz-e2ee-client's
// `rotation_retries_wait_2_to_32_seconds_then_give_up` pins in code. The pin
// on the guide lives here, not in the crate, so the crate's tests build from
// its published package, which has no `docs/`.
const guide = new URL("../content/docs/e2ee/sharing-and-keys.mdx", import.meta.url);
const delivery = new URL("../../crates/jazz-e2ee-client/src/spaces/delivery.rs", import.meta.url);

const collapse = (text: string) => text.split(/\s+/).filter(Boolean).join(" ");

test("the guide's rotation retry schedule matches the client's", async () => {
  const source = collapse(await readFile(delivery, "utf8"));
  // The numbers the crate's own test pins: waits after failures 1 to 5, and
  // the failure that gives up.
  expect(source).toContain("pub(crate) const ROTATION_ATTEMPTS: u32 = 6;");
  expect(source).toContain("assert_eq!(waits, [2, 4, 8, 16, 32]);");
  expect(source).toContain("assert_eq!(ROTATION_ATTEMPTS, 6);");

  const text = collapse(await readFile(guide, "utf8"));
  expect(text).toContain(
    "it tries again after 2 seconds, then 4, 8, 16 and 32, and gives up after 6 failed attempts in a row",
  );
  expect(text).toContain(
    "Failures while offline aren't counted, and wait 2 seconds like a first failure.",
  );
});
