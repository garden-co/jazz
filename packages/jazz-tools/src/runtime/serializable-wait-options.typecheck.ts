import type { SerializableWriteHandle, SerializableWriteResult } from "./client.js";

function acceptsCombinedOptions(
  handle: SerializableWriteHandle,
  result: SerializableWriteResult<number>,
) {
  const h: Promise<void> = handle.wait({ tier: "global", timeoutMs: 100 });
  const r: Promise<number> = result.wait({ tier: "local", timeoutMs: 100 });
  result.wait({ timeoutMs: 100 });
  handle.wait();
  // @ts-expect-error The removed Edge tier is not a serializable durability tier.
  handle.wait({ tier: "edge", timeoutMs: 100 });
  // @ts-expect-error A timeout is a number of milliseconds.
  result.wait({ timeoutMs: "100" });
  return { h, r };
}
void acceptsCombinedOptions;
