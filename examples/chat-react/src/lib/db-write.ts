import type { DurabilityTier } from "jazz-tools";

type WriteHandle<T = unknown> = {
  wait(options: { tier: DurabilityTier }): Promise<T>;
};

type WriteResult<T> = WriteHandle<T> & {
  value: T;
};

type MaybePromise<T> = T | Promise<T>;

export async function writeValue<T>(write: MaybePromise<WriteResult<T>>): Promise<T> {
  return (await write).value;
}

export async function waitForWrite<T>(
  write: MaybePromise<WriteHandle<T>>,
  options: { tier: DurabilityTier },
): Promise<T> {
  return (await write).wait(options);
}

export function fireAndReport(write: MaybePromise<WriteHandle>, label: string): void {
  void Promise.resolve(write).catch((error) => {
    console.error(label, error);
  });
}
