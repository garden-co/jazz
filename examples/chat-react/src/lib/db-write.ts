import type { DurabilityTier } from "jazz-tools";

type MutationResult<T> = {
  value: T;
  wait(options: { tier: DurabilityTier }): Promise<T>;
};

type MaybePromise<T> = T | Promise<T>;

export async function writeValue<T>(write: MaybePromise<MutationResult<T>>): Promise<T> {
  return (await write).value;
}

export async function waitForWrite<T>(
  write: MaybePromise<MutationResult<T>>,
  options: { tier: DurabilityTier },
): Promise<T> {
  return (await write).wait(options);
}

export function fireAndReport(write: MaybePromise<MutationResult<unknown>>, label: string): void {
  void Promise.resolve(write).catch((error) => {
    console.error(label, error);
  });
}
