import type { Page } from "playwright";

export function evaluateHarnessOperation<TResult = unknown>(
  page: Page,
  modulePath: string,
  moduleMethod: string,
  args: unknown,
  timeoutMs?: number,
): Promise<TResult>;
