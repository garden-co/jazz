export const REALISTIC_BROWSER_BENCH_TEST = "tests/browser/realistic-bench.test.ts";

function normalizeArg(value: string): string {
  return value.replaceAll("\\", "/");
}

export function shouldExcludeRealisticBrowserBench(input?: {
  argv?: string[];
  lifecycleEvent?: string;
}): boolean {
  const argv = input?.argv ?? process.argv;
  const lifecycleEvent = input?.lifecycleEvent ?? process.env.npm_lifecycle_event ?? "";

  if (lifecycleEvent === "bench:realistic:browser") {
    return false;
  }

  return !argv.some((value) => normalizeArg(value).endsWith(REALISTIC_BROWSER_BENCH_TEST));
}
