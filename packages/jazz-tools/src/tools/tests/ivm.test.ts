import { expect, test } from "vitest";
import {
  D2,
  map,
  filter,
  debug,
  MultiSet,
  orderBy,
  output,
  orderByWithFractionalIndex,
} from "@tanstack/db-ivm";

test("ivm", () => {
  const graph = new D2();

  // Input is [rawIndex, value]
  const input = graph.newInput<[number, number]>();

  input.pipe(
    filter((keyValue) => keyValue[1] % 2 === 0),
    orderByWithFractionalIndex((value) => value, { limit: 10 }),
    output((keyValue) => {
      // console.log(keyValue.getInner());
    }),
  );

  graph.finalize();

  const multiSetArray = Array.from({ length: 1000000 }, (_, idx) => [
    [idx, getRandomInt(0, 10000)],
    1,
  ]) as [[number, number], number][];
  console.time("Time to hydrate");
  input.sendData(new MultiSet(multiSetArray));

  graph.run();
  console.timeEnd("Time to hydrate");

  console.time("Time to process update");
  // The new element should go first
  input.sendData([[[10001, -2], 1]]);
  graph.run();
  console.timeEnd("Time to process update");
});

// Benchmark: filtering even values and ordering by value
// Time to hydrate
// 10k items: 28ms
// 100k items: 175ms
// 1M items: 1.9s *
// Time to process update
// 10k items: 7ms
// 100k items: 65ms
// 1M items: 900ms **
// Result times are reduced if `limit: 10` is applied to the orderBy operator:
// * 1.9s -> 840ms
// ** 900ms -> 520ms

// General impression: time to process updates is way slower than I expected after looking at
// Electric's benchmarks. TanStack DB makes several optimizations on top of db-ivm.
// TODO: try out orderByWithFractionalIndex. It trades off a (slower) hydration time for a (faster)
// update time. It appears to be REALLY slow for hydrating large datasets (>1' for 1M items!), but
// then updates are blazing fast (sub-ms times).
// Note: in TanStack DB, an orderBy clause is required to use limit/offset!!!
// https://github.com/TanStack/db/blob/5e2932fca4149083b850009b8a6a874db177f147/packages/db/src/query/builder/index.ts#L552

function getRandomInt(min: number, max: number): number {
  const minCeiled = Math.ceil(min);
  const maxFloored = Math.floor(max);
  return Math.floor(Math.random() * (maxFloored - minCeiled + 1) + minCeiled);
}
