import { col, table } from "jazz-tools";

const { promise, resolve } = Promise.withResolvers<void>();
setTimeout(resolve, 5);
await promise;
table("parallel_b", {
  value: col.int(),
});
