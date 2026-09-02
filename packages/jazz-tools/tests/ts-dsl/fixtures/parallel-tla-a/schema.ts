import { col, table } from "jazz-tools";

const { promise, resolve } = Promise.withResolvers<void>();
setTimeout(resolve, 35);
await promise;
table("parallel_a", {
  value: col.string(),
});
