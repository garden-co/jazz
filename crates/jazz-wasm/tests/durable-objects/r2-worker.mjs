// The packaged Jazz core Durable Object (as `host-worker.mjs`), for
// `r2.test.mjs`. With `FAIL_PUTS` set, the first that many large-value
// puts to `JAZZ_BLOBS` fail, as a bucket that is briefly unreachable does;
// each failure leaves a `test-failed-puts/<n>` marker at the bucket's root.
import { createJazzCore, r2, routeJazzRequest } from "./do/index.js";

function flaky(bucket, failures) {
  let left = Number(failures);
  return new Proxy(bucket, {
    get(target, name) {
      if (name === "put") {
        return async (key, value, options) => {
          if (left > 0 && !key.endsWith("/.jazz-owner")) {
            left -= 1;
            await target.put(`test-failed-puts/${left}`, "failed");
            throw new Error("injected put failure");
          }
          return target.put(key, value, options);
        };
      }
      const value = target[name];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export const JazzCore = createJazzCore({
  blobs: (env) =>
    env.BLOB_MODE === "sqlite"
      ? "sqlite"
      : r2(env.FAIL_PUTS ? flaky(env.JAZZ_BLOBS, env.FAIL_PUTS) : env.JAZZ_BLOBS),
});

export default { fetch: routeJazzRequest };
