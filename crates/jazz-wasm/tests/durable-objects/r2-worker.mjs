// The packaged Jazz core Durable Object (as `host-worker.mjs`), for
// `r2.test.mjs`. With `FAIL_PUTS` set, the first that many large-value
// puts to `JAZZ_BLOBS` fail, as a bucket that is briefly unreachable does;
// each failure leaves a `test-failed-puts/<n>` marker at the bucket's root.
import { JazzCore as Core, routeJazzRequest } from "./do/index.js";

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

export class JazzCore extends Core {
  constructor(ctx, env) {
    super(
      ctx,
      env.JAZZ_BLOBS !== undefined && env.FAIL_PUTS
        ? { ...env, JAZZ_BLOBS: flaky(env.JAZZ_BLOBS, env.FAIL_PUTS) }
        : env,
    );
  }
}

export default { fetch: routeJazzRequest };
