import { createJazz, remote, durableObjects, type CreateJazzOptions } from "jazz-tools/backend";
declare const options: CreateJazzOptions;
const jazz = createJazz(options);
jazz.handle(new Request("https://app.example/"), { waitUntil(_work) {} });
remote({ url: "https://core.example", appId: "app", secret: "secret" });
durableObjects({
  name: "my-core",
  credentials: { accountId: "account", apiToken: "token" },
  secret: "secret",
});
// @ts-expect-error Node server attachment is not a Worker capability.
jazz.attach({});
// @ts-expect-error Worker handlers use Request/Response, not Node req/res.
jazz.nodeHandler({}, {});
