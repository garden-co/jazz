import { expect, it } from "vitest";
import { testApp, testPermissions } from "../../testing/test-app.js";
import { allowAll } from "./allow-all.js";

it("allows what a permissions file allowing every operation allows", () => {
  const app = testApp({ tables: { notes: { columns: { text: "string" } } } });
  expect(allowAll(app)).toEqual(
    testPermissions(app, {
      tables: { notes: { read: "true", insert: "true", update: "true", delete: "true" } },
    }),
  );
});
