// A keyless backend's types: `keyless(app)` drops each column the generated
// types mark `encrypted: true` from rows (`?: undefined`), inserts and
// filters; the client's `app` keeps the plaintext types.
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  app as generated,
  type Task,
} from "../../../../crates/jazz-codegen/tests/apps/encrypted/jazz.generated.js";
import { keyless, type KeylessApp, type KeylessRow } from "../../src/backend/index.js";
import { testApp } from "../../src/testing/test-app.js";
import type { InsertOf, RowOf } from "../../src/typed-app.js";

const file = {
  tables: {
    projects: { columns: { name: "string" } },
    tasks: {
      columns: {
        projectId: "uuid",
        title: "bytes",
        __e2ee_eq_title: "bytes",
        notes: "bytes",
        done: "boolean",
      },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string", notes: "string?" },
        equality: ["title"],
      },
    },
    notes: { columns: { body: "string" } },
  },
} as const;

describe("keyless backend types", () => {
  it("drop a generated app's encrypted columns from rows, inserts and filters", () => {
    const backend = keyless(generated);
    expectTypeOf<RowOf<typeof backend.tasks>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      done: boolean;
      title?: undefined;
      notes?: undefined;
    }>();
    expectTypeOf<InsertOf<typeof backend.tasks>>().toEqualTypeOf<{
      projectId: string;
      done: boolean;
    }>();
    backend.tasks.where({ done: true });
    // @ts-expect-error the backend can't filter by an encrypted column
    backend.tasks.where({ title: "x" });
    // The client's app keeps the plaintext types.
    expectTypeOf<RowOf<typeof generated.tasks>>().toEqualTypeOf<{
      id: string;
      projectId: string;
      title: string;
      notes: string | null;
      done: boolean;
    }>();
    generated.tasks.where({ title: "x" });
  });

  it("leave tables without encrypted columns, and apps without encryption, as they are", () => {
    expectTypeOf<RowOf<KeylessApp<typeof generated>["__e2ee_members"]>>().toEqualTypeOf<
      RowOf<typeof generated.__e2ee_members>
    >();
    const plain = testApp({ tables: { notes: { columns: { body: "string" } } } } as const);
    expectTypeOf<KeylessApp<typeof plain>>().toEqualTypeOf<typeof plain>();
  });

  it("type an inline encrypted app as its bindings do", () => {
    const app = testApp(file);
    // The inferred types are the generated ones, so keyless maps them alike.
    expectTypeOf<RowOf<typeof app.tasks>>().toEqualTypeOf<RowOf<typeof generated.tasks>>();
    expectTypeOf<RowOf<KeylessApp<typeof app>["tasks"]>>().toEqualTypeOf<
      RowOf<KeylessApp<typeof generated>["tasks"]>
    >();
  });

  it("KeylessRow leaves a row with no encrypted column unchanged", () => {
    expectTypeOf<KeylessRow<Task, never>>().toEqualTypeOf<Task>();
    expectTypeOf<KeylessRow<Task, "title">>().toEqualTypeOf<{
      readonly id: string;
      projectId: string;
      notes: string | null;
      done: boolean;
      title?: undefined;
    }>();
  });

  it("is the app itself at runtime", () => {
    const app = testApp(file);
    expect(keyless(app)).toBe(app);
  });
});
