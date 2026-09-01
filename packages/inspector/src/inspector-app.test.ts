import { describe, expect, it } from "vitest";
import type { WasmSchema } from "jazz-tools";
import { defaultRuntimeContextKey } from "./contexts/default-runtime-context";
import type { InspectorRuntimeContext } from "./contexts/host-link";

const appId = "shared-app";
const logicalBase = "shared-db";
const externalPhysicalDbName =
  `${logicalBase}::jazz-browser-v1::` +
  "%7B%22version%22%3A1%2C%22appId%22%3A%22shared-app%22%2C%22auth%22%3A%7B%22kind%22%3A%22principal%22%2C%22authMode%22%3A%22external%22%7D%7D";
const localFirstPhysicalDbName =
  `${logicalBase}::jazz-browser-v1::` +
  "%7B%22version%22%3A1%2C%22appId%22%3A%22shared-app%22%2C%22auth%22%3A%7B%22kind%22%3A%22principal%22%2C%22authMode%22%3A%22local-first%22%7D%7D";

const context = (key: string, dbName: string): InspectorRuntimeContext => ({
  key,
  appId,
  dbName,
  schema: {} as WasmSchema,
});

describe("defaultRuntimeContextKey", () => {
  it("chooses the exact host auth scope when reversed contexts share its app and logical base", () => {
    const contexts = [
      context("external-first", externalPhysicalDbName),
      context("host-local-first", localFirstPhysicalDbName),
    ];

    expect(
      defaultRuntimeContextKey(contexts, {
        appId,
        runtimeSources: { inspectorHostPhysicalDbName: localFirstPhysicalDbName },
      }),
    ).toBe("host-local-first");
  });

  it("falls back only when the host context is absent", () => {
    const contexts = [context("external-first", externalPhysicalDbName)];

    expect(
      defaultRuntimeContextKey(contexts, {
        appId,
        runtimeSources: { inspectorHostPhysicalDbName: localFirstPhysicalDbName },
      }),
    ).toBe("external-first");
  });
});
