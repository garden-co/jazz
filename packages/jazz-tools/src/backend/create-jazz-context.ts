// Node host for the shared backend context. Workers import backend-context directly.
import {
  NapiDb,
  captureTextAnchor,
  captureTextRange,
  resolveTextAnchor,
  resolveTextRange,
} from "jazz-napi";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { authorBytesForSession } from "../runtime/author-id.js";
import { textAnchorsFrom } from "../runtime/text-anchors.js";
import {
  JazzContext as SharedJazzContext,
  type BackendContextConfig,
  type BackendNodeClock,
  type NativeBackendHost,
} from "./backend-context.js";
export type {
  BackendContextConfig,
  BackendNodeClock,
  BackendDriver,
  BackendSchemaSource,
  BackendQuerySchemaSource,
  BackendSchemaInput,
} from "./backend-context.js";
export { MAX_PENDING_MUTATION_ERROR_EVENTS } from "./backend-context.js";

const nativeHost: NativeBackendHost = {
  textAnchors: () =>
    textAnchorsFrom({ captureTextAnchor, captureTextRange, resolveTextAnchor, resolveTextRange }),
  open: ({ config, schema, migrations, node, systemNode }) =>
    new NativeRuntimeAdapter(
      NapiDb,
      schema,
      node,
      authorBytesForSession({ issuer: "https://jazz.invalid", user_id: "backend-open" }),
      1,
      true,
      {
        ...(config.driver.type === "persistent" ? { persistentPath: config.driver.dataPath } : {}),
        readAuthorizationHost: "trusted-serving",
        backendMode: true,
        systemNode,
        webSocket: config.webSocket,
        slowQueryLog: { appId: config.appId, thresholdMs: config.slowQueryThresholdMs },
        migrations,
      },
    ),
};
export class JazzContext extends SharedJazzContext {
  constructor(
    config: BackendContextConfig,
    nodeIdentity?: Uint8Array,
    nodeClock?: BackendNodeClock,
  ) {
    super(config, nodeIdentity, nodeClock, nativeHost);
  }
}
export function createJazzContext(config: BackendContextConfig): JazzContext {
  return new JazzContext(config);
}
