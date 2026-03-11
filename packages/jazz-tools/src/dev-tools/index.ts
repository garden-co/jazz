export { attachDevTools, type DevToolsAttachment } from "./dev-tools.js";
export {
  createDbFromInspectedPage,
  getActiveQuerySubscriptions,
  getRegisteredDbConfig,
  getRegisteredWasmSchema,
  waitForDevToolsBootstrap,
  onActiveQuerySubscriptionsChange,
  onDevToolsPortConnect,
  onDevToolsPortDisconnect,
} from "./extension-panel.js";
