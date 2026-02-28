export { attachDevTools, type DevToolsAttachment } from "./dev-tools.js";
export {
  createDbFromInspectedPage,
  getRegisteredDbConfig,
  getRegisteredWasmSchema,
  waitForDevToolsBootstrap,
  onDevToolsPortConnect,
  onDevToolsPortDisconnect,
} from "./extension-panel.js";
