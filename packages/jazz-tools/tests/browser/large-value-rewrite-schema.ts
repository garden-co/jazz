// #3816, shaped like MusicAgent: a conversation holds a large attachment and a
// reply that a backend rewrites word by word. The browser reads both only
// through a conversation it created, and lists attachments without their bytes.
// The app is apps/large-value-rewrite (its Node-side backend reads the same files).
export { app as largeValueRewriteApp } from "./apps/large-value-rewrite/jazz.generated.js";
export { default as largeValueRewritePermissions } from "./apps/large-value-rewrite/jazz.permissions.json";
