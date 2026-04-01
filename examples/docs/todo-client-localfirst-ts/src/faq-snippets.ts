import { createJazzContext } from "jazz-tools";

declare const context: Awaited<ReturnType<typeof createJazzContext>>;
const db = context.db;

// #region faq-delete-client-storage-ts
await db.deleteClientStorage();
// #endregion faq-delete-client-storage-ts
