import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import type { Db } from "../runtime/db.js";

declare const db: Db;
declare const scopeId: string;
declare const identifier: string;
declare const accountId: string;

db.insert(app.__e2ee_members, { scopeId, identifier, accountId });
app.__e2ee_members.where({ scopeId, identifier, accountId });
// @ts-expect-error membership filters require string account IDs
app.__e2ee_members.where({ accountId: 123 });
// @ts-expect-error the internal spaces table is hidden from generated bindings
void app.__e2ee_spaces;
