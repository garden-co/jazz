import type { App, RowOf } from "../typed-app.js";
import type { deviceRequestSchema } from "./device-requests.js";

/** Encrypted apps include these records; applications define their administration policies. */
export { groupSchema } from "./managed-schema.js";
import { groupSchema } from "./managed-schema.js";

export type GroupTables = Pick<
  App<typeof deviceRequestSchema & typeof groupSchema>,
  keyof typeof groupSchema
>;
export type GroupRoot = RowOf<GroupTables["__e2ee_groups"]>;
export type GroupDelivery = RowOf<GroupTables["__e2ee_group_deliveries"]>;
export type GroupRecoveryDelivery = RowOf<GroupTables["__e2ee_group_recovery_deliveries"]>;
export type GroupMembership = RowOf<GroupTables["__e2ee_group_membership"]>;
export type GroupRepair = RowOf<GroupTables["__e2ee_group_repairs"]>;
export type GroupSuccessor = RowOf<GroupTables["__e2ee_group_successors"]>;
