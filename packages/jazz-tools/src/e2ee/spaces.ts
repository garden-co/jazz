import type { App, RowOf } from "../typed-app.js";
import type { deviceRequestSchema } from "./device-requests.js";

/** Application policies own administration of these scoped key histories. */
export { spaceSchema } from "./managed-schema.js";
import { spaceSchema } from "./managed-schema.js";
export type SpaceTables = Pick<
  App<typeof deviceRequestSchema & typeof spaceSchema>,
  keyof typeof spaceSchema
>;
export type SpaceRoot = RowOf<SpaceTables["__e2ee_spaces"]>;
export type SpaceGrant = RowOf<SpaceTables["__e2ee_space_grants"]>;
export type SpaceDelivery = RowOf<SpaceTables["__e2ee_space_deliveries"]>;
export type SpaceSuccessor = RowOf<SpaceTables["__e2ee_space_successors"]>;
export type SpaceRecoveryDelivery = RowOf<SpaceTables["__e2ee_space_recovery_deliveries"]>;
