import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { applyDeviceRequestPermissions } from "./device-policy.js";

/**
 * Managed enrolment records: inserting a request alone never grants keys.
 * Ownership comes from verified $createdBy.account, not an account ID in the payload.
 * The request ID identifies an immutable proposal; a replacement needs a new ID.
 * Compose these records explicitly with the application schema and permissions.
 */
export { deviceRequestSchema } from "./managed-schema.js";
import { deviceRequestSchema } from "./managed-schema.js";

export const deviceRequestApp = s.defineApp(deviceRequestSchema);
export type DeviceTables = Pick<typeof deviceRequestApp, keyof typeof deviceRequestSchema>;

export const deviceRequestPermissions = definePermissions(
  deviceRequestApp,
  applyDeviceRequestPermissions,
);
