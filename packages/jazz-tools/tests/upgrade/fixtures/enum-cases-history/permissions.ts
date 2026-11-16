import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy }) => [policy.tickets.allowRead.always()]);
