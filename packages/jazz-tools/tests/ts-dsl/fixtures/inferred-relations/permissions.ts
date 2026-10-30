import { schema as s } from "jazz-tools";
import { app } from "./app.js";

export default s.definePermissions(app, ({ policy }) => [
  policy.records.allowRead.where(policy.exists(policy.records.hopTo("people"))),
  policy.people.allowRead.where(policy.exists(policy.people.hopTo("recordsViaPeople"))),
]);
