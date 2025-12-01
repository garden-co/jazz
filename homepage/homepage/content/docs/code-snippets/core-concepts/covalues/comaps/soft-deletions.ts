// [!code hide]
import { co, z } from "jazz-tools";
const Project = co.map({
  name: z.string(),
  // [!code ++]
  deleted: z.optional(z.boolean()),
});
