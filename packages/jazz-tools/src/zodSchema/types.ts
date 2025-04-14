import { AnyCoListSchema } from "./coList/schema.js";
import { AnyCoMapSchema } from "./coMap/schema.js";

export type CoValueSchema = AnyCoMapSchema | AnyCoListSchema;
