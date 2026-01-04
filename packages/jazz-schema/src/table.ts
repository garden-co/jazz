import type { ZodType, ZodOptional } from "zod";
import { TABLE_SYMBOL, type TableDescriptor } from "./types.js";

/**
 * Define a table schema.
 *
 * @example
 * ```ts
 * const Chat = table({
 *   title: z.string(),
 * });
 *
 * const Message = table({
 *   text: z.string(),
 *   chat: Chat,                      // Required reference
 *   replyTo: z.optional(Message),    // Optional self-reference
 * });
 * ```
 */
export function table<
  T extends Record<string, ZodType | TableDescriptor | ZodOptional<ZodType>>
>(columns: T): TableDescriptor & { columns: T } {
  return {
    [TABLE_SYMBOL]: true,
    name: "", // Will be set by generateSchema from the export name
    columns,
  };
}
