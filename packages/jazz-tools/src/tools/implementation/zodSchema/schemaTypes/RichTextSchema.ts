import { Account, CoRichText, Group } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CoreRichTextSchema extends CoreCoValueSchema {
  builtin: "CoRichText";
}

export interface RichTextSchema extends CoreRichTextSchema {
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoRichText;
  load(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<CoRichText>;
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: CoRichText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoRichText, unsubscribe: () => void) => void,
  ): () => void;
  getCoValueClass: () => typeof CoRichText;
}

export function createCoreCoRichTextSchema(): CoreRichTextSchema {
  const zodSchema = z.instanceof(CoRichText).meta({
    collaborative: true,
  });
  return Object.assign(zodSchema, {
    collaborative: true as const,
    builtin: "CoRichText" as const,
  });
}

export function enrichRichTextSchema(
  schema: CoreRichTextSchema,
  coValueClass: typeof CoRichText,
): RichTextSchema {
  return Object.assign(schema, {
    create: (...args: [any, ...any[]]) => {
      return coValueClass.create(...args);
    },
    load: (...args: [any, ...any[]]) => {
      return coValueClass.load(...args);
    },
    subscribe: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as RichTextSchema;
}
