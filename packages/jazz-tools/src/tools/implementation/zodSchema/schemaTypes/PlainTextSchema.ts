import { RawCoPlainText } from "cojson";
import { Account, CoPlainText, Group } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CorePlainTextSchema extends CoreCoValueSchema {
  builtin: "CoPlainText";
}

export interface PlainTextSchema extends CorePlainTextSchema {
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoPlainText;
  load(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<CoPlainText>;
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  fromRaw(raw: RawCoPlainText): CoPlainText;
  getCoValueClass: () => typeof CoPlainText;
}

export function createCoreCoPlainTextSchema(): CorePlainTextSchema {
  const zodSchema = z.instanceof(CoPlainText).meta({
    collaborative: true,
  });
  return Object.assign(zodSchema, {
    collaborative: true as const,
    builtin: "CoPlainText" as const,
  });
}

export function enrichPlainTextSchema(
  schema: CorePlainTextSchema,
  coValueClass: typeof CoPlainText,
): PlainTextSchema {
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
    fromRaw: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.fromRaw(...args);
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as PlainTextSchema;
}
