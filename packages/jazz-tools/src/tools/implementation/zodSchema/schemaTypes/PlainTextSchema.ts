import { RawCoPlainText } from "cojson";
import { Account, CoPlainText, Group } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";

export type AnyPlainTextSchema = z.core.$ZodCustom<CoPlainText, unknown> & {
  collaborative: true;
  builtin: "CoPlainText";
};

export type PlainTextSchema = AnyPlainTextSchema & {
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
};

export function enrichPlainTextSchema(
  schema: AnyPlainTextSchema,
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
