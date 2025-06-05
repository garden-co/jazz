import { RawCoPlainText } from "cojson";
import { Account, CoPlainText, Group } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";

export type PlainTextSchema = z.core.$ZodCustom<CoPlainText, unknown> & {
  collaborative: true;
  builtin: "CoPlainText";
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
  getCoSchema: () => typeof CoPlainText;
};
