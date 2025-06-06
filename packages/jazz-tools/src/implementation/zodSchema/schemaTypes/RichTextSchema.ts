import { Account, CoRichText, Group } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { z } from "../zodReExport.js";

export type RichTextSchema = z.core.$ZodCustom<CoRichText, unknown> & {
  collaborative: true;
  builtin: "CoRichText";
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
  getCoSchema: () => typeof CoRichText;
};
