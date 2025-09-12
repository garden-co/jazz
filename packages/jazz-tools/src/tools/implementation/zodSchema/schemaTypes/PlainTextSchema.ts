import { RawCoPlainText } from "cojson";
import {
  Account,
  BranchDefinition,
  CoPlainText,
  Group,
  coOptionalDefiner,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CorePlainTextSchema extends CoreCoValueSchema {
  builtin: "CoPlainText";
}

export function createCoreCoPlainTextSchema(): CorePlainTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoPlainText" as const,
  };
}

export class PlainTextSchema implements CorePlainTextSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoPlainText" as const;

  constructor(private coValueClass: typeof CoPlainText) {}

  create(text: string, options?: { owner: Group } | Group): CoPlainText;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoPlainText;
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoPlainText {
    return this.coValueClass.create(text, options);
  }

  load(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<CoPlainText | null> {
    return this.coValueClass.load(id, options);
  }

  subscribe(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(...args: [any, ...any[]]) {
    // @ts-expect-error
    return this.coValueClass.subscribe(...args);
  }

  fromRaw(raw: RawCoPlainText): CoPlainText {
    return this.coValueClass.fromRaw(raw);
  }

  getCoValueClass(): typeof CoPlainText {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }
}
