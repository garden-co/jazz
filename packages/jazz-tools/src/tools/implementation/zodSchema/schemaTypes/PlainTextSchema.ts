import { RawCoPlainText } from "cojson";
import {
  Account,
  BranchDefinition,
  CoPlainText,
  Group,
  MaybeLoaded,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  ResolveQuery,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

export interface CorePlainTextSchema extends CoreCoValueSchema {
  builtin: "CoPlainText";
}

export function createCoreCoPlainTextSchema(): CorePlainTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoPlainText" as const,
    defaultResolveQuery: false,
  };
}

export class PlainTextSchema<EagerlyLoaded extends boolean = false>
  implements CorePlainTextSchema
{
  readonly collaborative = true as const;
  readonly builtin = "CoPlainText" as const;

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  get defaultResolveQuery(): EagerlyLoaded {
    return this.isEagerlyLoaded;
  }

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
  ): Promise<MaybeLoaded<CoPlainText>> {
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

  unstable_merge(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<void> {
    // @ts-expect-error
    return unstable_mergeBranchWithResolve(this.coValueClass, id, options);
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

  resolved(): PlainTextSchema<true> {
    if (this.isEagerlyLoaded) {
      return this as PlainTextSchema<true>;
    }
    const copy = new PlainTextSchema<true>(this.coValueClass);
    copy.isEagerlyLoaded = true;
    return copy;
  }
}
