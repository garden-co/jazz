import {
  Account,
  BranchDefinition,
  CoRichText,
  Group,
  MaybeLoaded,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  ResolveQuery,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

export interface CoreRichTextSchema extends CoreCoValueSchema {
  builtin: "CoRichText";
}

export function createCoreCoRichTextSchema(): CoreRichTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoRichText" as const,
    defaultResolveQuery: false,
  };
}

export class RichTextSchema<EagerlyLoaded extends boolean = false>
  implements CoreRichTextSchema
{
  readonly collaborative = true as const;
  readonly builtin = "CoRichText" as const;

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  get defaultResolveQuery(): EagerlyLoaded {
    return this.isEagerlyLoaded;
  }

  constructor(private coValueClass: typeof CoRichText) {}

  create(text: string, options?: { owner: Group } | Group): CoRichText;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoRichText;
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoRichText {
    return this.coValueClass.create(text, options);
  }

  load(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<MaybeLoaded<CoRichText>> {
    return this.coValueClass.load(id, options);
  }

  subscribe(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: CoRichText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoRichText, unsubscribe: () => void) => void,
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

  getCoValueClass(): typeof CoRichText {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  resolved(): RichTextSchema<true> {
    if (this.isEagerlyLoaded) {
      return this as RichTextSchema<true>;
    }
    const copy = new RichTextSchema<true>(this.coValueClass);
    copy.isEagerlyLoaded = true;
    return copy;
  }
}
