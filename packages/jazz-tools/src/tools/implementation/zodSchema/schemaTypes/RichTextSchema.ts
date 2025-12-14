import {
  Account,
  BranchDefinition,
  CoRichText,
  Group,
  RefsToResolve,
  Settled,
  SubscribeRestArgs,
  coOptionalDefiner,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";

export interface CoreRichTextSchema extends CoreCoValueSchema {
  builtin: "CoRichText";
}

export function createCoreCoRichTextSchema(): CoreRichTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoRichText" as const,
    resolveQuery: true as const,
  };
}

export class RichTextSchema implements CoreRichTextSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoRichText" as const;
  readonly resolveQuery = true as const;

  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

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
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.create(text, optionsWithPermissions);
  }

  load(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoRichText>> {
    return loadCoValueWithoutMe(this.coValueClass, id, options) as Promise<
      Settled<CoRichText>
    >;
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
  subscribe(...args: [any, ...[any]]) {
    const [id, ...restArgs] = args;
    const { options, listener } = parseSubscribeRestArgs<
      CoRichText,
      RefsToResolve<CoRichText>
    >(restArgs);
    return subscribeToCoValueWithoutMe(
      this.coValueClass,
      id,
      options,
      listener as any,
    );
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

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(permissions: SchemaPermissions): RichTextSchema {
    const copy = new RichTextSchema(this.coValueClass);
    copy.permissions = permissions;
    return copy;
  }
}
