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
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
} from "../../../internal.js";
import { RawCoPlainText } from "cojson";
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
    const { owner } = parseCoValueCreateOptions(optionsWithPermissions);
    return new this.coValueClass({ text, owner }, this as any);
  }

  fromRaw(raw: RawCoPlainText): CoRichText {
    return new this.coValueClass({ fromRaw: raw }, this as any);
  }

  load(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoRichText>> {
    return loadCoValueWithoutMe(this, id, options) as Promise<
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
    return subscribeToCoValueWithoutMe(this, id, options, listener as any);
  }

  unstable_merge(
    id: string,
    options: {
      loadAs: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<void> {
    if (!options.unstable_branch) {
      throw new Error("unstable_branch is required for unstable_merge");
    }
    return unstable_mergeBranchWithResolve(this, id, {
      ...options,
      branch: options.unstable_branch,
    });
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
