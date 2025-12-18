import { RawCoPlainText } from "cojson";
import {
  Account,
  BranchDefinition,
  CoPlainText,
  CoreAccountSchema,
  CoreGroupSchema,
  Group,
  Loaded,
  ResolveQuery,
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
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";

export interface CorePlainTextSchema extends CoreCoValueSchema {
  builtin: "CoPlainText";
}

export function createCoreCoPlainTextSchema(): CorePlainTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoPlainText" as const,
    resolveQuery: true as const,
  };
}

export class PlainTextSchema implements CorePlainTextSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoPlainText" as const;
  readonly resolveQuery = true as const;

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

  constructor(private coValueClass: typeof CoPlainText) {}

  create(text: string, options?: { owner: Group } | Group): CoPlainText;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    text: string,
    options?:
      | { owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema> }
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>,
  ): CoPlainText;
  create(
    text: string,
    options?:
      | { owner: Loaded<CoreAccountSchema, true> | Loaded<CoreGroupSchema> }
      | Loaded<CoreAccountSchema, true>
      | Loaded<CoreGroupSchema>,
  ): CoPlainText {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    const { owner } = parseCoValueCreateOptions(optionsWithPermissions);
    return new this.coValueClass({ text, owner }, this);
  }

  load(
    id: string,
    options: {
      loadAs: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CorePlainTextSchema>> {
    return loadCoValueWithoutMe(this, id, options) as Promise<
      Settled<CorePlainTextSchema>
    >;
  }

  subscribe(
    id: string,
    options: {
      loadAs: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(...args: [any, ...[any]]) {
    const [id, ...restArgs] = args;
    const { options, listener } = parseSubscribeRestArgs<
      CorePlainTextSchema,
      ResolveQuery<CorePlainTextSchema>
    >(restArgs);
    return subscribeToCoValueWithoutMe(this, id, options, listener);
  }

  unstable_merge(
    id: string,
    options: {
      loadAs: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
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

  fromRaw(raw: RawCoPlainText): CoPlainText {
    return new this.coValueClass({ fromRaw: raw }, this);
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(permissions: SchemaPermissions): PlainTextSchema {
    const copy = new PlainTextSchema(this.coValueClass);
    copy.permissions = permissions;
    return copy;
  }
}
