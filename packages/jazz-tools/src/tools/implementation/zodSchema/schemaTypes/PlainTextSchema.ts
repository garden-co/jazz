import { RawCoPlainText } from "cojson";
import {
  Account,
  BranchDefinition,
  CoPlainText,
  Group,
  Settled,
  coOptionalDefiner,
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
import { z } from "../zodReExport.js";
import { coValueValidationSchema } from "./schemaValidators.js";

export interface CorePlainTextSchema extends CoreCoValueSchema {
  builtin: "CoPlainText";
}

export function createCoreCoPlainTextSchema(): CorePlainTextSchema {
  return {
    collaborative: true as const,
    builtin: "CoPlainText" as const,
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

export class PlainTextSchema implements CorePlainTextSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoPlainText" as const;
  readonly resolveQuery = true as const;

  #permissions: SchemaPermissions | null = null;
  #validationSchema: z.ZodType | undefined = undefined;
  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    const validationSchema = z.string();

    this.#validationSchema = coValueValidationSchema(
      validationSchema,
      CoPlainText,
    );
    return this.#validationSchema;
  };

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  get permissions(): SchemaPermissions {
    return this.#permissions ?? DEFAULT_SCHEMA_PERMISSIONS;
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
  ): Promise<Settled<CoPlainText>> {
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

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: Omit<SchemaPermissions, "writer">,
  ): PlainTextSchema {
    const copy = new PlainTextSchema(this.coValueClass);
    copy.#permissions = permissions;
    return copy;
  }
}
