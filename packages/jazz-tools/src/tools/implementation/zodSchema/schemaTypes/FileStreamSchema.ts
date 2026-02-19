import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
  Settled,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { z } from "../zodReExport.js";

export interface CoreFileStreamSchema extends CoreCoValueSchema {
  builtin: "FileStream";
}

export function createCoreFileStreamSchema(): CoreFileStreamSchema {
  return {
    collaborative: true as const,
    builtin: "FileStream" as const,
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

export class FileStreamSchema implements CoreFileStreamSchema {
  readonly collaborative = true as const;
  readonly builtin = "FileStream" as const;
  readonly resolveQuery = true as const;

  #validationSchema: z.ZodType | undefined = undefined;
  #permissions: SchemaPermissions | null = null;
  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    this.#validationSchema = z.instanceof(FileStream);
    return this.#validationSchema;
  };

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  get permissions(): SchemaPermissions {
    return this.#permissions ?? DEFAULT_SCHEMA_PERMISSIONS;
  }

  constructor(private coValueClass: typeof FileStream) {}

  create(options?: { owner: Group } | Group): FileStream;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(options?: { owner: Account | Group } | Account | Group): FileStream;
  create(options?: { owner: Account | Group } | Account | Group): FileStream {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.create(optionsWithPermissions);
  }

  createFromBlob(
    blob: Blob | File,
    options?:
      | { owner?: Group; onProgress?: (progress: number) => void }
      | Group,
  ): Promise<FileStream>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  createFromBlob(
    blob: Blob | File,
    options?:
      | { owner?: Account | Group; onProgress?: (progress: number) => void }
      | Account
      | Group,
  ): Promise<FileStream>;
  createFromBlob(
    blob: Blob | File,
    options?:
      | {
          owner?: Account | Group;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ): Promise<FileStream> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.createFromBlob(blob, optionsWithPermissions);
  }

  createFromArrayBuffer(
    arrayBuffer: ArrayBuffer,
    mimeType: string,
    fileName: string | undefined,
    options?:
      | {
          owner?: Account | Group;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ) {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.createFromArrayBuffer(
      arrayBuffer,
      mimeType,
      fileName,
      optionsWithPermissions,
    );
  }

  loadAsBlob(
    id: string,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Blob | undefined> {
    return this.coValueClass.loadAsBlob(id, options);
  }

  load(
    id: string,
    options?: { loadAs?: Account | AnonymousJazzAgent },
  ): Promise<Settled<FileStream>> {
    return this.coValueClass.load(id, options);
  }

  unstable_merge(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<void> {
    // @ts-expect-error
    return unstable_mergeBranchWithResolve(this.coValueClass, id, options);
  }

  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: FileStream, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: FileStream, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(...args: [any, ...any[]]) {
    // @ts-expect-error
    return this.coValueClass.subscribe(...args);
  }

  getCoValueClass(): typeof FileStream {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: Omit<SchemaPermissions, "onInlineCreate" | "writer">,
  ): FileStreamSchema {
    const copy = new FileStreamSchema(this.coValueClass);
    copy.#permissions = permissions;
    return copy;
  }
}
