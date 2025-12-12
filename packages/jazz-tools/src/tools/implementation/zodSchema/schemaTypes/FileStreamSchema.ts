import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
  Settled,
  SubscribeRestArgs,
  coOptionalDefiner,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  unstable_mergeBranchWithResolve,
  withSchemaPermissions,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";

export interface CoreFileStreamSchema extends CoreCoValueSchema {
  builtin: "FileStream";
}

export function createCoreFileStreamSchema(): CoreFileStreamSchema {
  return {
    collaborative: true as const,
    builtin: "FileStream" as const,
    resolveQuery: true as const,
  };
}

export class FileStreamSchema implements CoreFileStreamSchema {
  readonly collaborative = true as const;
  readonly builtin = "FileStream" as const;
  readonly resolveQuery = true as const;

  /**
   * Permissions to be used when creating or composing CoValues
   */
  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

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

  async loadAsBlob(
    id: string,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Blob | undefined> {
    const stream = await this.load(id, options);

    if (!stream.$isLoaded) {
      return undefined;
    }

    return stream.toBlob({
      allowUnfinished: options?.allowUnfinished,
    });
  }

  async loadAsBase64(
    id: string,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
      dataURL?: boolean;
    },
  ): Promise<string | undefined> {
    const stream = await this.load(id, options);

    if (!stream.$isLoaded) {
      return undefined;
    }

    return stream.asBase64(options);
  }

  async load(
    id: string,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      allowUnfinished?: boolean;
    },
  ): Promise<Settled<FileStream>> {
    const stream = await loadCoValueWithoutMe(this.coValueClass, id, options);

    /**
     * If the user hasn't requested an incomplete blob and the
     * stream isn't complete wait for the stream download before progressing
     */
    if (
      !options?.allowUnfinished &&
      stream.$isLoaded &&
      !stream.isBinaryStreamEnded()
    ) {
      return new Promise<FileStream>((resolve) => {
        subscribeToCoValueWithoutMe(
          this.coValueClass,
          id,
          options || {},
          (value, unsubscribe) => {
            if (value.isBinaryStreamEnded()) {
              unsubscribe();
              resolve(value);
            }
          },
        );
      }) as Promise<Settled<FileStream>>;
    }

    return stream as Settled<FileStream>;
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
  subscribe(...args: [any, ...[any]]) {
    const [id, ...restArgs] = args;
    const { options, listener } = parseSubscribeRestArgs(restArgs);
    return subscribeToCoValueWithoutMe(
      this.coValueClass,
      id,
      options,
      listener as any,
    );
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
    permissions: Omit<SchemaPermissions, "onInlineCreate">,
  ): FileStreamSchema {
    const copy = new FileStreamSchema(this.coValueClass);
    copy.permissions = permissions;
    return copy;
  }
}
