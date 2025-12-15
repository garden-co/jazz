import {
  Account,
  AnonymousJazzAgent,
  BranchDefinition,
  FileStream,
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
import { RawBinaryCoStream } from "cojson";
import { cojsonInternals } from "cojson";
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
    const { owner } = parseCoValueCreateOptions(optionsWithPermissions);
    return new this.coValueClass({ owner }, this);
  }

  fromRaw(raw: RawBinaryCoStream): FileStream {
    return new this.coValueClass({ fromRaw: raw }, this);
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
  async createFromBlob(
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
    const arrayBuffer = await blob.arrayBuffer();
    return this.createFromArrayBuffer(
      arrayBuffer,
      blob.type,
      blob instanceof File ? blob.name : undefined,
      optionsWithPermissions,
    );
  }

  async createFromArrayBuffer(
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
  ): Promise<FileStream> {
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    const { owner } = parseCoValueCreateOptions(optionsWithPermissions);
    const stream = this.create({ owner });
    const onProgress =
      optionsWithPermissions && "onProgress" in optionsWithPermissions
        ? optionsWithPermissions.onProgress
        : undefined;

    const start = Date.now();

    const data = new Uint8Array(arrayBuffer);
    stream.start({
      mimeType,
      totalSizeBytes: arrayBuffer.byteLength,
      fileName,
    });
    const chunkSize =
      cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

    let lastProgressUpdate = Date.now();

    for (let idx = 0; idx < data.length; idx += chunkSize) {
      stream.push(data.slice(idx, idx + chunkSize));

      if (Date.now() - lastProgressUpdate > 100) {
        onProgress?.(idx / data.length);
        lastProgressUpdate = Date.now();
      }

      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    stream.end();
    const end = Date.now();

    console.debug(
      "Finished creating binary stream in",
      (end - start) / 1000,
      "s - Throughput in MB/s",
      (1000 * (arrayBuffer.byteLength / (end - start))) / (1024 * 1024),
    );
    onProgress?.(1);

    return stream;
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
    const stream = (await loadCoValueWithoutMe(
      this,
      id,
      options,
    )) as FileStream;

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
          this,
          id,
          options || {},
          (value, unsubscribe) => {
            const streamValue = value as FileStream;
            if (streamValue.isBinaryStreamEnded()) {
              unsubscribe();
              resolve(streamValue);
            }
          },
        );
      }) as Promise<Settled<FileStream>>;
    }

    return stream as Settled<FileStream>;
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
    const { options, listener } = parseSubscribeRestArgs<
      FileStream,
      RefsToResolve<FileStream>
    >(restArgs);
    return subscribeToCoValueWithoutMe(this, id, options, listener as any);
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
