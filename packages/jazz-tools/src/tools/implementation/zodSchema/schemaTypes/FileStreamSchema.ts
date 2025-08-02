import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
  coOptionalDefiner,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CoreFileStreamSchema extends CoreCoValueSchema {
  builtin: "FileStream";
}

export function createCoreFileStreamSchema(): CoreFileStreamSchema {
  return {
    collaborative: true as const,
    builtin: "FileStream" as const,
  };
}

export class FileStreamSchema implements CoreFileStreamSchema {
  readonly collaborative = true as const;
  readonly builtin = "FileStream" as const;

  constructor(private coValueClass: typeof FileStream) {}

  create(options?: { owner?: Account | Group } | Account | Group): FileStream {
    return this.coValueClass.create(options);
  }

  createFromBlob(
    blob: Blob | File,
    options?:
      | {
          owner?: Group | Account;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ): Promise<FileStream> {
    return this.coValueClass.createFromBlob(blob, options);
  }

  createFromArrayBuffer(
    ...args: Parameters<typeof FileStream.createFromArrayBuffer>
  ) {
    return this.coValueClass.createFromArrayBuffer(...args);
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
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<FileStream | null> {
    return this.coValueClass.load(id, options);
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
}
