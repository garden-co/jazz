import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface CoreFileStreamSchema extends CoreCoValueSchema {
  builtin: "FileStream";
}

export interface FileStreamSchema extends CoreFileStreamSchema {
  create(options?: { owner?: Account | Group } | Account | Group): FileStream;
  createFromBlob(
    blob: Blob | File,
    options?:
      | {
          owner?: Group | Account;
          onProgress?: (progress: number) => void;
        }
      | Account
      | Group,
  ): Promise<FileStream>;
  loadAsBlob(
    id: string,
    options?: {
      allowUnfinished?: boolean;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Blob | undefined>;
  load(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<FileStream>;
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: FileStream, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: FileStream, unsubscribe: () => void) => void,
  ): () => void;
  getCoValueClass: () => typeof FileStream;
}

export function createCoreFileStreamSchema(): CoreFileStreamSchema {
  const zodSchema = z.instanceof(FileStream).meta({
    collaborative: true,
  });
  return Object.assign(zodSchema, {
    collaborative: true as const,
    builtin: "FileStream" as const,
  });
}

export function enrichFileStreamSchema(
  schema: CoreFileStreamSchema,
  coValueClass: typeof FileStream,
): FileStreamSchema {
  return Object.assign(schema, {
    create: (...args: any[]) => {
      return coValueClass.create(...args);
    },
    createFromBlob: (...args: [any, ...any[]]) => {
      return coValueClass.createFromBlob(...args);
    },
    load: (...args: [any, ...any[]]) => {
      return coValueClass.load(...args);
    },
    loadAsBlob: (...args: [any, ...any[]]) => {
      return coValueClass.loadAsBlob(...args);
    },
    subscribe: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as FileStreamSchema;
}
