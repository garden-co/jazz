import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
} from "../../../internal.js";
import { z } from "../zodReExport.js";

export type AnyFileStreamSchema = z.core.$ZodCustom<FileStream, unknown> & {
  collaborative: true;
  builtin: "FileStream";
};

export type FileStreamSchema = AnyFileStreamSchema & {
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
};

export function enrichFileStreamSchema(
  schema: AnyFileStreamSchema,
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
