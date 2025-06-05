import {
  Account,
  AnonymousJazzAgent,
  FileStream,
  Group,
} from "../../../internal.js";
import { z } from "../zodReExport.js";

export type FileStreamSchema = z.core.$ZodCustom<FileStream, unknown> & {
  collaborative: true;
  builtin: "FileStream";
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
  getCoSchema: () => typeof FileStream;
};
