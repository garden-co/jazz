import type { KnownStateMessage, NewContentMessage } from "./sync.js";

export interface StorageAPI {
  setPushCallback(
    callback: (data: NewContentMessage | KnownStateMessage) => void,
  ): void;

  load(id: string): Promise<boolean>;
  store(data: NewContentMessage): Promise<KnownStateMessage>;

  getKnownState(id: string): Promise<KnownStateMessage>;
}
