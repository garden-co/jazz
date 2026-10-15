import { GROOVE_DURABLE_INDEX_V2_CODEC, StorageCodecProfile } from "../_extern/types-protocol.js";

export const JAZZ_NODE_STORAGE_CODECS = [GROOVE_DURABLE_INDEX_V2_CODEC, "jazz.node-storage.v1"];
export const JAZZ_CATALOGUE_STORAGE_CODECS = ["jazz.catalogue-storage.v1"];
export const JAZZ_ACCOUNT_STORAGE_CODECS = ["jazz.account-storage.v1"];

export const nodeStorageCodecProfile = () =>
  StorageCodecProfile.grooveEpoch1().withAdditionalCodecs(JAZZ_NODE_STORAGE_CODECS);
export const catalogueStorageCodecProfile = () =>
  StorageCodecProfile.grooveEpoch1().withAdditionalCodecs(JAZZ_CATALOGUE_STORAGE_CODECS);
export const accountStorageCodecProfile = () =>
  StorageCodecProfile.grooveEpoch1().withAdditionalCodecs(JAZZ_ACCOUNT_STORAGE_CODECS);
