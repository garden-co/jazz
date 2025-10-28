type StorageOption = "indexedDB";
type CombinedStorageOption = ["indexedDB"];
export type StorageConfig =
  | StorageOption
  | CombinedStorageOption
  | [StorageOption];

export function getStorageOptions(storage?: StorageConfig): {
  useIndexedDB: boolean;
} {
  const useIndexedDB =
    !storage ||
    (Array.isArray(storage) && storage.includes("indexedDB")) ||
    storage === "indexedDB";

  return { useIndexedDB };
}
