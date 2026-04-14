export function getItemAsync(_key: string): Promise<string | null> {
  throw new Error("expo-secure-store is not available in test environment");
}

export function setItemAsync(_key: string, _value: string): Promise<void> {
  throw new Error("expo-secure-store is not available in test environment");
}

export function deleteItemAsync(_key: string): Promise<void> {
  throw new Error("expo-secure-store is not available in test environment");
}
