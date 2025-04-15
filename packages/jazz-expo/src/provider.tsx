import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import React from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";
import { ExpoSQLiteAdapter } from "./storage/expo-sqlite-adapter.js";

export function JazzProvider(props: JazzProviderProps) {
  // Destructure kvStore and pass everything else via rest
  const { kvStore, storage, ...rest } = props;

  return (
    <JazzProviderCore
      {...rest}
      storage={storage ?? new ExpoSQLiteAdapter()}
      kvStore={kvStore ?? new ExpoSecureStoreAdapter()}
    />
  );
}
