import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import React from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";

export function JazzProvider(props: JazzProviderProps) {
  // Destructure kvStore and pass everything else via rest
  const { kvStore, ...rest } = props;

  return (
    <JazzProviderCore
      {...rest}
      kvStore={kvStore ?? new ExpoSecureStoreAdapter()}
    />
  );
}
