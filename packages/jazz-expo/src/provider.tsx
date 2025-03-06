import {
  JazzProviderCore,
  JazzProviderProps,
  KvStore,
} from "jazz-react-native-core";
import React from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";

// Create a new props type that makes kvStore optional
type ProviderProps = Omit<JazzProviderProps, "kvStore"> & {
  kvStore?: KvStore;
};

export function JazzProvider(props: ProviderProps) {
  // Destructure kvStore and pass everything else via rest
  const { kvStore, ...rest } = props;

  return (
    <JazzProviderCore
      {...rest}
      kvStore={kvStore ?? new ExpoSecureStoreAdapter()}
    />
  );
}
