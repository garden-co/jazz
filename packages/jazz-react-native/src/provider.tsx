import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import React from "react";
import { MMKVStore } from "./storage/mmkv-store-adapter.js";

export function JazzProvider(props: JazzProviderProps) {
  // Destructure kvStore and pass everything else via rest
  const { kvStore, ...rest } = props;

  return <JazzProviderCore {...rest} kvStore={kvStore ?? new MMKVStore()} />;
}
