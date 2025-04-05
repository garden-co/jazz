import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import React from "react";
import { MMKVStore } from "./storage/mmkv-store-adapter.js";
import { OPSQLiteAdapter } from "./storage/op-sqlite-adapter.js";

export function JazzProvider(props: JazzProviderProps) {
  // Destructure kvStore and pass everything else via rest
  const { kvStore, storage, ...rest } = props;

  return (
    <JazzProviderCore
      {...rest}
      storage={storage ?? new OPSQLiteAdapter()}
      kvStore={kvStore ?? new MMKVStore()}
    />
  );
}
