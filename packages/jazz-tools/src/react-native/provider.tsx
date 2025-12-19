import { Account, AnyAccountSchema, CoValueFromRaw } from "jazz-tools";
import {
  JazzProviderCore,
  JazzProviderProps,
} from "jazz-tools/react-native-core";
import React, { useMemo } from "react";
import { MMKVStore } from "./storage/mmkv-store-adapter.js";
import { OPSQLiteAdapter } from "./storage/op-sqlite-adapter.js";

export function JazzReactNativeProvider<S extends AnyAccountSchema>(
  props: JazzProviderProps<S>,
) {
  // Destructure kvStore and pass everything else via rest
  const storage = useMemo(() => {
    return props.storage ?? new OPSQLiteAdapter();
  }, [props.storage]);

  const kvStore = useMemo(() => {
    return props.kvStore ?? new MMKVStore();
  }, [props.kvStore]);

  return <JazzProviderCore<S> {...props} storage={storage} kvStore={kvStore} />;
}
