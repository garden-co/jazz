import { AnyAccountSchema } from "jazz-tools";
import {
  JazzProviderCore,
  JazzProviderProps,
} from "jazz-tools/react-native-core";
import React, { useMemo } from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";
import { ExpoSQLiteAdapter } from "./storage/expo-sqlite-adapter.js";

export function JazzExpoProvider<S extends AnyAccountSchema>(
  props: JazzProviderProps<S>,
) {
  const storage = useMemo(() => {
    return props.storage ?? new ExpoSQLiteAdapter();
  }, [props.storage]);

  const kvStore = useMemo(() => {
    return props.kvStore ?? new ExpoSecureStoreAdapter();
  }, [props.kvStore]);

  return <JazzProviderCore<S> {...props} storage={storage} kvStore={kvStore} />;
}
