import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
} from "jazz-tools";
import React, { useMemo } from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";
import { ExpoSQLiteAdapter } from "./storage/expo-sqlite-adapter.js";

export function JazzProvider<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(props: JazzProviderProps<S>) {
  const storage = useMemo(() => {
    return props.storage ?? new ExpoSQLiteAdapter();
  }, [props.storage]);

  const kvStore = useMemo(() => {
    return props.kvStore ?? new ExpoSecureStoreAdapter();
  }, [props.kvStore]);

  return <JazzProviderCore {...props} storage={storage} kvStore={kvStore} />;
}
