import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
} from "jazz-tools";
import React, { useEffect, useMemo } from "react";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";
import { ExpoSQLiteAdapter } from "./storage/expo-sqlite-adapter.js";

let jazzStorage: JazzProviderProps<AnyAccountSchema>["storage"] = undefined;
export const clearLocalData = async () => {
  if (
    jazzStorage &&
    jazzStorage !== "disabled" &&
    "clearLocalData" in jazzStorage
  ) {
    await (jazzStorage as ExpoSQLiteAdapter).clearLocalData();
  }
};

export function JazzProvider<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(props: JazzProviderProps<S>) {
  const storage = useMemo(() => {
    return props.storage ?? new ExpoSQLiteAdapter();
  }, [props.storage]);
  useEffect(() => {
    jazzStorage = storage;
  }, [storage]);

  const kvStore = useMemo(() => {
    return props.kvStore ?? new ExpoSecureStoreAdapter();
  }, [props.kvStore]);

  return <JazzProviderCore {...props} storage={storage} kvStore={kvStore} />;
}
