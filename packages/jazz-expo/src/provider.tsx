import { JazzProviderCore, JazzProviderProps } from "jazz-react-native-core";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
} from "jazz-tools";
import React from "react";
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
  // Destructure kvStore and pass everything else via rest
  const { kvStore, storage, ...rest } = props;

  jazzStorage = storage ?? new ExpoSQLiteAdapter();

  return (
    <JazzProviderCore
      {...rest}
      storage={jazzStorage}
      kvStore={kvStore ?? new ExpoSecureStoreAdapter()}
    />
  );
}
