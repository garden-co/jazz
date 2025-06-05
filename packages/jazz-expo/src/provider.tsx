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

export function JazzProvider<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(props: JazzProviderProps<S>) {
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
