import { JazzProvider, useJazzSession, type JazzUser } from "./index.js";
import type { AccountStore } from "../accounts/persistence.js";
declare const store: AccountStore;
function Data() {
  const { status, user, refresh, createDeviceAccount, linkDeviceAccount, exportDeviceKey } =
    useJazzSession();
  const signedIn: JazzUser | null = user;
  void signedIn?.kind;
  void refresh;
  void createDeviceAccount;
  void linkDeviceAccount;
  void exportDeviceKey;
  return status;
}
export const nativeApplication = (
  <JazzProvider
    config={{ scope: "native-app", serverUrl: "https://sync.example.test", store }}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
export const nativeApplicationWithoutGuests = (
  <JazzProvider
    config={{ serverUrl: "https://app.example.test/jazz", store, initial: "signed-out" }}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
export const nativeDeviceAccounts = (
  <JazzProvider config={{ serverUrl: "https://app.example.test/jazz", store, initial: "device" }}>
    <Data />
  </JazzProvider>
);
