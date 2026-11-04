import { JazzProvider, useJazzAuth, type JazzUser } from "./index.js";
import type { AccountStore } from "../accounts/persistence.js";
declare const store: AccountStore;
function Data() {
  const { status, sessionActions, user, reauthenticate } = useJazzAuth();
  const signedIn: JazzUser | null = user;
  void signedIn;
  void sessionActions;
  void reauthenticate;
  return status;
}
export const nativeApplication = (
  <JazzProvider
    appId="native-app"
    serverUrl="https://sync.example.test"
    store={store}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
export const nativeApplicationWithoutGuests = (
  <JazzProvider
    serverUrl="https://app.example.test/jazz"
    store={store}
    initial="signed-out"
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
