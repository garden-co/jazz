import { JazzProvider, backendAuth, useJazzAuth } from "./index.js";
import type { AccountStore } from "../accounts/persistence.js";
declare const store: AccountStore;
function Data() {
  const { status, sessionActions } = useJazzAuth();
  void sessionActions;
  return status;
}
export const nativeApplication = (
  <JazzProvider
    appId="native-app"
    serverUrl="https://sync.example.test"
    store={store}
    auth={backendAuth({ key: null, logout: async () => {} })}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
declare const signedInUser: string | null | undefined;
export const nativeApplicationFollowingUser = (
  <JazzProvider
    serverUrl="https://app.example.test/jazz"
    store={store}
    user={signedInUser}
    signedOut={null}
  >
    <Data />
  </JazzProvider>
);
