import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

// Everyone starts as a guest: a device account, created on this device.
// Your backend accepts guests with acceptDeviceAccounts.
export function AuthAnonymous(props: ParentProps) {
  return (
    <JazzProvider config={{ serverUrl: "/jazz", initial: "device" }} loading={<p>Loading...</p>}>
      {props.children}
    </JazzProvider>
  );
}
