import { type JSX, type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

// Your backend names the user (its authenticate reads your session cookie).
// In `signedOut`, call useJazzSession().refresh() after your own sign-in.
export function AuthJwt(props: ParentProps<{ signedOut: JSX.Element }>) {
  return (
    <JazzProvider
      config={{ serverUrl: "/jazz" }}
      signedOut={props.signedOut}
      loading={<p>Loading...</p>}
    >
      {props.children}
    </JazzProvider>
  );
}
