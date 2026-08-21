import { type ParentProps } from "solid-js";
import { JazzProvider } from "jazz-tools/solid";

export function AuthJwt(props: ParentProps) {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "http://127.0.0.1:4200",
        jwtToken: "<provider-jwt>",
      }}
    >
      {props.children}
    </JazzProvider>
  );
}
