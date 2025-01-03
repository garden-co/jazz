import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";

export function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = useDemoAuth();

  return (
    <>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=chat-example-jazz@garden.co"
      >
        {children}
      </JazzProvider>
      {state.state !== "signedIn" && (
        <DemoAuthBasicUI appName="Jazz Chat" state={state} />
      )}
    </>
  );
}
