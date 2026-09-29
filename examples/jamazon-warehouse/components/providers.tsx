"use client";

import { Theme } from "@astryxdesign/core";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";
import { jazzTheme } from "@garden-co/design/jazz";
import { betterAuth, JazzProvider } from "jazz-tools/react";
import type { ReactNode } from "react";
import { authClient } from "@/src/lib/auth-client";
import { jazzAppId, jazzServerUrl } from "@/src/lib/config";
import { Console } from "./console";
import { SignIn } from "./sign-in";

const jazzAuth = betterAuth(authClient);

export function Providers({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      <JazzProvider
        appId={jazzAppId}
        serverUrl={jazzServerUrl}
        auth={jazzAuth}
        signedOut={<SignIn />}
        loading={<Loading />}
        error={(state) => (
          <Center height="100dvh">
            <Banner
              status="error"
              title="Couldn't open your operator account"
              description="The sync server did not accept this session."
              endContent={<Button label="Try again" onClick={() => void state.retry()} />}
            />
          </Center>
        )}
      >
        <Console>{children}</Console>
      </JazzProvider>
    </Theme>
  );
}

export function Loading() {
  return (
    <Center height="100dvh">
      <Spinner label="Opening the console" />
    </Center>
  );
}
