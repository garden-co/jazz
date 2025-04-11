"use client";

import { useAccount, usePasskeyAuth } from "jazz-react";
import { useState } from "react";
import { APPLICATION_NAME } from "../main";
import { Button } from "./Button";
import { Card } from "./Card";
import { CreateMagicLinkAsConsumer } from "./CreateMagicLinkAsConsumer";
import { CreateMagicLinkAsProvider } from "./CreateMagicLinkAsProvider";

export function AuthButtons() {
  const { logOut } = useAccount();

  const auth = usePasskeyAuth({ appName: APPLICATION_NAME });

  const [magicLinkFlow, setMagicLinkFlow] = useState(false);

  function handleLogOut() {
    logOut();
    window.history.pushState({}, "", "/");
  }

  if (auth.state === "signedIn") {
    return (
      <div className="flex flex-col items-center gap-8">
        <div className="flex gap-2 justify-center flex-wrap">
          <Button color="destructive" onClick={handleLogOut}>
            Log out
          </Button>

          <Button color="primary" onClick={() => setMagicLinkFlow(true)}>
            ✨ Get your mobile device logged in ✨
          </Button>
        </div>

        {magicLinkFlow ? (
          <Card className="w-full">
            <CreateMagicLinkAsProvider />
          </Card>
        ) : null}
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center gap-8">
      <div className="flex gap-2 justify-center flex-wrap">
        <Button onClick={() => auth.signUp("supercoolusername")}>
          Sign up
        </Button>

        <Button onClick={() => auth.logIn()}>Log in with passkey</Button>

        <Button color="primary" onClick={() => setMagicLinkFlow(true)}>
          ✨ Use mobile device to log in ✨
        </Button>
      </div>

      {magicLinkFlow ? (
        <Card className="w-full">
          <CreateMagicLinkAsConsumer
            onLoggedIn={() => {
              console.log("logged in!");
              setMagicLinkFlow(false);
            }}
          />
        </Card>
      ) : (
        <p className="text-balance">
          You can also use the{" "}
          <span className="font-bold">Get your mobile device logged in</span>{" "}
          button on an already logged-in device
        </p>
      )}
    </div>
  );
}
