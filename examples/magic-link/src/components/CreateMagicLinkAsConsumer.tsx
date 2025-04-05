"use client";

import { useCreateMagicLinkAuthAsConsumer } from "jazz-react";
import { useState } from "react";
import { Button } from "./Button";
import { QRCodeContainer } from "./QRCodeContainer";

interface CreateMagicLinkAsConsumerProps {
  onLoggedIn: () => void;
}

export function CreateMagicLinkAsConsumer({
  onLoggedIn,
}: CreateMagicLinkAsConsumerProps) {
  const [link, setLink] = useState<string | null>(null);

  const { status, createLink } = useCreateMagicLinkAuthAsConsumer({
    consumerHandlerPath: "/#/magic-link-handler-consumer",
    providerHandlerPath: "/#/magic-link-handler-provider",
    onLoggedIn,
  });

  const onCreateLink = () => createLink().then(setLink);

  if (status === "idle") {
    return (
      <Button color="primary" onClick={onCreateLink}>
        Create QR code
      </Button>
    );
  }

  if (status === "waitingForProvider") {
    return (
      <div className="flex flex-col gap-2">
        <p>Scan QR code to log in</p>

        {link ? <QRCodeContainer url={link} /> : null}
      </div>
    );
  }

  if (status === "waitingForConfirmLogIn") {
    return (
      <div className="flex flex-col gap-2">
        <p>Please confirm the log in on your mobile device</p>
      </div>
    );
  }

  if (status === "authorized") return <p>Logged in!</p>;
  if (status === "error") {
    return (
      <div className="flex flex-col gap-2">
        <p>Something went wrong</p>

        <button onClick={onCreateLink}>Try again</button>
      </div>
    );
  }

  return null;
}
