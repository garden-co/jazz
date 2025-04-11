import { useCreateMagicLinkAuth } from "jazz-react";
import { useState } from "react";
import { Button } from "./Button";
import { ConfirmationCodeInput } from "./ConfirmationCodeInput";
import { QRCodeContainer } from "./QRCodeContainer";

interface CreateMagicLinkAsConsumerProps {
  onLoggedIn: () => void;
}

export function CreateMagicLinkAsConsumer({
  onLoggedIn,
}: CreateMagicLinkAsConsumerProps) {
  const [link, setLink] = useState<string | null>(null);

  const { status, createLink, sendConfirmationCode } = useCreateMagicLinkAuth({
    mode: "authenticate-current-device",
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
      <div className="flex flex-col items-center gap-4">
        <p>Scan QR code to log in</p>

        {link ? <QRCodeContainer url={link} /> : null}
      </div>
    );
  }

  if (status === "confirmationCodeRequired") {
    return (
      <div className="flex flex-col items-center gap-4">
        <p>Enter the confirmation code displayed on your other device</p>

        {sendConfirmationCode ? (
          <ConfirmationCodeInput onSubmit={sendConfirmationCode} />
        ) : null}
      </div>
    );
  }

  if (status === "confirmationCodePending") {
    return <p>Confirming...</p>;
  }

  if (status === "confirmationCodeIncorrect") {
    return (
      <div className="flex flex-col gap-4">
        <p>Incorrect confirmation code!</p>

        <Button color="primary" onClick={onCreateLink}>
          Start again
        </Button>
      </div>
    );
  }

  if (status === "authorized") {
    return <p>Logged in!</p>;
  }

  if (status === "error") {
    return (
      <div className="flex flex-col gap-4">
        <p>Something went wrong</p>

        <button onClick={onCreateLink}>Try again</button>
      </div>
    );
  }

  return null;
}
