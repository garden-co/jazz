import { useCreateAccountTransfer } from "jazz-react";
import { useState } from "react";
import { Button } from "./Button";
import { ConfirmationCodeForm } from "./ConfirmationCodeForm";
import { QRCode } from "./QRCode";

interface CreateCrossDeviceAccountTransferAsTargetProps {
  onLoggedIn: () => void;
}

export function CreateCrossDeviceAccountTransferAsTarget({
  onLoggedIn,
}: CreateCrossDeviceAccountTransferAsTargetProps) {
  const [link, setLink] = useState<string | undefined>();

  const { status, createLink, sendConfirmationCode } = useCreateAccountTransfer(
    {
      as: "target",
      handlerPath: "/#/share-current-account",
      onLoggedIn,
    },
  );

  const onCreateLink = () => createLink().then(setLink);

  switch (status) {
    case "idle":
      return (
        <Button color="primary" onClick={onCreateLink}>
          Create QR code
        </Button>
      );

    case "waitingForHandler":
      return (
        <>
          <p>Scan QR code to log in</p>

          {link ? <QRCode url={link} /> : null}
        </>
      );

    case "confirmationCodeRequired":
      return (
        <>
          <p>Enter the confirmation code displayed on your other device</p>

          {sendConfirmationCode ? (
            <ConfirmationCodeForm onSubmit={sendConfirmationCode} />
          ) : null}
        </>
      );

    case "confirmationCodePending":
      return <p>Confirming...</p>;

    case "authorized":
      return <p>Logged in!</p>;

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>Incorrect confirmation code!</p>

          <Button color="primary" onClick={onCreateLink}>
            Try again
          </Button>
        </>
      );

    case "error":
      return (
        <>
          <p>Something went wrong</p>

          <Button onClick={onCreateLink}>Try again</Button>
        </>
      );

    case "cancelled":
      return (
        <>
          <p>Cancelled</p>

          <Button onClick={onCreateLink}>Try again</Button>
        </>
      );

    default:
      const check: never = status;
      if (check) throw new Error(`Unhandled status: ${check}`);
  }

  return null;
}
