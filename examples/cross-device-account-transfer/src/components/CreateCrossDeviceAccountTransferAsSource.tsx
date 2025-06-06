import { useCreateAccountTransfer } from "jazz-react";
import { useState } from "react";
import { Button } from "./Button";
import { QRCode } from "./QRCode";

export function CreateCrossDeviceAccountTransferAsSource() {
  const [link, setLink] = useState<string | undefined>();

  const { status, createLink, confirmationCode } = useCreateAccountTransfer({
    as: "source",
    handlerPath: "/#/accept-account-transfer",
  });

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
          <p>Scan QR code to get your mobile device logged in</p>

          {link ? <QRCode url={link} /> : null}
        </>
      );

    case "confirmationCodeGenerated":
      return (
        <>
          <p>Confirmation code:</p>

          <p className="font-medium text-3xl tracking-widest">
            {confirmationCode ?? "empty"}
          </p>
        </>
      );

    case "confirmationCodeCorrect":
      return <p>Confirmed! Logging in...</p>;

    case "authorized":
      return <p>Your device has been logged in!</p>;

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>Incorrect confirmation code</p>

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
          <p>Login cancelled</p>

          <Button color="primary" onClick={onCreateLink}>
            Try again
          </Button>
        </>
      );

    default:
      const check: never = status;
      if (check) throw new Error(`Unhandled status: ${check}`);
  }

  return null;
}
