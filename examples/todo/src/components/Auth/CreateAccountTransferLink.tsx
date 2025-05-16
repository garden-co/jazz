import { Button } from "@/basicComponents/ui/button";
import { useCreateAccountTransfer } from "jazz-react";
import { useEffect, useRef, useState } from "react";
import {
  InputOTP,
  InputOTPGroup,
  InputOTPSlot,
} from "../../basicComponents/ui/input-otp";
import { QRCode } from "./QRCode";

interface CreateAccountTransferLinkProps {
  onLoggedIn: () => void;
}

export function CreateAccountTransferLink({
  onLoggedIn,
}: CreateAccountTransferLinkProps) {
  const [link, setLink] = useState<string | undefined>();
  const [confirmationCode, setConfirmationCode] = useState("");
  const createdLinkRef = useRef<boolean>(false);

  const { status, createLink, sendConfirmationCode } = useCreateAccountTransfer(
    {
      as: "target",
      handlerPath: "/#/accept-account-transfer",
      onLoggedIn,
    },
  );

  const onCreateLink = () => createLink().then(setLink);

  useEffect(() => {
    if (createdLinkRef.current) return;
    createdLinkRef.current = true;
    onCreateLink();
  }, []);

  switch (status) {
    case "idle":
      return <p>Loading...</p>;

    case "waitingForHandler":
      return (
        <>
          <p>Scan the QR code from your logged-in mobile device</p>
          {link ? <QRCode url={link} /> : null}
        </>
      );

    case "confirmationCodeRequired":
      return (
        <>
          <p>Enter the code displayed on your mobile device</p>
          <InputOTP
            maxLength={6}
            value={confirmationCode}
            onChange={(value) => setConfirmationCode(value)}
            onComplete={() => sendConfirmationCode?.(confirmationCode)}
          >
            <InputOTPGroup>
              <InputOTPSlot index={0} />
              <InputOTPSlot index={1} />
              <InputOTPSlot index={2} />
              <InputOTPSlot index={3} />
              <InputOTPSlot index={4} />
              <InputOTPSlot index={5} />
            </InputOTPGroup>
          </InputOTP>
        </>
      );

    case "confirmationCodePending":
      return <p>Checking code...</p>;

    case "authorized":
      return <p>Authorized! 🚀</p>;

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>Incorrect code - please try again</p>
          <Button onClick={onCreateLink} className="w-full">
            Try again
          </Button>
        </>
      );

    case "error":
      return (
        <>
          <p>Oops! Something went wrong</p>
          <Button onClick={onCreateLink} className="w-full">
            Try again
          </Button>
        </>
      );

    case "cancelled":
      return (
        <>
          <p>Authentication cancelled</p>
          <Button onClick={onCreateLink} className="w-full">
            Try again
          </Button>
        </>
      );

    default:
      const check: never = status;
      if (check) throw new Error(`Unhandled status: ${check}`);
  }
}
