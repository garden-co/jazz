import { useAcceptAccountTransfer } from "jazz-react";
import { BackToHomepageContainer } from "../BackToHomepageContainer";
import { ConfirmationCodeForm } from "../ConfirmationCodeForm";

export default function CrossDeviceAccountTransferHandlerTarget() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-xl">Account Transfer Target Handler</h1>
      <HandleAccountTransferAsTarget />
    </main>
  );
}

function HandleAccountTransferAsTarget() {
  const { status, sendConfirmationCode } = useAcceptAccountTransfer({
    as: "target",
    handlerPath: "/#/accept-account-transfer",
    onLoggedIn: () => {
      console.log("logged in!");
    },
  });

  switch (status) {
    case "idle":
      return <p>Loading...</p>;

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
      return <BackToHomepageContainer>Logged in!</BackToHomepageContainer>;

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>Incorrect confirmation code!</p>
          <p>Please try again</p>
        </>
      );

    case "error":
      return (
        <BackToHomepageContainer>Something went wrong</BackToHomepageContainer>
      );

    case "cancelled":
      return <BackToHomepageContainer>Login cancelled</BackToHomepageContainer>;

    default:
      const check: never = status;
      if (check) throw new Error(`Unhandled status: ${check}`);
  }
}
