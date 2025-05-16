import { useAcceptAccountTransfer } from "jazz-react";
import { BackToHomepageContainer } from "../BackToHomepageContainer";

export default function CrossDeviceAccountTransferHandlerSource() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-xl">Account Transfer Source Handler</h1>
      <HandleAccountTransferAsSource />
    </main>
  );
}

function HandleAccountTransferAsSource() {
  const { status, confirmationCode } = useAcceptAccountTransfer({
    as: "source",
    handlerPath: "/#/share-current-account",
  });

  switch (status) {
    case "idle":
      return <p>Loading...</p>;

    case "confirmationCodeGenerated":
      return (
        <>
          <p>Confirmation code:</p>
          <p className="font-medium text-3xl tracking-widest">
            {confirmationCode ?? "empty"}
          </p>
          <p className="text-red-600">Never share this code with anyone!</p>
        </>
      );

    case "authorized":
      return (
        <BackToHomepageContainer>
          Your device has been logged in!
        </BackToHomepageContainer>
      );

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>Incorrect confirmation code</p>
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
