import { useHandleMagicLinkAuth } from "jazz-react";
import { BackToHomepageContainer } from "../BackToHomepageContainer";
import { ConfirmationCodeInput } from "../ConfirmationCodeInput";

export default function MagicLinkHandlerConsumerPage() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-xl">Magic Link Auth Consumer Handler</h1>
      <MagicLinkHandlerConsumer />
    </main>
  );
}

function MagicLinkHandlerConsumer() {
  const { status, sendConfirmationCode } = useHandleMagicLinkAuth({
    mode: "authenticate-current-device",
    consumerHandlerPath: "/#/magic-link-handler-consumer",
    providerHandlerPath: "/#/magic-link-handler-provider",
    onLoggedIn: () => {
      console.log("logged in!");
    },
  });

  if (status === "idle") {
    return <p>Loading...</p>;
  }

  if (status === "confirmationCodeRequired") {
    return (
      <div className="flex flex-col items-center gap-4">
        <p>Enter the confirmation code displayed on your other device</p>

        {sendConfirmationCode ? (
          <ConfirmationCodeInput
            onSubmit={(code) => sendConfirmationCode(code)}
          />
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
        <p>Please try again</p>
      </div>
    );
  }

  if (status === "authorized") {
    return <BackToHomepageContainer>Logged in!</BackToHomepageContainer>;
  }

  if (status === "error") {
    return (
      <BackToHomepageContainer>Something went wrong</BackToHomepageContainer>
    );
  }

  return null;
}
