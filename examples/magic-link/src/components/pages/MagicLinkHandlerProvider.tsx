import { useHandleMagicLinkAuth } from "jazz-react";
import { BackToHomepageContainer } from "../BackToHomepageContainer";

export default function MagicLinkHandlerProviderPage() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-xl">Magic Link Auth Provider Handler</h1>
      <MagicLinkHandlerProvider />
    </main>
  );
}

function MagicLinkHandlerProvider() {
  const { status, confirmationCode } = useHandleMagicLinkAuth({
    mode: "share-local-credentials",
    consumerHandlerPath: "/#/magic-link-handler-consumer",
    providerHandlerPath: "/#/magic-link-handler-provider",
  });

  if (status === "idle") {
    return <p>Loading...</p>;
  }

  if (status === "confirmationCodeGenerated") {
    return (
      <div className="flex flex-col items-center gap-2">
        <p>Confirmation code:</p>
        <p className="font-medium text-3xl tracking-widest">
          {confirmationCode ?? "empty"}
        </p>
        <p className="text-red-600">Never share this code with anyone!</p>
      </div>
    );
  }

  if (status === "confirmationCodeIncorrect") {
    return (
      <div className="flex flex-col gap-4">
        <p>Incorrect confirmation code</p>
        <p>Please try again</p>
      </div>
    );
  }

  if (status === "authorized") {
    return (
      <BackToHomepageContainer>
        Your device has been logged in!
      </BackToHomepageContainer>
    );
  }

  if (status === "error") {
    return (
      <BackToHomepageContainer>Something went wrong</BackToHomepageContainer>
    );
  }

  return null;
}
