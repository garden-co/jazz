import { Button } from "@/basicComponents/ui/button";
import { useAcceptAccountTransfer } from "jazz-react";
import { useNavigate } from "react-router-dom";

export function AccountTransferLinkHandlerPage() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-2xl font-bold">Get your device logged in</h1>
      <div className="flex flex-col items-center gap-4 p-6">
        <AccountTransferHandler />
      </div>
    </main>
  );
}

export function AccountTransferHandler() {
  const navigate = useNavigate();

  const { status, confirmationCode } = useAcceptAccountTransfer({
    as: "source",
    handlerPath: "/#/accept-account-transfer",
  });

  const handleBackToHome = () => {
    navigate("/");
  };

  switch (status) {
    case "idle":
      return <p>Loading...</p>;

    case "confirmationCodeGenerated":
      return (
        <>
          <p>Enter this code on your other device</p>
          <p className="font-medium text-3xl tracking-widest text-center">
            {confirmationCode ?? "empty"}
          </p>
          <p className="text-red-500">Never share this code with anyone!</p>
        </>
      );

    case "authorized":
      return (
        <>
          <p>Your device has been logged in! 🚀</p>
          <Button onClick={handleBackToHome} className="w-full">
            Back to home
          </Button>
        </>
      );

    case "confirmationCodeIncorrect":
      return (
        <>
          <p>The confirmation code was incorrect - please try again</p>
          <Button onClick={handleBackToHome} className="w-full">
            Back to home
          </Button>
        </>
      );

    case "error":
      return (
        <>
          <p>Oops! Something went wrong</p>
          <Button onClick={handleBackToHome} className="w-full">
            Back to home
          </Button>
        </>
      );

    case "cancelled":
      return (
        <>
          <p>The login process was cancelled</p>
          <Button onClick={handleBackToHome} className="w-full">
            Back to home
          </Button>
        </>
      );

    default:
      const check: never = status;
      if (check) throw new Error(`Unhandled status: ${check}`);
  }
}
