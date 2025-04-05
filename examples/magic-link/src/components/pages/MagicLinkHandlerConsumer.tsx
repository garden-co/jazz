import { useHandleMagicLinkAuthAsConsumer } from "jazz-react";

export default function MagicLinkHandlerConsumerPage() {
  return (
    <main className="container flex flex-col items-center gap-4 px-4 py-8 text-center">
      <h1 className="text-xl">Magic Link Auth Consumer Handler</h1>
      <MagicLinkHandlerConsumer />
    </main>
  );
}

function MagicLinkHandlerConsumer() {
  const { status } = useHandleMagicLinkAuthAsConsumer({
    consumerHandlerPath: "/#/magic-link-handler-consumer",
    providerHandlerPath: "/#/magic-link-handler-provider",
    onLoggedIn: () => {
      console.log("logged in!");
    },
  });

  if (status === "idle") return <p>Loading...</p>;

  if (status === "waitingForProvider") {
    return <p>Waiting for provider to confirm log in...</p>;
  }

  if (status === "authorized") return <p>Logged in!</p>;
  if (status === "error") return <p>Something went wrong</p>;
}
