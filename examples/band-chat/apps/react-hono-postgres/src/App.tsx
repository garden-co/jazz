import { Center, Spinner } from "@astryxdesign/core";
import { DataProvider, useBandChatLifecycle } from "@/components/data-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { authClient } from "./lib/auth-client";

/** Signed out: the sign-in form. Signed in: BandChat, kept live by the server's WebSocket. */
export function App() {
  const { data: session, isPending } = authClient.useSession();
  return (
    <ThemeProvider>
      {isPending ? (
        <Center axis="both" padding={4} className="page-fill">
          <Spinner label="Loading BandChat…" />
        </Center>
      ) : session ? (
        <DataProvider>
          <Dashboard author={session.user.id} name={session.user.name} />
        </DataProvider>
      ) : (
        <SignInForm />
      )}
    </ThemeProvider>
  );
}

function Dashboard({ author, name }: { author: string; name: string }) {
  const { signOut } = useBandChatLifecycle();
  return <BandChat author={author} defaultDisplayName={name} onSignOut={() => void signOut()} />;
}
