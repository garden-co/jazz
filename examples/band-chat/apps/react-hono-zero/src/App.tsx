import { Center, Spinner } from "@astryxdesign/core";
import { ZeroProvider, useBandChatLifecycle } from "@/components/zero-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { authClient } from "./lib/auth-client";

/** Signed out: the sign-in form. Signed in: BandChat, synced through zero-cache. */
export function App() {
  const { data: session, isPending } = authClient.useSession();
  return (
    <ThemeProvider>
      {isPending ? (
        <Center axis="both" padding={4} className="page-fill">
          <Spinner label="Loading BandChat…" />
        </Center>
      ) : session ? (
        <ZeroProvider userID={session.user.id}>
          <Dashboard author={session.user.id} name={session.user.name} />
        </ZeroProvider>
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
