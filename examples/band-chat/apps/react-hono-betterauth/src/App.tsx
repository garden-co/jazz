import { Center, Spinner } from "@astryxdesign/core";
import { JazzProvider, useBandChatLifecycle } from "@/components/jazz-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { authClient } from "./lib/auth-client";

/** Signed out: the sign-in form. Signed in: BandChat, synced through `/jazz`. */
export function App() {
  const { data: session, isPending } = authClient.useSession();
  return (
    <ThemeProvider>
      {isPending ? (
        <Center axis="both" padding={4} className="page-fill">
          <Spinner label="Loading BandChat…" />
        </Center>
      ) : session ? (
        <JazzProvider>
          <Dashboard name={session.user.name} />
        </JazzProvider>
      ) : (
        <SignInForm />
      )}
    </ThemeProvider>
  );
}

function Dashboard({ name }: { name: string }) {
  const { signOut } = useBandChatLifecycle();
  return <BandChat defaultDisplayName={name} onSignOut={() => void signOut()} />;
}
