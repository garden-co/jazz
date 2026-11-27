import { Center, Spinner } from "@astryxdesign/core";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { authClient } from "./lib/auth-client";
import { db } from "./lib/db";

/** Signed out: the sign-in form. Signed in: BandChat, synced with Instant. */
export function App() {
  const { isLoading, user } = db.useAuth();
  return (
    <ThemeProvider>
      {isLoading ? (
        <Center axis="both" padding={4} className="page-fill">
          <Spinner label="Loading BandChat…" />
        </Center>
      ) : user ? (
        <Dashboard userId={user.id} />
      ) : (
        <SignInForm />
      )}
    </ThemeProvider>
  );
}

function Dashboard({ userId }: { userId: string }) {
  // The name given at sign-up pre-fills the profile setup.
  const { data } = db.useQuery({ $users: { $: { where: { id: userId }, fields: ["name"] } } });
  return (
    <BandChat
      author={userId}
      defaultDisplayName={data?.$users[0]?.name ?? undefined}
      onSignOut={() => void authClient.signOut().then(() => window.location.assign("/"))}
    />
  );
}
