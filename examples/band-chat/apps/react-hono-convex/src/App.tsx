import { Center, Spinner } from "@astryxdesign/core";
import { useConvexAuth, useQuery } from "convex/react";
import { ConvexProvider, useBandChatLifecycle } from "@/components/convex-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { api } from "@/convex/_generated/api";
import { BandChat } from "./BandChat";

/** Signed out: the sign-in form. Signed in: BandChat, live through Convex. */
export function App() {
  return (
    <ThemeProvider>
      <ConvexProvider>
        <Session />
      </ConvexProvider>
    </ThemeProvider>
  );
}

function Session() {
  const { isLoading, isAuthenticated } = useConvexAuth();
  const viewer = useQuery(api.users.viewer, isAuthenticated ? {} : "skip");
  if (isLoading || (isAuthenticated && viewer === undefined))
    return (
      <Center axis="both" padding={4} className="page-fill">
        <Spinner label="Loading BandChat…" />
      </Center>
    );
  if (!isAuthenticated || !viewer) return <SignInForm />;
  return <Dashboard author={viewer.id} name={viewer.name} />;
}

function Dashboard({ author, name }: { author: string; name: string }) {
  const { signOut } = useBandChatLifecycle();
  return <BandChat author={author} defaultDisplayName={name} onSignOut={() => void signOut()} />;
}
