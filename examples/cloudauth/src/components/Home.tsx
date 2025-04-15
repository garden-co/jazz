import { useAccount, useIsAuthenticated } from "jazz-react";
import { Button } from "./atoms/Button";

export function Home() {
  const { me, logOut } = useAccount({ resolve: { root: true, profile: true } });
  const isAuthenticated = useIsAuthenticated();

  if (!me) return;
  if (!isAuthenticated) return;

  return (
    <main className="min-h-screen flex flex-col justify-center">
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        <h1 className="flex flex-col gap-6 text-lg">
          Welcome back, {me?.profile?.name}.
        </h1>
        <Button variant="destructive" onClick={() => logOut()}>
          Logout
        </Button>
      </div>
    </main>
  );
}
