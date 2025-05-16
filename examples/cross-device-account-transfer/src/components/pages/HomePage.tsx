import { useIsAuthenticated } from "jazz-react";
import { AuthButtons } from "../AuthButtons.tsx";
import { Logo } from "../Logo.tsx";

export default function HomePage() {
  const isAuthenticated = useIsAuthenticated();

  return (
    <>
      <header>
        <nav className="container flex flex-col items-center gap-8 py-8 px-4 text-center">
          <p className="text-lg">
            {isAuthenticated ? "You're logged in!" : "Sign up or log in"}
          </p>

          <AuthButtons />
        </nav>
      </header>

      <main className="container flex flex-col gap-8">
        <Logo />
      </main>
    </>
  );
}
