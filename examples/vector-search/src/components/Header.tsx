import { useIsAuthenticated } from "jazz-tools/react";
import { AuthButton } from "./AuthButton";
import { Logo } from "./Logo";

export function Header({ isSeeding }: { isSeeding: boolean }) {
  const isAuthenticated = useIsAuthenticated();

  return (
    <header className="z-50 border-b border-zinc-200 bg-zinc-50 py-3 px-4 flex flex-col gap-2 md:flex-row md:items-center justify-between">
      <div className="flex flex-col gap-1">
        <a
          className="inline-block w-20 cursor-pointer"
          href="https://jazz.tools"
          target="_blank"
          rel="noopener noreferrer"
        >
          <Logo />
        </a>

        <span className="text-xs text-zinc-500">
          Demo of local-first vector similarity search in Jazz.{" "}
          <a
            className="underline hover:no-underline"
            href="https://github.com/garden-co/jazz/tree/main/examples/vector-search"
            target="_blank"
            rel="noopener noreferrer"
          >
            Source →
          </a>
        </span>
      </div>

      <nav
        className={[
          "flex flex-col gap-2 md:flex-row md:items-center justify-between border-t border-zinc-200 pt-2 md:pt-0 md:border-t-0 transition-opacity",
          isSeeding && "opacity-0 pointer-events-none",
        ].join(" ")}
      >
        <div className="text-xs text-zinc-500">
          {isAuthenticated ? (
            <>You're logged in.</>
          ) : (
            <>Authenticate to share the data with another device.</>
          )}
        </div>

        <div className="flex gap-2">
          <AuthButton />
        </div>
      </nav>
    </header>
  );
}
