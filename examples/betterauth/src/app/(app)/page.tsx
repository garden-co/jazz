"use client";

import { Button } from "@/components/ui/button";
import { Account } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import {
  AppWindowMacIcon,
  FileTextIcon,
  GlobeIcon,
  WrenchIcon,
} from "lucide-react";
import Image from "next/image";

export default function Home() {
  const { me } = useAccount(Account, { resolve: { profile: {} } });

  if (!me) {
    return null;
  }

  return (
    <div className="grow flex flex-col items-center justify-center">
      <main className="flex flex-col gap-8 row-start-2 grow justify-center">
        <Image
          src="/jazz-logo.svg"
          alt="Jazz logo"
          width={180}
          height={38}
          priority
        />
        <p className="text-sm/6 text-center sm:text-left">
          Signed in as{" "}
          <span className="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-mono font-semibold">
            {me.profile.name}
          </span>{" "}
          with id{" "}
          <span className="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-mono font-semibold">
            {me.$jazz.id}
          </span>
        </p>

        <div className="flex gap-4 items-center flex-row">
          <Button asChild size="lg">
            <a
              href="https://jazz.tools/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              <Image src="/jazz.svg" alt="" width={20} height={20} />
              Start building
            </a>
          </Button>

          <Button asChild variant="secondary" size="lg">
            <a
              href="https://jazz.tools/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              <FileTextIcon className="size-4" />
              Read the docs
            </a>
          </Button>
        </div>
      </main>

      <footer className="flex gap-4 py-8">
        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/api-reference"
            target="_blank"
            rel="noopener noreferrer"
          >
            <FileTextIcon className="size-4" />
            API reference
          </a>
        </Button>
        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/examples"
            target="_blank"
            rel="noopener noreferrer"
          >
            <AppWindowMacIcon className="size-4" />
            Examples
          </a>
        </Button>

        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/status"
            target="_blank"
            rel="noopener noreferrer"
          >
            <GlobeIcon className="size-4" />
            Status
          </a>
        </Button>

        <Button asChild variant="ghost">
          <a
            href="https://jazz.tools/showcase"
            target="_blank"
            rel="noopener noreferrer"
          >
            <WrenchIcon className="size-4" />
            Built with Jazz
          </a>
        </Button>
      </footer>
    </div>
  );
}
