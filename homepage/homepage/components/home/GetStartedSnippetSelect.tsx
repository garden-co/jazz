'use client'

import { Framework } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import NpxCreateJazzApp from "@/components/home/NpxCreateJazzApp.mdx";
import { CopyButton } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import { useState, useEffect } from "react";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import Link from "next/link";
import { FrameworkSelect } from "../docs/FrameworkSelect";
import clsx from "clsx";
import { track } from "@vercel/analytics";
import { GappedGrid } from "@garden-co/design-system/src/components/molecules/GappedGrid";

export function GetStartedSnippetSelect() {
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);

  useEffect(() => {
    setSelectedFramework(defaultFramework);
  }, [defaultFramework]);

  return (
    <GappedGrid>
      <div className="relative w-full col-span-2 lg:col-span-3 border-2 border-primary rounded-lg overflow-hidden">
        <CopyButton
          code="npx create-jazz-app@latest"
          size="sm"
          className={clsx("mt-0.5 mr-0.5 z-100 md:opacity-100 hidden md:block")}
          onCopy={() => track("create-jazz-app command copied from hero")}
        />
        <NpxCreateJazzApp />
      </div>
      <div className="col-span-2 lg:col-span-3 flex flex-row gap-2">
        <div className="h-full items-center w-[175px]">
          <FrameworkSelect onSelect={setSelectedFramework} size="md" routerPush={false} className="h-full md:px-4" />
        </div>
        <div className="flex h-full items-center">
          <Button intent="primary" size="lg" className="w-full">
            <Link className="my-[0.11rem]" href={`/docs/${selectedFramework}`}>Get started</Link>
          </Button>
        </div>
      </div>
    </GappedGrid>
  );
}
