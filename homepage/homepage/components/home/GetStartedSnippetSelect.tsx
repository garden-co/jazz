'use client'

import { Framework } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import NpxCreateJazzApp from "@/components/home/NpxCreateJazzApp.mdx";
import { CopyButton } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import { useState } from "react";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import Link from "next/link";
import { FrameworkSelect } from "../docs/FrameworkSelect";
import clsx from "clsx";

const getStartedSnippet = (framework: Framework) => {
  const props = framework !== Framework.React ? ` --framework ${framework}` : "";
  return `npx create-jazz-app@latest${props}`;
};

export function GetStartedSnippetSelect() {
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);

  console.log(selectedFramework);

  const snippet = getStartedSnippet(selectedFramework);


  return (
    <div className="flex flex-col grid grid-cols-8 gap-2 w-full col-span-2 justify-end relative mt-9 tex-sm">
      <div className="relative w-full col-span-4 md:col-span-4 border-2 border-primary rounded-lg overflow-hidden">
        <CopyButton
          code={snippet}
          size="sm"
          className={clsx("mt-0.5 mr-0.5 z-100 md:opacity-100")}
        />
        <NpxCreateJazzApp />
        {/* {snippet} */}
      </div>
      <div className="col-span-2 md:col-span-1 col-start-5 md:col-start-6 flex align-middle h-full items-center">
        <Button variant="primary" size="sm" className="w-full h-fit">
          <Link className="my-[0.11rem]" href={`/docs/${selectedFramework}`}>Get started</Link>
        </Button>
      </div>
      <div className="col-span-2 md:col-span-1 flex h-full items-center">
      <FrameworkSelect onSelect={setSelectedFramework} size="sm" routerPush={false} />
      </div>
    </div>
  );
}
