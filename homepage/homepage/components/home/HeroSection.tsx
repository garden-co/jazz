"use client";

import { marketingCopy } from "@/content/marketingCopy";
import { H1 } from "@garden-co/design-system/src/components/atoms/Headings";
import { Kicker } from "@garden-co/design-system/src/components/atoms/Kicker";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import Link from "next/link";
import { CodeTabs } from "@/components/home/CodeTabs";
import { JazzSyncs } from "@/components/icons/JazzSyncs";
import NpxCreateJazzApp from "@/components/home/NpxCreateJazzApp.mdx";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { CopyButton } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import clsx from "clsx";
import { track } from "@vercel/analytics";
import { SupportedEnvironmentsSection } from "./SupportedEnvironmentsSection";

export function HeroSection() {
  return (
    <section className="container grid items-start gap-8 py-12 md:grid-cols-12 md:gap-0 md:py-20">
      <div className="md:col-span-4">
        {/* <Kicker className="mb-6">Ship Better Apps, Faster.</Kicker> */}
        <H1 className="text-4xl md:text-5xl lg:text-5xl">
          All you need is sync.
          {/* <JazzSyncs className="max-w-96 md:max-w-full" aria-hidden="true" /> */}
          <span className="sr-only">{marketingCopy.headline}</span>
        </H1>
        <Prose
          size="lg"
          className="mt-6 prose-p:leading-normal dark:text-white"
        >
          <p>
            Jazz is a distributed database that runs across your frontend,
            containers, serverless functions - and its own cloud.
          </p>
          <p>
            It instantly syncs data, files and LLM streams. It feels like
            reactive local JSON.
          </p>
        </Prose>

        <div className="mb-2 mt-8 grid gap-4">
          <div className="relative col-span-2 w-full flex-1 overflow-hidden rounded-lg border-2 text-sm md:text-base lg:col-span-3">
            <NpxCreateJazzApp />

            <CopyButton
              code="npx create-jazz-app@latest"
              size="sm"
              className={clsx(
                "z-100 mr-0.5 mt-0.5 hidden md:block md:opacity-100",
              )}
              onCopy={() => track("create-jazz-app command copied from hero")}
            />
          </div>
        </div>

        <p className="text-base text-stone-600 dark:text-stone-400">
          Open source (MIT) and self-hostable.
        </p>

        <SupportedEnvironmentsSection />
      </div>
      <div className="md:col-span-7 md:col-start-6">
        <CodeTabs />
      </div>
    </section>
  );
}
