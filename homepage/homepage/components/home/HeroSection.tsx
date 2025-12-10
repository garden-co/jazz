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
    <section className="container grid items-start gap-8 pt-12 md:grid-cols-12 md:gap-0 md:pt-20">
      <div className="md:col-span-4">
        <Kicker className="mb-2">The database that syncs.</Kicker>
        <H1 className="text-3xl md:text-4xl lg:text-4xl">
          Things are easy when data syncs anywhere, instantly.
        </H1>
        <Prose
          size="lg"
          className="mt-6 prose-p:leading-normal dark:text-white"
        >
          <p>
            Jazz is a distributed database that runs across your frontend,
            containers, functions, and our global auto-scaling storage cloud.
          </p>
          <p>
            Efficiently sync data, files and LLM streams. Use them like reactive
             local JSON state.
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
