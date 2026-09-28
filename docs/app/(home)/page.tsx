import type { Metadata } from "next";
import { Card } from "@astryxdesign/core/Card";
import { Heading } from "@astryxdesign/core/Heading";
import { Link as AstryxLink } from "@astryxdesign/core/Link";
import { Text } from "@astryxdesign/core/Text";
import { Eyebrow } from "@garden-co/design/react";
import { AppLink } from "@/components/design/app-link";
import { JazzTheme } from "@/components/design/jazz-theme";
import { PricingCalculator } from "@/components/home/pricing-calculator";
import { pricingMeters } from "@/lib/home-pricing";

export const metadata: Metadata = {
  title: "Jazz - The database that syncs.",
};

const homepageSections = [
  {
    title: "Local-first data with tunable consistency",
    body: (
      <>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Like an embedded database, Jazz brings durable data directly into your frontend and
          backend &mdash; but also automatically syncs it to the cloud.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          This is what makes Jazz feel like magically shared reactive state that abstracts away
          networking. Because data is granularly synced on-demand, your app is fast on first use and
          instant afterwards.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          This is possible because Jazz is eventually-consistent by default. But where
          transactionality matters, you can trade off low-latency and use traditional, globally
          consistent transactions, all in the same database.
        </Text>
      </>
    ),
  },
  {
    title: "Row-level security and per-query auth",
    body: (
      <>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Row-level security allows you to express permissions in a well-defined and testable way.
          This removes significant complexity and compute effort from your backend and gives you
          zero-roundtrip security.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Jazz modernizes RLS by integrating it deeply with auth (policies over both data and user
          JWT claims) and by optimizing each user query and its applicable policy queries as a unit.
        </Text>
      </>
    ),
  },
  {
    title: "Real-time collaboration and deep edit histories",
    body: (
      <>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Jazz was conceived in the era of Notion and Figma when real-time collaboration became
          table stakes.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Things have only gotten faster since then: users collaborate with agents to modify data at
          a much higher rate. At the same time, data versioning and edit histories have become more
          important than ever to reason about data after-the-fact.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          By giving each row a full git-like branching history, Jazz gives you powerful APIs to work
          with historical data and complex collaboration traces.
        </Text>
      </>
    ),
  },
  {
    title: "Fluid schema evolution for fast teams",
    body: (
      <>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Instead of traditional stop-the-world migrations that quickly become a bottleneck to
          shipping app updates, Jazz's migrations act as live data compatibility layers that
          translate between different versions of your app.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          This allows you to iterate on app features at high speed in a full-stack way. It also
          automatically enables backwards-compatibility for old clients and makes complex apps with
          many feature flags much safer to manage.
        </Text>
      </>
    ),
  },
  {
    title: "Slims your backend and simplifies your infra",
    body: (
      <>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          Jazz does a lot of things to ease the burden of the backend by its design: By abstracting
          away networking, making permissions a database concern, integrating directly with auth and
          offering a collaboration-native data model it standardizes a large portion of
          complications that typically dilutes business logic. This clarity and high level of
          abstraction is crucial in large companies, complex apps and agentically engineered
          codebases.
        </Text>
        <Text as="p" display="block" className="max-w-[38rem] text-base">
          In addition, it takes on data-centric roles that usually require dedicated infrastructure
          components or even vendors: blob storage, file and image CDN, durable streams and
          real-time message queues. This means that you can build complex systems much faster using
          only Jazz - and where integration points are still necessary, Jazz is great glue between
          other systems.
        </Text>
      </>
    ),
  },
] as const;

export default function HomePage() {
  return (
    <JazzTheme>
      <div className="w-full">
        <section className="h-[80vh] w-full">
          <div className="mx-auto flex h-full w-full max-w-(--fd-layout-width) items-end px-4 relative">
            <aside className="absolute right-4 top-4 z-30 max-w-sm">
              <Card className="border-fd-border/70 shadow dark:border-white/50">
                <Text as="p" display="block" className="leading-relaxed">
                  Announcing the Jazz v2 alpha!
                </Text>
                <Text as="p" display="block" className="leading-relaxed">
                  See the{" "}
                  <AppLink href="/blog/what-is-jazz" color="inherit" className="font-medium">
                    announcement post
                  </AppLink>
                  .
                </Text>
                <Text as="p" display="block" color="secondary" className="leading-relaxed">
                  (Looking for{" "}
                  <AstryxLink href="https://classic.jazz.tools" color="inherit">
                    classic Jazz
                  </AstryxLink>
                  ?)
                </Text>
              </Card>
            </aside>
            <div className="w-full max-w-[42rem] space-y-6 pb-2 sm:space-y-10">
              <Heading level={1} type="display-1">
                <span className="block">the</span>
                <span className="block -ml-[0.04em]">database</span>
                <span className="block">that syncs</span>
              </Heading>
              <Text as="p" display="block" className="max-w-[40em] text-xl leading-relaxed">
                Jazz is a local-first relational database. It runs across your frontend, backend and
                our global storage cloud. Sync partial tables, durable streams and files, fast.
                Feels like simple reactive state.
              </Text>
            </div>
          </div>
        </section>
        <section className="w-full pb-24 pt-20 sm:pb-28 sm:pt-24 lg:pb-32 lg:pt-28">
          <div className="mx-auto grid w-full max-w-(--fd-layout-width) gap-x-12 gap-y-14 px-4 md:grid-cols-2 lg:gap-x-16 lg:gap-y-18">
            {homepageSections.map((section) => (
              <div key={section.title} className="max-w-[34rem] space-y-4">
                <Heading level={2} className="text-3xl leading-[0.9] sm:text-[2.6rem]">
                  {section.title}
                </Heading>
                {section.body}
              </div>
            ))}
          </div>
        </section>
        <section className="w-full pb-28 pt-10 sm:pb-32 sm:pt-12 lg:pb-40 lg:pt-16">
          <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
            <div className="grid gap-14 lg:grid-cols-[minmax(0,0.86fr)_minmax(0,1.14fr)] lg:items-end">
              <div className="max-w-[34rem] space-y-4">
                <Eyebrow>Jazz Cloud</Eyebrow>
                <Heading level={2} className="text-3xl leading-[0.9] sm:text-[2.6rem]">
                  A globally synced, auto-scaling database cloud
                </Heading>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  The single-tenant Jazz database server will always be open-source and is very easy
                  to self-host, but you'll have an even better experience with Jazz Cloud.
                </Text>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  Jazz Cloud is a globally distributed, fault-tolerant and geo-optimized
                  infrastructure tailored for Jazz.
                </Text>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  It's zero-config to set up, gives you a "it just works" experience from your first
                  experiments and scales much more granularly than traditional instance-based
                  databases.
                </Text>
              </div>
            </div>
          </div>
        </section>
        <section className="w-full pb-28 pt-10 sm:pb-32 sm:pt-12 lg:pb-40 lg:pt-16">
          <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
            <div className="grid gap-14 lg:grid-cols-[minmax(0,0.86fr)_minmax(0,1.14fr)]">
              <div className="max-w-[34rem] space-y-4">
                <Eyebrow>Usage-based pricing</Eyebrow>
                <Heading level={2} className="text-3xl leading-[0.9] sm:text-[2.6rem]">
                  Simple billing
                  <br />
                  that scales to zero
                </Heading>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  Because Jazz is incredibly flexible and supports a wide range of different apps,
                  it's important that its pricing is just as flexible.
                </Text>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  The idea: we bill for the things that are irreducibly-hard, making no assumptions
                  about your app or your users.
                </Text>
                <Text
                  as="p"
                  display="block"
                  color="secondary"
                  className="max-w-[34rem] text-base leading-relaxed sm:text-lg"
                >
                  You benefit from global infrastructure with multi-region edges, our operational
                  experience and pricing that is only possible at scale, while being billed in
                  predictable, scale-to-zero units.
                </Text>
              </div>
              <div className="grid gap-x-8 gap-y-10 sm:grid-cols-3">
                {pricingMeters.map((meter) => (
                  <div key={meter.name} className="border-t pt-4">
                    <Eyebrow>{meter.name}</Eyebrow>
                    <Heading level={3} type="display-3" className="mt-2">
                      {meter.price}
                    </Heading>
                    <Text as="p" display="block" weight="medium" className="mt-1 text-sm">
                      {meter.unit}
                    </Text>
                    <Text
                      as="p"
                      display="block"
                      color="secondary"
                      className="mt-3 text-sm leading-relaxed"
                    >
                      {meter.note}
                    </Text>
                    <Text
                      as="p"
                      display="block"
                      color="secondary"
                      className="mt-3 text-sm leading-relaxed"
                    >
                      {meter.included}
                    </Text>
                  </div>
                ))}
              </div>
              {/* Not ported to Astryx yet. A data-astryx-theme attribute ends the
                  theme's @scope, so its element resets (p, h1-h6, code) leave the
                  calculator's Tailwind typography alone. */}
              <div className="sm:col-start-2" data-astryx-theme="none">
                <PricingCalculator />
              </div>
            </div>
            <div className="mt-20 border-t pt-12 sm:mt-24 sm:pt-14"></div>
          </div>
        </section>
        <footer className="w-full pb-24 pt-4 sm:pb-28 lg:pb-32">
          <div className="mx-auto flex w-full max-w-(--fd-layout-width) flex-col items-center gap-6 px-4">
            <Heading level={2} type="display-2">
              npm create jazz
            </Heading>
            <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center sm:gap-4">
              <Text as="p" display="block" color="secondary" className="text-sm sm:text-base">
                Join the
              </Text>
              <a
                href="https://discord.gg/RN9UKh52be"
                className="inline-flex items-center rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-accent hover:text-fd-accent-foreground"
                target="_blank"
                rel="noreferrer"
              >
                Jazz Discord
              </a>
            </div>
          </div>
        </footer>
      </div>
    </JazzTheme>
  );
}
