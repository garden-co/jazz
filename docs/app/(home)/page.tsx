import { PricingCalculator } from "@/components/home/pricing-calculator";
import { pricingMeters } from "@/lib/home-pricing";

const homepageSections = [
  {
    title: "Local-first data with tunable consistency",
    body: (
      <>
        <p className="max-w-[38rem] text-base">
          Like an embedded database, Jazz brings durable data directly into your frontend and
          backend &mdash; but also automatically syncs it to the cloud.
        </p>
        <p className="max-w-[38rem] text-base">
          This is what makes Jazz feel like magically shared reactive state that completely
          abstracts away networking. Because data is granularly synced on-demand, your app is super
          snappy on first use and only gets faster.
        </p>
        <p className="max-w-[38rem] text-base">
          This is possible because Jazz is eventually-consistent by default. But where
          transactionality matters, you can trade off low-latency and use traditional, globally
          consistent transactions, all in the same database.
        </p>
      </>
    ),
  },
  {
    title: "Row-level security and per-query auth",
    body: (
      <>
        <p className="max-w-[38rem] text-base">
          Row-level security allows you to express permissions in a well-defined and testable way.
          This removes significant complexity and compute effort from your backend and makes
          security a zero-roundtrip
        </p>
        <p className="max-w-[38rem] text-base">
          Jazz modernizes RLS by integrating it deeply with auth (policies over both data and user
          JWT claims) and by optimizing each user query and its applicable policy queries as a unit.
        </p>
      </>
    ),
  },
  {
    title: "Real-time collaboration and deep edit histories",
    body: (
      <>
        <p className="max-w-[38rem] text-base">
          Jazz was conceived in the era of Notion and Figma when real-time collaboration became
          table stakes.
        </p>
        <p className="max-w-[38rem] text-base">
          Things have only gotten faster since then: users collaborate with agents to modify data at
          a much higher rate. At the same time, data versioning and edit histories have become more
          important than ever to reason about data after-the-fact.
        </p>
        <p className="max-w-[38rem] text-base">
          By giving each row a full git-like branching history, Jazz gives you the perfect
          primitives and powerful APIs to work with historical data and complex collaboration
          traces.
        </p>
      </>
    ),
  },
  {
    title: "Fluid schema evolution for fast teams",
    body: (
      <>
        <p className="max-w-[38rem] text-base">
          Instead of traditional stop-the-world migrations that quickly become a bottleneck to
          shipping app updates, Jazz's migrations act as live data compatibility layers that
          translate between different versions of your app.
        </p>
        <p>
          This allows you to iterate on app features at high speed in a full-stack way. It also
          automatically enables backwards-compatiblity for old clients and makes complex apps with
          many feature flags much safer to manage.
        </p>
      </>
    ),
  },
  {
    title: "Replaces 90% of your backend and infra",
    body: (
      <>
        <p className="max-w-[38rem] text-base">
          Jazz does a lot of things to ease the burden of the backend by its design: By abstracting
          away networking, making permissions a database concern, integrating directly with auth and
          offering a collaboration-native data model it standardizes a large portion of
          complications that typically dilutes business logic. This clarity and high level of
          abstraction is crucial in large companies, complex apps and agentically engineered
          codebases.
        </p>
        <p>
          In addition, it takes on data-centric roles that usually require dedicated infrastructure
          components or even vendors: blob storage, file and image CDN, durable streams and
          real-time message queues. This means that you can build complex systems much faster using
          only Jazz - and where integration points are still ecessary, Jazz is the ideal glue
          between other systems.
        </p>
      </>
    ),
  },
] as const;

export default function HomePage() {
  return (
    <div className="w-full">
      <section className="h-[80vh] w-full">
        <div className="mx-auto flex h-full w-full max-w-(--fd-layout-width) items-end px-4">
          <div className="w-full max-w-[42rem] space-y-6 pb-2 sm:space-y-10">
            <h1 className="w-full text-[clamp(4rem,11vw,10rem)] font-black leading-[0.84] tracking-[-0.05em]">
              <span className="block">the</span>
              <span className="block -ml-[0.04em]">database</span>
              <span className="block">that syncs</span>
            </h1>
            <p className="max-w-[40em] text-xl leading-relaxed">
              Jazz is a local-first relational database. It runs across your frontend, backend and
              our global storage cloud. Sync partial tables, durable streams and files, fast. Feels
              like simple reactive state.
            </p>
          </div>
        </div>
      </section>
      <section className="w-full pb-24 pt-20 sm:pb-28 sm:pt-24 lg:pb-32 lg:pt-28">
        <div className="mx-auto grid w-full max-w-(--fd-layout-width) gap-x-12 gap-y-14 px-4 md:grid-cols-2 lg:gap-x-16 lg:gap-y-18">
          {homepageSections.map((section) => (
            <div key={section.title} className="max-w-[34rem] space-y-4">
              <h2 className="text-3xl font-black leading-[0.9] tracking-[-0.04em] sm:text-[2.6rem] text-balance">
                {section.title}
              </h2>
              {section.body}
            </div>
          ))}
        </div>
      </section>
      <section className="w-full pb-28 pt-10 sm:pb-32 sm:pt-12 lg:pb-40 lg:pt-16">
        <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
          <div className="grid gap-14 lg:grid-cols-[minmax(0,0.86fr)_minmax(0,1.14fr)] lg:items-end">
            <div className="max-w-[34rem] space-y-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Jazz Cloud
              </p>
              <h2 className="text-3xl font-black leading-[0.9] tracking-[-0.04em] sm:text-[2.6rem] text-balance">
                A globally synced, auto-scaling database cloud
              </h2>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                The single-tenant Jazz database server will always be open-source and is very easy
                to self-host, but you'll have an even better experience with Jazz Cloud.
              </p>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                Jazz Cloud is a globally distributed, fault-tolerant and geo-optimized
                infrastructure tailored for Jazz.
              </p>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                It's zero-config to set up, gives you a "it just works" experience from your first
                experiments and scales automatically in ways that even most other hosted databases
                can't offer.
              </p>
            </div>
          </div>
        </div>
      </section>
      <section className="w-full pb-28 pt-10 sm:pb-32 sm:pt-12 lg:pb-40 lg:pt-16">
        <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
          <div className="grid gap-14 lg:grid-cols-[minmax(0,0.86fr)_minmax(0,1.14fr)] lg:items-end">
            <div className="max-w-[34rem] space-y-4">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                Usage-based pricing
              </p>
              <h2 className="text-3xl font-black leading-[0.9] tracking-[-0.04em] sm:text-[2.6rem] text-balance">
                Simple billing that scales to zero
              </h2>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                Because Jazz is incredibly flexible and supports a wide range of different apps,
                it's important that its pricing is just as flexible.
              </p>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                The idea: we bill for the things that are irreducibly-hard, making no assumptions
                about your app or your users.
              </p>
              <p className="max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                You benefit from global infrastructure, our operational experience and pricing that
                is only possible at scale, while being billed in predictable, scale-to-zero units.
              </p>
            </div>
            <div className="grid gap-x-8 gap-y-10 sm:grid-cols-3">
              {pricingMeters.map((meter) => (
                <div key={meter.name} className="border-t pt-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                    {meter.name}
                  </p>
                  <p className="mt-2 text-4xl font-black tracking-[-0.06em]">{meter.price}</p>
                  <p className="mt-1 text-sm font-medium">{meter.unit}</p>
                  <p className="mt-3 text-sm leading-relaxed text-fd-muted-foreground">
                    {meter.note}
                  </p>
                </div>
              ))}
            </div>
          </div>
          <div className="mt-20 border-t pt-12 sm:mt-24 sm:pt-14">
            <PricingCalculator />
          </div>
        </div>
      </section>
    </div>
  );
}
