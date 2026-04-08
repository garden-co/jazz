const homepageSections = [
  {
    title: "Conceived as a whole from the metal to the screen",
    body: "By building a completely new database engine, sync protocol and ... Placeholder",
  },
  {
    title: "Local-first and tunable consistency",
    body: "Placeholder",
  },
  {
    title: "Auth, permissions and collaboration baked in",
    body: "We made auth and permissions a database concern... Placeholder",
  },
  {
    title: "Made for teams that ship quickly",
    body: "If you're a 10x engineer, command a horde of agents or run a complex app with a thousand feature flags, schema evolution becomes your main bottleneck. With fluid migrations... Placeholder",
  },
  {
    title: "Batteries, bells & whistles included",
    body: "Serves images, calls webhooks, ... Placeholder",
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
              Jazz is a local-first relational database. It runs across your frontend,
              backend/functions, and our global storage cloud. Sync partial tables, files and
              durable streams at high tempo. Feels like simple reactive state.
            </p>
          </div>
        </div>
      </section>
      <section className="w-full pb-24 pt-20 sm:pb-28 sm:pt-24 lg:pb-32 lg:pt-28">
        <div className="mx-auto grid w-full max-w-(--fd-layout-width) gap-x-12 gap-y-14 px-4 md:grid-cols-2 lg:gap-x-16 lg:gap-y-18">
          {homepageSections.map((section) => (
            <div key={section.title} className="max-w-[34rem] space-y-4">
              <h2 className="text-3xl font-black leading-[0.9] tracking-[-0.04em] sm:text-[2.6rem]">
                {section.title}
              </h2>
              <p className="max-w-[38rem] text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
                {section.body}
              </p>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
