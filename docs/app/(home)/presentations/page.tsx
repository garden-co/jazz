import type { Metadata } from "next";
import Link from "next/link";
import { getPresentationDecks } from "@/lib/presentations";

export const metadata: Metadata = {
  title: "Presentations",
  description: "Slides and talk decks powered by the same MDX pipeline as the docs and blog.",
};

export default function PresentationsIndexPage() {
  const decks = getPresentationDecks();

  return (
    <div className="w-full">
      <section className="w-full pb-24 pt-18 sm:pb-28 sm:pt-22 lg:pb-32 lg:pt-26">
        <div className="mx-auto w-full max-w-(--fd-layout-width) px-4">
          <div className="max-w-[42rem] space-y-4">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Presentations
            </p>
            <h1 className="text-[clamp(3rem,8vw,5.5rem)] font-black leading-[0.9] tracking-[-0.05em]">
              Talks built from the same MDX stack
            </h1>
            <p className="max-w-[38rem] text-lg leading-relaxed text-fd-muted-foreground sm:text-xl">
              Decks live in content files, can share components with the homepage and blog, and run
              in a dedicated full-screen presenter view.
            </p>
          </div>
          <div className="mt-18 grid gap-x-12 gap-y-12 md:grid-cols-2 lg:gap-x-16">
            {decks.map((deck) => (
              <Link
                key={deck.slug}
                href={`/presentations/${deck.slug}`}
                className="group block border-t pt-4"
              >
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
                  {deck.slideCount} slides
                </p>
                <h2 className="mt-3 text-3xl font-black leading-[0.92] tracking-[-0.04em] transition-colors group-hover:text-fd-primary">
                  {deck.title}
                </h2>
                <p className="mt-4 max-w-[34rem] text-base leading-relaxed text-fd-muted-foreground">
                  {deck.description ?? "Open the deck in its full-screen slide view."}
                </p>
              </Link>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
