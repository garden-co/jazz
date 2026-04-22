import {
  Notes,
  PresentationDeckView,
  PresentationNotesProvider,
  Slide,
} from "@/components/presentations/slide";
import { getMDXComponents } from "@/mdx-components";
import {
  getPresentationDeckPage,
  getPresentationDecks,
  getPresentationDeckSlides,
  getPresentationSlide,
  presentationsSource,
} from "@/lib/presentations";
import { resolvePresentationSlideIdentity } from "@/lib/presentation-deck";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import type { ComponentProps } from "react";

export default async function PresentationNotesPage(props: PageProps<"/presenter/[deck]/[slide]">) {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  const slide = await getPresentationSlide(params.deck, params.slide);

  if (!slide) notFound();

  const deckSlides = await getPresentationDeckSlides(params.deck);
  const slideIndex = deckSlides.findIndex((deckSlide) => deckSlide.slug === slide.slug);
  const activeIndex = slideIndex === -1 ? 0 : slideIndex;
  const currentDurationSeconds = deckSlides[activeIndex]?.estimatedDurationSeconds ?? 0;
  const cumulativeDurationSeconds = deckSlides
    .slice(0, activeIndex + 1)
    .reduce((total, deckSlide) => total + deckSlide.estimatedDurationSeconds, 0);
  const MDX = page.data.body;
  let renderedSlideIndex = 0;

  function DeckSlide(props: ComponentProps<typeof Slide>) {
    const identity = resolvePresentationSlideIdentity(deckSlides, renderedSlideIndex, props);
    renderedSlideIndex += 1;

    return <Slide {...props} slug={identity.slug} title={identity.title} />;
  }

  return (
    <PresentationNotesProvider
      cumulativeDurationSeconds={cumulativeDurationSeconds}
      currentDurationSeconds={currentDurationSeconds}
      slideCount={deckSlides.length}
      slideNumber={activeIndex + 1}
      slideTitle={slide.title}
    >
      <PresentationDeckView activeSlide={slide.slug} mode="notes">
        <MDX
          components={getMDXComponents({
            a: createRelativeLink(presentationsSource, { ...page, url: slide.href }),
            Notes,
            Slide: DeckSlide,
          })}
        />
      </PresentationDeckView>
    </PresentationNotesProvider>
  );
}

export async function generateStaticParams() {
  const decks = await getPresentationDecks();

  return (
    await Promise.all(
      decks.map(async (deck) =>
        (await getPresentationDeckSlides(deck.slug)).map((slide) => ({
          deck: deck.slug,
          slide: slide.slug,
        })),
      ),
    )
  ).flat();
}

export async function generateMetadata(
  props: PageProps<"/presenter/[deck]/[slide]">,
): Promise<Metadata> {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  const slide = await getPresentationSlide(params.deck, params.slide);

  if (!slide) notFound();

  return {
    title: `Notes - ${slide.title} - ${page.data.title}`,
    description: `Presenter notes for ${slide.title}`,
  };
}
