import { Notes, PresentationDeckView, Slide } from "@/components/presentations/slide";
import { getMDXComponents } from "@/mdx-components";
import {
  getPresentationDeckPage,
  getPresentationDecks,
  getPresentationDeckSlides,
  presentationsSource,
} from "@/lib/presentations";
import { resolvePresentationSlideIdentity } from "@/lib/presentation-deck";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import type { ComponentProps } from "react";

export default async function PresentationDeckPage(props: PageProps<"/presentations/[deck]">) {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  const deckSlides = await getPresentationDeckSlides(params.deck);
  const MDX = page.data.body;
  let renderedSlideIndex = 0;

  function DeckSlide(props: ComponentProps<typeof Slide>) {
    const identity = resolvePresentationSlideIdentity(deckSlides, renderedSlideIndex, props);
    renderedSlideIndex += 1;

    return <Slide {...props} slug={identity.slug} title={identity.title} />;
  }

  return (
    <PresentationDeckView slides={deckSlides}>
      <MDX
        components={getMDXComponents({
          a: createRelativeLink(presentationsSource, page),
          Notes,
          Slide: DeckSlide,
        })}
      />
    </PresentationDeckView>
  );
}

export async function generateStaticParams() {
  const decks = await getPresentationDecks();

  return decks.map((deck) => ({ deck: deck.slug }));
}

export async function generateMetadata(
  props: PageProps<"/presentations/[deck]">,
): Promise<Metadata> {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  return {
    title: page.data.title,
    description: page.data.description,
  };
}
