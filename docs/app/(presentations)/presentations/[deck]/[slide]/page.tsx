import { Notes, PresentationDeckView, Slide } from "@/components/presentations/slide";
import { getMDXComponents } from "@/mdx-components";
import {
  getPresentationDeckPage,
  getPresentationDecks,
  getPresentationDeckSlides,
  getPresentationSlide,
  presentationsSource,
} from "@/lib/presentations";
import { createRelativeLink } from "fumadocs-ui/mdx";
import type { Metadata } from "next";
import { notFound } from "next/navigation";

export default async function PresentationSlidePage(
  props: PageProps<"/presentations/[deck]/[slide]">,
) {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  const slide = await getPresentationSlide(params.deck, params.slide);

  if (!slide) notFound();

  const MDX = page.data.body;

  return (
    <PresentationDeckView activeSlide={slide.slug}>
      <MDX
        components={getMDXComponents({
          a: createRelativeLink(presentationsSource, { ...page, url: slide.href }),
          Notes,
          Slide,
        })}
      />
    </PresentationDeckView>
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
  props: PageProps<"/presentations/[deck]/[slide]">,
): Promise<Metadata> {
  const params = await props.params;
  const page = getPresentationDeckPage(params.deck);

  if (!page) notFound();

  const slide = await getPresentationSlide(params.deck, params.slide);

  if (!slide) notFound();

  return {
    title: `${slide.title} - ${page.data.title}`,
    description: page.data.description,
  };
}
