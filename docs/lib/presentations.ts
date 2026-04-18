import { type InferPageType, loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { toFumadocsSource } from "fumadocs-mdx/runtime/server";
import { presentationSlides } from "fumadocs-mdx:collections/server";

export const presentationsSource = loader({
  baseUrl: "/presentations",
  source: toFumadocsSource(presentationSlides, []),
  plugins: [lucideIconsPlugin()],
});

export type PresentationSlide = InferPageType<typeof presentationsSource>;

type PresentationDeck = {
  description: string | undefined;
  firstSlideUrl: string;
  slideCount: number;
  slug: string;
  title: string;
};

function sortSlides(left: PresentationSlide, right: PresentationSlide) {
  return left.data.order - right.data.order || left.url.localeCompare(right.url);
}

export function getAllPresentationSlides() {
  return [...presentationsSource.getPages()].sort(sortSlides);
}

export function getPresentationDeckSlides(deck: string) {
  return getAllPresentationSlides().filter((slide) => slide.data.deck === deck);
}

export function getPresentationDecks(): PresentationDeck[] {
  const decks = new Map<string, PresentationDeck>();

  for (const slide of getAllPresentationSlides()) {
    const existing = decks.get(slide.data.deck);

    if (existing) {
      existing.slideCount += 1;
      continue;
    }

    decks.set(slide.data.deck, {
      description: slide.data.description,
      firstSlideUrl: slide.url,
      slideCount: 1,
      slug: slide.data.deck,
      title: slide.data.deckTitle,
    });
  }

  return [...decks.values()].sort((left, right) => left.title.localeCompare(right.title));
}

export function getPresentationNotes(slide: PresentationSlide) {
  const { notes } = slide.data;

  if (!notes) return [];

  return Array.isArray(notes) ? notes : [notes];
}
