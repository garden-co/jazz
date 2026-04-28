import { type InferPageType, loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { toFumadocsSource } from "fumadocs-mdx/runtime/server";
import { presentationDecks } from "../.source/server";
import {
  extractPresentationImageSrcsFromMdx,
  parsePresentationSlidesFromMdx,
  type PresentationSlideDefinition,
} from "./presentation-deck";

export const presentationsSource = loader({
  baseUrl: "/presentations",
  source: toFumadocsSource(presentationDecks, []),
  plugins: [lucideIconsPlugin()],
});

export type PresentationDeckPage = InferPageType<typeof presentationsSource>;
export type PresentationSlide = PresentationSlideDefinition;

type PresentationDeck = {
  description: string | undefined;
  slideCount: number;
  slug: string;
  title: string;
};

function sortDecks(left: PresentationDeck, right: PresentationDeck) {
  return left.title.localeCompare(right.title);
}

function getDeckSlug(page: PresentationDeckPage) {
  const deckSlug = page.slugs[0];

  if (!deckSlug) {
    throw new Error(`Presentation deck at "${page.url}" is missing a deck slug.`);
  }

  return deckSlug;
}

export function getPresentationDeckPage(deck: string) {
  return presentationsSource.getPage([deck]);
}

const presentationSlideCache = new Map<string, Promise<PresentationSlide[]>>();
const presentationImageSrcCache = new Map<string, Promise<string[]>>();

async function getRawDeckSource(page: PresentationDeckPage) {
  return page.data.getText("raw");
}

export async function getPresentationSlidesForPage(
  page: PresentationDeckPage,
): Promise<PresentationSlide[]> {
  const deckSlug = getDeckSlug(page);
  const cached = presentationSlideCache.get(deckSlug);

  if (cached) return cached;

  const parsedSlides = getRawDeckSource(page).then((rawMdx) =>
    parsePresentationSlidesFromMdx(deckSlug, rawMdx),
  );

  presentationSlideCache.set(deckSlug, parsedSlides);

  return parsedSlides;
}

export async function getPresentationImageSrcsForPage(page: PresentationDeckPage) {
  const deckSlug = getDeckSlug(page);
  const cached = presentationImageSrcCache.get(deckSlug);

  if (cached) return cached;

  const imageSrcs = getRawDeckSource(page).then((rawMdx) => extractPresentationImageSrcsFromMdx(rawMdx));

  presentationImageSrcCache.set(deckSlug, imageSrcs);

  return imageSrcs;
}

export async function getPresentationDeckSlides(deck: string) {
  const page = getPresentationDeckPage(deck);

  if (!page) return [];

  return await getPresentationSlidesForPage(page);
}

export async function getPresentationDecks(): Promise<PresentationDeck[]> {
  const decks = await Promise.all(
    presentationsSource.getPages().map(async (page) => {
      const slides = await getPresentationSlidesForPage(page);

      return {
        description: page.data.description,
        slideCount: slides.length,
        slug: getDeckSlug(page),
        title: page.data.title,
      };
    }),
  );

  return decks.sort(sortDecks);
}
