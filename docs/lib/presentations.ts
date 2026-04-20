import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { type InferPageType, loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";
import { toFumadocsSource } from "fumadocs-mdx/runtime/server";
import { presentationDecks } from "../.source/server";
import {
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
  firstSlideUrl: string;
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
const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function getRawDeckSource(page: PresentationDeckPage) {
  const relativePath = (
    page.data as PresentationDeckPage["data"] & {
      info?: {
        fullPath?: string;
      };
    }
  ).info?.fullPath;

  if (typeof relativePath !== "string") {
    throw new Error(`Presentation deck "${getDeckSlug(page)}" is missing raw source access.`);
  }

  return readFile(path.resolve(docsRoot, relativePath), "utf8");
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
        firstSlideUrl: slides[0].href,
        slideCount: slides.length,
        slug: getDeckSlug(page),
        title: page.data.title,
      };
    }),
  );

  return decks.sort(sortDecks);
}

export async function getPresentationSlide(deck: string, slideSlug: string) {
  const slides = await getPresentationDeckSlides(deck);

  return slides.find((slide) => slide.slug === slideSlug);
}
