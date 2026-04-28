export type PresentationSlideDefinition = {
  estimatedDurationSeconds: number;
  href: string;
  notesText: string;
  notesHref: string;
  slug: string;
  title: string;
};

export type PresentationSlideIdentity = {
  slug?: string;
  title?: string;
};

const notesTagPattern = /<Notes\b[^>]*>([\s\S]*?)<\/Notes>/g;
const slideTagPattern = /<Slide\b([\s\S]*?)>([\s\S]*?)<\/Slide>/g;
const imageTagPattern = /<img\b[\s\S]*?\bsrc=(["'])([\s\S]*?)\1[\s\S]*?\/?>/g;
const slideHashPrefix = "slide=";
const letterCanvasArrowMessageType = "jazz-letter-canvas:arrow-key";

export function createPresentationSlideHref(basePath: string, slideSlug: string) {
  return `${basePath}#${slideHashPrefix}${encodeURIComponent(slideSlug)}`;
}

export function readPresentationSlideSlugFromHash(hash: string) {
  const fragment = hash.startsWith("#") ? hash.slice(1) : hash;

  if (!fragment) return null;

  const value = fragment.startsWith(slideHashPrefix)
    ? fragment.slice(slideHashPrefix.length)
    : fragment;

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function readLetterCanvasArrowNavigationDirection(message: unknown) {
  if (!message || typeof message !== "object") return null;

  const data = message as { key?: unknown; type?: unknown };

  if (data.type !== letterCanvasArrowMessageType) return null;

  if (data.key === "ArrowRight") return "next";
  if (data.key === "ArrowLeft") return "previous";

  return null;
}

export function extractPresentationImageSrcsFromMdx(rawMdx: string) {
  const seenSrcs = new Set<string>();
  const imageSrcs: string[] = [];

  for (const match of rawMdx.matchAll(imageTagPattern)) {
    const src = match[2]?.trim();

    if (!src || seenSrcs.has(src)) continue;

    seenSrcs.add(src);
    imageSrcs.push(src);
  }

  return imageSrcs;
}

export function parsePresentationSlidesFromMdx(
  deckSlug: string,
  rawMdx: string,
): PresentationSlideDefinition[] {
  const slides: PresentationSlideDefinition[] = [];
  const seenSlugs = new Set<string>();

  for (const [index, match] of Array.from(rawMdx.matchAll(slideTagPattern)).entries()) {
    const attributes = match[1] ?? "";
    const body = match[2] ?? "";
    const fallbackValue = String(index + 1);
    const normalizedSlug = readStringAttribute(attributes, "slug")?.trim() || fallbackValue;
    const normalizedTitle = readStringAttribute(attributes, "title")?.trim() || fallbackValue;
    const notesText = extractNotesText(body);

    if (seenSlugs.has(normalizedSlug)) {
      throw new Error(
        `Presentation deck "${deckSlug}" has duplicate slide slug "${normalizedSlug}".`,
      );
    }

    seenSlugs.add(normalizedSlug);

    slides.push({
      estimatedDurationSeconds: estimatePresentationSpeakingDurationSeconds(notesText),
      href: createPresentationSlideHref(`/presentations/${deckSlug}`, normalizedSlug),
      notesText,
      notesHref: createPresentationSlideHref(`/presenter/${deckSlug}`, normalizedSlug),
      slug: normalizedSlug,
      title: normalizedTitle,
    });
  }

  if (slides.length === 0) {
    throw new Error(`Presentation deck "${deckSlug}" must define at least one <Slide ...> block.`);
  }

  return slides;
}

export function estimatePresentationSpeakingDurationSeconds(notesText: string) {
  const normalized = notesText.trim();

  if (normalized.length === 0) return 0;

  const words = normalized.split(/\s+/).filter(Boolean).length;
  const paragraphBreaks = Math.max(0, normalized.split(/\n{2,}/).length - 1);
  const rawSeconds = (words / 160) * 60;

  return Math.max(4, Math.round(rawSeconds)) + paragraphBreaks * 2;
}

export function resolvePresentationSlideIdentity(
  slides: PresentationSlideDefinition[],
  slideIndex: number,
  identity: PresentationSlideIdentity,
) {
  const fallback = slides[slideIndex];

  if (!fallback) {
    throw new Error(`No parsed slide definition exists at index ${slideIndex + 1}.`);
  }

  return {
    slug: identity.slug?.trim() || fallback.slug,
    title: identity.title?.trim() || fallback.title,
  };
}

function readStringAttribute(attributes: string, name: string) {
  const pattern = new RegExp(`(?:^|\\s)${name}=(["'])([\\s\\S]*?)\\1`);
  const match = attributes.match(pattern);

  return match?.[2];
}

function extractNotesText(slideBody: string) {
  const notesBlocks = Array.from(slideBody.matchAll(notesTagPattern)).map((match) =>
    normalizeNotesText(match[1] ?? ""),
  );

  return notesBlocks.filter(Boolean).join("\n\n");
}

function normalizeNotesText(notesBody: string) {
  return notesBody
    .replace(/<[^>]+>/g, " ")
    .replace(/\[([^\]]+)]\(([^)]+)\)/g, "$1")
    .replace(/[*_`~>#-]+/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}
