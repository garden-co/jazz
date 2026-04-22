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
      href: `/presentations/${deckSlug}/${normalizedSlug}`,
      notesText,
      notesHref: `/presenter/${deckSlug}/${normalizedSlug}`,
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
  const rawSeconds = (words / 130) * 60;

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
