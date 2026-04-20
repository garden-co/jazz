export type PresentationSlideDefinition = {
  href: string;
  slug: string;
  title: string;
};

const slideTagPattern = /<Slide\b([\s\S]*?)>/g;

export function parsePresentationSlidesFromMdx(
  deckSlug: string,
  rawMdx: string,
): PresentationSlideDefinition[] {
  const slides: PresentationSlideDefinition[] = [];
  const seenSlugs = new Set<string>();

  for (const [index, match] of Array.from(rawMdx.matchAll(slideTagPattern)).entries()) {
    const attributes = match[1] ?? "";
    const slug = readStringAttribute(attributes, "slug");
    const title = readStringAttribute(attributes, "title");

    if (!slug || !title) {
      throw new Error(
        `Slide ${index + 1} in presentation deck "${deckSlug}" must include string "slug" and "title" attributes.`,
      );
    }

    const normalizedSlug = slug.trim();
    const normalizedTitle = title.trim();

    if (normalizedSlug.length === 0 || normalizedTitle.length === 0) {
      throw new Error(
        `Slide ${index + 1} in presentation deck "${deckSlug}" must not use empty "slug" or "title" values.`,
      );
    }

    if (seenSlugs.has(normalizedSlug)) {
      throw new Error(
        `Presentation deck "${deckSlug}" has duplicate slide slug "${normalizedSlug}".`,
      );
    }

    seenSlugs.add(normalizedSlug);

    slides.push({
      href: `/presentations/${deckSlug}/${normalizedSlug}`,
      slug: normalizedSlug,
      title: normalizedTitle,
    });
  }

  if (slides.length === 0) {
    throw new Error(`Presentation deck "${deckSlug}" must define at least one <Slide ...> block.`);
  }

  return slides;
}

function readStringAttribute(attributes: string, name: string) {
  const pattern = new RegExp(`(?:^|\\s)${name}=(["'])([\\s\\S]*?)\\1`);
  const match = attributes.match(pattern);

  return match?.[2];
}
