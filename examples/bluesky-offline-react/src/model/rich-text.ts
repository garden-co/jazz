export type RichTextSegment = {
  text: string;
  uri?: string;
};

type LinkFacet = {
  byteStart: number;
  byteEnd: number;
  uri: string;
};

function parseLinkFacets(facetsJson: string | null | undefined, byteLength: number) {
  if (!facetsJson) return [];

  try {
    const facets = JSON.parse(facetsJson) as LinkFacet[];
    if (!Array.isArray(facets)) return [];

    return facets
      .filter(
        ({ byteStart, byteEnd, uri }) =>
          Number.isInteger(byteStart) &&
          Number.isInteger(byteEnd) &&
          byteStart >= 0 &&
          byteEnd > byteStart &&
          byteEnd <= byteLength &&
          /^https?:\/\//.test(uri),
      )
      .sort((a, b) => a.byteStart - b.byteStart)
      .filter(
        (facet, index, sorted) => index === 0 || facet.byteStart >= sorted[index - 1].byteEnd,
      );
  } catch {
    return [];
  }
}

export function segmentRichText(
  text: string,
  facetsJson: string | null | undefined,
): RichTextSegment[] {
  const bytes = new TextEncoder().encode(text);
  const decoder = new TextDecoder();
  const facets = parseLinkFacets(facetsJson, bytes.length);
  const segments: RichTextSegment[] = [];
  let end = 0;

  for (const facet of facets) {
    if (facet.byteStart > end) {
      segments.push({ text: decoder.decode(bytes.slice(end, facet.byteStart)) });
    }
    segments.push({
      text: decoder.decode(bytes.slice(facet.byteStart, facet.byteEnd)),
      uri: facet.uri,
    });
    end = facet.byteEnd;
  }

  if (end < bytes.length || segments.length === 0) {
    segments.push({ text: decoder.decode(bytes.slice(end)) });
  }

  return segments;
}
