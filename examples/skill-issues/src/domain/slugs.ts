const SLUG_PATTERN = /^[a-z0-9][a-z0-9_-]*$/;

export function validateSlug(slug: string): string {
  if (!SLUG_PATTERN.test(slug)) {
    throw new Error(`Invalid item slug: ${slug}`);
  }
  return slug;
}
