/**
 * What `s.enum({ ...cases })` throws: enum cases with fields are gone for
 * good (jazz#5758).
 */
export const ENUM_CASES_NOT_SUPPORTED =
  'enum cases with fields are not supported; use s.enum("a", "b") plus a nullable column per field, or s.json() (see https://github.com/garden-co/jazz/issues/5758)';
