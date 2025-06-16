// @ts-check

import GithubSlugger from 'github-slugger';

const slugs = new GithubSlugger();
const FRAMEWORK_PATTERN = /\s*\[\!framework=([a-zA-Z0-9,_-]+)\]\s*$/;

/**
 * Extracts framework markers from a header text
 * @param {string} text - The header text to extract framework markers from
 * @returns {{ text: string; frameworks: string[] | null }} An object containing the extracted text and frameworks
 */
export function extractFrameworkMarkers(text) {
  const match = text.match(FRAMEWORK_PATTERN);
  if (!match) return { text, frameworks: null };
  
  return {
    text: text.replace(FRAMEWORK_PATTERN, ''),
    frameworks: match[1].split(',')
  };
}

/**
 * Generates a URL-friendly ID for a header, handling:
 * - Code blocks (text within backticks)
 * - Framework visibility markers ([!framework=...])
 * - Special characters and spaces
 * @param {string} headerText - The header text to generate an ID for
 * @returns {string} The generated header ID
*/
export function generateHeaderId(headerText) {
  slugs.reset();
  
  // Remove framework visibility markers
  const { text } = extractFrameworkMarkers(headerText);
  
  // Generate the slug
  return slugs.slug(text);
}
