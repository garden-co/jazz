/**
 * Converts a string (like a coID) to a consistent color with controlled brightness
 * Uses Oklch color model for better perceptual uniformity
 * @param str - The string to convert to a color (typically a coID)
 * @returns An Oklch color string
 */
export const getColor = (str: string): string => {
  // Simple hash function to get a number from a string
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }

  // Convert to a positive number
  hash = Math.abs(hash);

  // Use the hash to determine the hue (0-360)
  // This spreads colors around the entire color wheel
  const hue = hash % 360;

  // Use fixed values for lightness and chroma to ensure consistent brightness
  // Lightness: 0.65 gives good visibility on both light and dark backgrounds
  // Chroma: 0.15 gives vibrant but not overpowering colors
  const lightness = 0.65;
  const chroma = 0.15;

  // Return the color as an Oklch string
  return `oklch(${lightness} ${chroma} ${hue})`;
};
