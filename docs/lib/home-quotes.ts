/**
 * Adopter quotes on the homepage. Only add quotes the person has approved
 * for public use. The section is hidden while this list is empty.
 *
 * `image` is a path under /public: a square avatar (shown round) or a
 * company logo (set `imageKind: "logo"`, shown on a neutral tile).
 */
export type AdopterQuote = {
  quote: string;
  name: string;
  role: string;
  company: string;
  image?: string;
  imageKind?: "avatar" | "logo";
  href?: string;
};

// Empty until real, approved quotes arrive. The docs site ships by promoting a
// preview build, so anything listed here reaches jazz.tools as is.
export const adopterQuotes: AdopterQuote[] = [];
