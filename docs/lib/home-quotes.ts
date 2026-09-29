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

// PLACEHOLDERS: real, approved quotes replace these before this ships.
export const adopterQuotes: AdopterQuote[] = [
  {
    quote:
      "Placeholder for the featured quote. One or two sentences on what changed for this team after moving to Jazz, ideally with a concrete before and after.",
    name: "Adopter name",
    role: "Role",
    company: "Company",
  },
  {
    quote:
      "Placeholder quote about shipping faster: which parts of the backend they no longer had to build.",
    name: "Adopter name",
    role: "Role",
    company: "Company",
    imageKind: "logo",
  },
  {
    quote:
      "Placeholder quote about the local-first experience their users noticed, such as instant loads or offline edits.",
    name: "Adopter name",
    role: "Role",
    company: "Company",
  },
  {
    quote: "Placeholder quote about permissions or sync correctness, in the adopter's own words.",
    name: "Adopter name",
    role: "Role",
    company: "Company",
    imageKind: "logo",
  },
  {
    quote: "Placeholder quote about working with the Jazz team or the community.",
    name: "Adopter name",
    role: "Role",
    company: "Company",
  },
];
