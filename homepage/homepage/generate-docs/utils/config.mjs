import { docNavigationItems } from "../../content/docs/docNavigationItems.js";

// Transform docNavigationItems into the format we need
function transformNavItems() {
  return docNavigationItems
    .map((section) => ({
      title: section.name,
      pages: section.items
        .filter((item) => item.done !== 0) // Skip not-yet-written docs
        .map((item) => ({
          title: item.name,
          url: item.href,
        })),
    }))
    .filter((section) => section.pages.length > 0); // Only include sections with pages
}

export const DOC_SECTIONS = transformNavItems();

export const PACKAGES = [
  {
    packageName: "jazz-tools",
    entryPoint: "exports.ts",
    description:
      "The base implementation for Jazz, a framework for distributed state. Provides a high-level API around the CoJSON protocol.",
  },
  {
    packageName: "jazz-react",
    entryPoint: "index.ts",
    description: "React bindings for Jazz, a framework for distributed state.",
  },
  {
    packageName: "jazz-browser",
    description: "Browser (Vanilla JavaScript) bindings for Jazz",
  },
  {
    packageName: "jazz-browser-media-images",
    description: "Image handling utilities for Jazz in the browser",
  },
  {
    packageName: "jazz-nodejs",
    description: "NodeJS/Bun server worker bindings for Jazz",
  },
];
