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
