import { docNavigationItems } from "../../content/docs/docNavigationItems.js";

// Recursively collect all pages from an item and its nested items
function collectPages(item) {
  const pages = [];
  
  // If the item has an href and is done, add it as a page
  if (item.href && item.done !== 0) {
    pages.push({
      title: item.name,
      url: item.href,
    });
  }
  
  // If the item has nested items, recursively collect pages from them
  if (item.items && Array.isArray(item.items)) {
    for (const subItem of item.items) {
      pages.push(...collectPages(subItem));
    }
  }
  
  return pages;
}

// Transform docNavigationItems into the format we need
function transformNavItems() {
  return docNavigationItems
    .map((section) => ({
      title: section.name,
      pages: section.items.flatMap(collectPages),
    }))
    .filter((section) => section.pages.length > 0); // Only include sections with pages
}

export const DOC_SECTIONS = transformNavItems();
