"use client";

import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { clsx } from "clsx";
import { useEffect, useState } from "react";

/**
 * Simple HTML to Markdown converter for common elements
 */
function htmlToMarkdown(html: string): string {
  // Create a temporary container to parse HTML
  const tempDiv = document.createElement("div");
  tempDiv.innerHTML = html;

  function convertNode(node: Node): string {
    if (node.nodeType === Node.TEXT_NODE) {
      return node.textContent || "";
    }

    if (node.nodeType !== Node.ELEMENT_NODE) {
      return "";
    }

    const element = node as Element;
    const tagName = element.tagName.toLowerCase();
    const children = Array.from(element.childNodes)
      .map(convertNode)
      .join("")
      .trim();

    switch (tagName) {
      case "h1":
        return `# ${children}\n\n`;
      case "h2":
        return `## ${children}\n\n`;
      case "h3":
        return `### ${children}\n\n`;
      case "h4":
        return `#### ${children}\n\n`;
      case "h5":
        return `##### ${children}\n\n`;
      case "h6":
        return `###### ${children}\n\n`;
      case "p":
        return `${children}\n\n`;
      case "strong":
      case "b":
        return `**${children}**`;
      case "em":
      case "i":
        return `*${children}*`;
      case "code":
        // Check if parent is pre (code block) or inline
        if (element.parentElement?.tagName.toLowerCase() === "pre") {
          return children;
        }
        return `\`${children}\``;
      case "pre":
        const codeElement = element.querySelector("code");
        const language = codeElement?.className
          ?.replace(/language-/, "")
          .replace(/hljs\s+/, "") || "";
        const codeContent = codeElement
          ? Array.from(codeElement.childNodes)
              .map(convertNode)
              .join("")
          : children;
        return `\`\`\`${language}\n${codeContent}\n\`\`\`\n\n`;
      case "ul":
        return `${children}\n`;
      case "ol":
        return `${children}\n`;
      case "li":
        const parent = element.parentElement;
        const isOrdered = parent?.tagName.toLowerCase() === "ol";
        const index = parent
          ? Array.from(parent.children).indexOf(element) + 1
          : 1;
        const prefix = isOrdered ? `${index}. ` : "- ";
        return `${prefix}${children}\n`;
      case "a":
        const href = element.getAttribute("href") || "";
        return `[${children}](${href})`;
      case "blockquote":
        return `> ${children.split("\n").join("\n> ")}\n\n`;
      case "hr":
        return `---\n\n`;
      case "br":
        return "\n";
      case "table":
        const thead = element.querySelector("thead");
        const tbody = element.querySelector("tbody");
        let result = "";
        
        // Process header row
        if (thead) {
          const headerRow = thead.querySelector("tr");
          if (headerRow) {
            const headerCells = Array.from(headerRow.children)
              .map((cell) => {
                const cellContent = Array.from(cell.childNodes)
                  .map(convertNode)
                  .join("")
                  .trim();
                return cellContent;
              })
              .join(" | ");
            result += `| ${headerCells} |\n`;
            // Add separator row
            const cellCount = headerRow.children.length;
            result += `| ${Array(cellCount).fill("---").join(" | ")} |\n`;
          }
        }
        
        // Process body rows
        if (tbody) {
          const rows = tbody.querySelectorAll("tr");
          rows.forEach((row) => {
            const cells = Array.from(row.children)
              .map((cell) => {
                const cellContent = Array.from(cell.childNodes)
                  .map(convertNode)
                  .join("")
                  .trim();
                return cellContent;
              })
              .join(" | ");
            result += `| ${cells} |\n`;
          });
        }
        
        return result + "\n";
      case "thead":
      case "tbody":
        // Handled in table
        return children;
      case "tr":
        // Handled in table/thead/tbody
        return children;
      case "th":
      case "td":
        // Handled in tr
        return children;
      case "img":
        const src = element.getAttribute("src") || "";
        const alt = element.getAttribute("alt") || "";
        return `![${alt}](${src})`;
      default:
        return children;
    }
  }

  return Array.from(tempDiv.childNodes)
    .map(convertNode)
    .join("")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export function CopyAsMarkdownButton() {
  const [copied, setCopied] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000);
      return () => clearTimeout(timeout);
    }
  }, [copied]);

  const handleCopy = async () => {
    setIsLoading(true);
    try {
      // Find the prose content area
      const proseElement = document.querySelector(".prose");
      if (!proseElement) {
        console.error("Could not find prose element");
        return;
      }

      // Clone the element to avoid modifying the original
      const cloned = proseElement.cloneNode(true) as HTMLElement;

      // Remove elements that shouldn't be in markdown
      cloned.querySelectorAll(".not-prose, [data-pagefind-ignore]").forEach((el) => {
        el.remove();
      });

      // Convert HTML to markdown
      const markdown = htmlToMarkdown(cloned.innerHTML);

      // Copy to clipboard
      await navigator.clipboard.writeText(markdown);
      setCopied(true);
    } catch (error) {
      console.error("Failed to copy markdown:", error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      disabled={isLoading}
      className={clsx(
        "flex items-center gap-1.5 text-xs text-stone-600 dark:text-stone-400",
        "hover:text-highlight transition-colors",
        "mb-3 px-2 py-1.5 rounded-md",
        "hover:bg-stone-100 dark:hover:bg-stone-900",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        copied && "text-primary"
      )}
      aria-label="Copy page as Markdown"
    >
      <Icon
        name={copied ? "copySuccess" : "copy"}
        size="xs"
        className={clsx(
          "size-3.5 transition-colors",
          copied ? "stroke-primary" : "stroke-current"
        )}
      />
      <span>{copied ? "Copied!" : "Copy as Markdown"}</span>
    </button>
  );
}
