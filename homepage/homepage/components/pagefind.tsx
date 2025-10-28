"use client";

import { Framework, frameworkNames, frameworks } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import {
  Dialog,
  DialogBody,
} from "@garden-co/design-system/src/components/organisms/Dialog";
import {
  Combobox,
  ComboboxInput,
  ComboboxOption,
  ComboboxOptions,
} from "@headlessui/react";
import { track } from "@vercel/analytics";
import { clsx } from "clsx";
import { usePathname } from "next/navigation";
import React, { useState, useEffect, useRef } from "react";
import { singletonHook } from "react-singleton-hook";

// Types
interface PagefindResult {
  id: string;
  url: string;
  meta: {
    title: string;
    framework?: string;
  };
  excerpt: string;
  sub_results?: Array<PagefindSubResult>;
}

interface PagefindSubResult {
  id: string;
  title: string;
  url: string;
  excerpt: string;
  anchor?: {
    element: string;
  };
}

// Constants
const SEARCH_SHORTCUT_KEY = "k";

// Utility functions
const processUrl = (url: string): string => {
  const urlPath = url
    ?.split("/_next/static/chunks/pages/")?.[1]
    ?.split(".html")?.[0];
  return urlPath?.startsWith("/") ? urlPath : `/${urlPath}`;
};

const processSubUrl = (url: string): { path: string; hash: string } => {
  const [subUrlPath, subUrlHash] =
    url?.split("/_next/static/chunks/pages/")?.[1]?.split(".html") || [];

  const path = subUrlPath?.startsWith("/") ? subUrlPath : `/${subUrlPath}`;
  const hash = subUrlHash ? `${subUrlHash}` : "";

  return { path, hash };
};

const navigateToUrl = (url: string, close: () => void) => {
  if (!url) return;
  window.location.href = `${window.location.origin}${url}`;
  close();
};

const alternativeKeywordsByFramework: Partial<Record<Framework, string[]>> = {
  [Framework.React]: ["reactjs", "react.js", "next.js", "nextjs"],
  [Framework.ReactNative]: ["react native"],
  [Framework.ReactNativeExpo]: ["react native expo", "expo"],
  [Framework.Vanilla]: ["javascript", "js", "plain js", "vanilla js"],
};

const relatedFrameworks: Partial<Record<Framework, Framework[]>> = {
  [Framework.ReactNative]: [Framework.ReactNativeExpo],
  [Framework.ReactNativeExpo]: [Framework.ReactNative],
};

const filterAndPrioritizeResultsByFramework = (
  results: PagefindResult[],
  currentFramework: Framework = Framework.React,
  query: string,
): PagefindResult[] => {
  const frameworksToSearch: Framework[] = [];

  frameworks.forEach((framework) => {
    const alternativeKeywords = alternativeKeywordsByFramework[framework] || [];

    if (
      framework.startsWith(query) ||
      alternativeKeywords.some((keyword: string) => keyword.startsWith(query))
    ) {
      frameworksToSearch.push(framework);
      frameworksToSearch.push(...(relatedFrameworks[framework] || []));
    }
  });

  frameworksToSearch.push(currentFramework);

  const filteredResults = results.filter((result) => {
    const url = processUrl(result.url);
    const fragments = url.split("/").filter(Boolean);
    const frameworkInUrl = fragments[1];

    return fragments.length > 1
      ? frameworksToSearch.includes(frameworkInUrl as Framework)
      : false;
  });

  return prioritizeResultsByFramework(filteredResults, frameworksToSearch[0]);
};

const prioritizeResultsByFramework = (
  results: PagefindResult[],
  framework: Framework,
): PagefindResult[] => {
  return results.sort((a, b) => {
    const aUrl = processUrl(a.url);
    const bUrl = processUrl(b.url);

    const aHasFramework = aUrl.includes(`/${framework}`);
    const bHasFramework = bUrl.includes(`/${framework}`);

    // Prioritize results that match the current framework
    if (aHasFramework && !bHasFramework) return -1;
    if (!aHasFramework && bHasFramework) return 1;

    // Keep original order for results with same framework priority
    return 0;
  });
};

export const usePagefindSearch = singletonHook(
  { open: false, setOpen: () => {} },
  () => {
    const [open, setOpen] = useState(false);
    return { open, setOpen };
  },
);

function HighlightedText({ text }: { text: string }) {
  const decodedText = text.replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  const parts = decodedText.split(/(<mark>.*?<\/mark>)/g);

  return (
    <p className="mt-1 text-sm line-clamp-2">
      {parts.map((part, i) => {
        if (part.startsWith("<mark>")) {
          const content = part.replace(/<\/?mark>/g, "");
          return (
            <mark
              key={i}
              className="bg-transparent text-primary dark:text-white dark:text-underline dark:bg-highlight"
            >
              {content}
            </mark>
          );
        }
        return part;
      })}
    </p>
  );
}

export function PagefindSearch() {
  const { open, setOpen } = usePagefindSearch();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<PagefindResult[]>([]);
  const listRef = useRef<HTMLDivElement>(null);
  const currentFramework = useFramework();
  const pathname = usePathname();

  const close = () => {
    setOpen(false);
    setQuery("");
  };

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === SEARCH_SHORTCUT_KEY && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((open) => !open);
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [setOpen, close]);

  useEffect(() => {
    async function loadPagefind() {
      if (typeof window !== "undefined" && !window.pagefind) {
        try {
          // First try loading from the standard location
          const pagefindModule = await import(
            // @ts-expect-error - pagefind.js is generated after build and not available at compile time
            /* webpackIgnore: true */ "/_pagefind/pagefind.js"
          ).catch(() => {
            // If that fails, try the alternative location
            return import(
              // @ts-expect-error - pagefind.js is generated after build and not available at compile time
              /* webpackIgnore: true */ "/_next/static/chunks/pages/pagefind/pagefind.js"
            );
          });

          window.pagefind = pagefindModule.default || pagefindModule;

          if (window.pagefind && window.pagefind.options) {
            await window.pagefind.options({
              ranking: {
                termFrequency: 0.8,
                pageLength: 0.6,
                termSaturation: 1.2,
              },
            });
          }
        } catch (e) {
          console.warn("Failed to load Pagefind:", e);
          window.pagefind = {
            search: async () => ({ results: [] }),
            options: async () => {},
          };
        }
      }
    }
    loadPagefind();
  }, []);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = 0;
    }
  }, [results]);

  const handleSearch = async (value: string) => {
    setQuery(value);
    if (window.pagefind) {
      const search = await window.pagefind.search(value);
      const results = await Promise.all(
        search.results.map((result: any) => result.data()),
      );

      const filteredResults = filterAndPrioritizeResultsByFramework(
        results,
        currentFramework,
        value,
      );

      setResults(filteredResults);
    }
  };

  if (!open) return null;

  return (
    <Dialog open={open} onClose={close} className="!p-0">
      <DialogBody className="!mt-0">
        <Combobox
          onChange={(result: PagefindResult | PagefindSubResult) => {
            if (result) {
              let url;
              if ("meta" in result) {
                url = processUrl(result.url);
              } else {
                const { path, hash } = processSubUrl(result.url);
                url = `${path}${hash}`;
              }
              navigateToUrl(url, close);
              track("Docs search", {
                query,
                destination: url,
                source: pathname,
              });
            }
          }}
        >
          <div className="p-2 grid grid-cols-1">
            <ComboboxInput
              className={clsx(
                "col-start-1 row-start-1",
                "text-highlight placeholder:text-stone-500 sm:bg-stone-100 sm:dark:bg-stone-925",
                "w-full pl-11 pr-4 py-2.5 rounded-xl sm:rounded-lg",
                "outline-none focus-visible:outline-none",
              )}
              placeholder="Search documentation..."
              onChange={(e) => handleSearch(e.target.value)}
              value={query}
              autoFocus
              autoComplete="off"
              onBlur={() => setQuery("")}
              onKeyDown={(e) => {
                if (e.key === "Escape") {
                  close();
                }
              }}
            />
            <Icon
              name="search"
              size="sm"
              className="col-start-1 row-start-1 ml-3 self-center pointer-events-none text-stone-600"
            />
          </div>
          <div ref={listRef}>
            {results.length > 0 ? (
              <ComboboxOptions className="border-t divide-y max-h-[calc(100vh-84px)] sm:max-h-[600px] overflow-y-auto overflow-x-hidden overscroll-contain">
                {results.map((result) => (
                  <div className="space-y-1 p-2">
                    <ComboboxOption
                      key={result.id}
                      value={result}
                      className="cursor-default flex gap-3 items-center group data-[focus]:bg-stone-100 rounded-lg p-2 dark:data-[focus]:bg-stone-900"
                    >
                      <Icon name="file" className="shrink-0" />
                      <div>
                        <p className="font-medium text-highlight line-clamp-1">
                          {result.meta?.title || "No title"}{" "}
                          {result.meta?.framework ? (
                            <span className="text-stone-600 dark:text-stone-400 font-normal">
                              (
                              {
                                frameworkNames[
                                  result.meta?.framework as Framework
                                ].label
                              }
                              )
                            </span>
                          ) : null}
                        </p>

                        {result.sub_results?.length ? null : (
                          <HighlightedText text={result.excerpt || ""} />
                        )}
                      </div>
                    </ComboboxOption>
                    {result.sub_results?.map((subResult) =>
                      subResult.anchor?.element === "h1" ? null : (
                        <>
                          <ComboboxOption
                            key={subResult.id}
                            value={subResult}
                            className="group cursor-default flex gap-3 items-center group data-[focus]:bg-stone-100 rounded-lg p-2 dark:data-[focus]:bg-stone-900"
                          >
                            <Icon name="hash" className="shrink-0" />
                            <div>
                              <p className="text-sm text-highlight">
                                {subResult.title?.replace("#", "") ||
                                  "No title"}
                              </p>
                              <HighlightedText text={subResult.excerpt || ""} />
                            </div>
                          </ComboboxOption>
                        </>
                      ),
                    )}
                  </div>
                ))}
              </ComboboxOptions>
            ) : null}
          </div>
        </Combobox>
      </DialogBody>
    </Dialog>
  );
}

// Global type augmentation for pagefind
declare global {
  interface Window {
    pagefind?: {
      search: (query: string) => Promise<{
        results: Array<{
          data: () => Promise<PagefindResult>;
        }>;
      }>;
      options?: (config: {
        ranking?: {
          termFrequency?: number;
          pageLength?: number;
          termSaturation?: number;
          termSimilarity?: number;
        };
      }) => Promise<void>;
    };
  }
}
