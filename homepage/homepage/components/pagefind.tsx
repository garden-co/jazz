"use client";

import { Framework, frameworks } from "@/content/framework";
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

const navigateToUrl = (url: string, setOpen?: (open: boolean) => void) => {
  if (!url) return;
  window.location.href = `${window.location.origin}${url}`;
  // setOpen(false);
};

const alternativeKeywordsByFramework: Partial<Record<Framework, string[]>> = {
  [Framework.React]: ["reactjs", "react.js", "next.js", "nextjs"],
  [Framework.Vue]: ["vuejs", "vue.js"],
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

// Hooks
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
              className="px-0.5 bg-transparent text-primary group-data-[focus]:text-underline"
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

function Result({
  result,
  onClick,
}: {
  result: PagefindResult | PagefindSubResult;
  onClick: () => void;
}) {
  const title = "meta" in result ? result.meta?.title : result.title;
  const subResults = "sub_results" in result ? result.sub_results : [];
  const framework = "meta" in result ? (result.meta as any)?.framework : null;
  return (
    <>
      <button
        type="button"
        onClick={onClick}
        className={`text-left group relative flex items-center gap-2 sm:gap-3 px-3 sm:px-4 py-2 sm:py-3 cursor-pointer text-sm rounded-md select-none
          transition-all duration-200 ease-in-out
          `}
      >
        <div className="min-w-0 flex-1">
          <h3 className="font-medium text-highlight truncate">
            {title || "No title"} {framework ? `(${framework})` : ""}
          </h3>
          <HighlightedText text={result.excerpt || ""} />
        </div>
      </button>

      {subResults?.length ? (
        <ul className="ml-4">
          {subResults?.map((subResult) => (
            <li key={subResult.id} className="block">
              <Result
                result={subResult}
                onClick={() => {
                  const { path, hash } = processSubUrl(subResult.url);
                  navigateToUrl(`${path}${hash}`);
                }}
              />
            </li>
          ))}
        </ul>
      ) : null}
    </>
  );
}

export function PagefindSearch() {
  const { open, setOpen } = usePagefindSearch();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<PagefindResult[]>([]);
  const listRef = useRef<HTMLDivElement>(null);
  const currentFramework = useFramework();

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === SEARCH_SHORTCUT_KEY && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((open) => !open);
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [setOpen]);

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

      console.log(filteredResults);

      setResults(filteredResults);
    }
  };

  if (!open) return null;

  return (
    <Dialog open={open} onClose={() => setOpen(false)} className="!p-0">
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
              navigateToUrl(url, setOpen);
            }
          }}
        >
          <div className="p-2 border-b">
            <ComboboxInput
              className="w-full rounded-md bg-stone-100 px-4 py-2.5 text-highlight outline-none placeholder:text-stone-500"
              placeholder="Search documentation..."
              onChange={(e) => handleSearch(e.target.value)}
              value={query}
              autoFocus
              autoComplete="off"
            />
          </div>
          <div
            ref={listRef}
            className="h-[50vh] sm:h-[300px] max-h-[60vh] sm:max-h-[400px] overflow-y-auto overflow-x-hidden overscroll-contain"
          >
            {results.length === 0 ? (
              <p className="text-center py-5">No results found.</p>
            ) : (
              <ComboboxOptions>
                {results.map((result) => (
                  <div className="border-b space-y-1 p-2">
                    <ComboboxOption
                      key={result.id}
                      value={result}
                      className="cursor-default flex gap-3 items-center group data-[focus]:bg-stone-100 rounded-lg p-2"
                    >
                      <Icon name="file" className="shrink-0" />
                      <p className="font-medium text-highlight line-clamp-1">
                        {result?.meta?.title || "No title"}
                      </p>
                    </ComboboxOption>
                    {result.sub_results?.map((subResult) =>
                      subResult.anchor?.element === "h1" ? null : (
                        <>
                          <ComboboxOption
                            key={subResult.id}
                            value={subResult}
                            className="group cursor-default flex gap-3 items-center group data-[focus]:bg-stone-100 rounded-lg p-2"
                          >
                            <Icon name="hash" className="shrink-0" />
                            <div>
                              <p className="text-sm text-highlight">
                                {subResult?.title?.replace("#", "") ||
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
            )}
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
