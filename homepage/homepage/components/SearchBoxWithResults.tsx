"use client";

import { Framework, frameworkNames, frameworks } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { Input } from "quint-ui";
import React, { useState, useEffect } from "react";
import Pagination from "./Pagination";

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

// Utility functions
const processUrl = (url: string): string => {
  const urlPath = url
    ?.split("/_next/static/chunks/pages/")?.[1]
    ?.split(".html")?.[0];
  return urlPath?.startsWith("/") ? urlPath : `/${urlPath}`;
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

export function SearchBoxWithResults({ searchTerms }: { searchTerms: string }) {
  const PAGE_LENGTH = 5;
  const [query, setQuery] = useState<string>(searchTerms ?? "");
  const [page, setPage] = useState(0);
  const [loading, setLoading] = useState(true);
  const [results, setResults] = useState<PagefindResult[]>([]);
  const currentFramework = useFramework();

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
            options: async () => { },
          };
        }
      }
    }
    loadPagefind().then(async () => {
      await handleSearch(searchTerms);
      setLoading(false);
    });
  }, [searchTerms]);

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

  if (loading) return null;
  return (
    <>
      <Input value={query} onChange={(e) => handleSearch(e.target.value)} placeholder="Search" id="search-in-searchbox" />

      <div className="mt-4">
        {results.length > 0 ? (
          <>
            <small>Page {page + 1} of {Math.ceil(results.length / PAGE_LENGTH)}</small>
            <ul>
              {results.slice((page) * PAGE_LENGTH, (page + 1) * PAGE_LENGTH).map((result) => (
                <li className="cursor-default flex flex-col group data-[focus]:bg-stone-100 dark:data-[focus]:bg-stone-900" key={result.id}>
                  <div>
                    <p className="font-bold text-highlight line-clamp-1 border-b">
                      <a href={processUrl(result.url)}>
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
                      </a>
                    </p>


                    {result.sub_results?.length ? null : (
                      <HighlightedText text={result.excerpt || ""} />
                    )}
                  </div>
                  <ul>
                    {result.sub_results?.map((subResult) =>
                      subResult.anchor?.element === "h1" ? null : (
                        <li
                          key={subResult.id}
                          className="group cursor-pointer group data-[focus]:bg-stone-100 rounded-lg p-2 dark:data-[focus]:bg-stone-900"
                        >
                          <a href={processUrl(subResult.url)}>
                            <p className="text-sm text-highlight font-bold">
                              {subResult.title?.replace("#", "") ||
                                "No title"}
                            </p>
                            <HighlightedText text={subResult.excerpt || ""} />
                          </a>
                        </li>

                      ),
                    )}
                  </ul>
                </li>
              ))}
            </ul>
            <Pagination pages={Math.ceil(results.length / PAGE_LENGTH)} page={page} setPage={setPage} />
          </>
        ) : query && <p className="mt-2">
          Sorry, no results for "{query}".
        </p>
        }
      </div>
    </>
  );
}