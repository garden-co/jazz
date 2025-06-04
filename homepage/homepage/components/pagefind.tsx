"use client";

import { Framework, frameworks } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { Command } from "cmdk";
import React, { useState, useEffect, useRef } from "react";
import { singletonHook } from "react-singleton-hook";

// Types
interface PagefindResult {
  id: string;
  url: string;
  meta: {
    title: string;
  };
  excerpt: string;
  sub_results?: Array<{
    id: string;
    title: string;
    url: string;
    excerpt: string;
  }>;
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

const navigateToUrl = (url: string, setOpen: (open: boolean) => void) => {
  if (!url) return;
  window.location.href = `${window.location.origin}${url}`;
  setOpen(false);
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

    // Check if query contains framework name or any of its alternative keywords
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

// Components
function HighlightedText({ text }: { text: string }) {
  const decodedText = text.replace(/&lt;/g, "<").replace(/&gt;/g, ">");
  const parts = decodedText.split(/(<mark>.*?<\/mark>)/g);

  return (
    <p className="mt-1">
      {parts.map((part, i) => {
        if (part.startsWith("<mark>")) {
          const content = part.replace(/<\/?mark>/g, "");
          return (
            <mark
              key={i}
              className="px-0.5 bg-primary-100 text-primary-900 dark:bg-stone-900 dark:text-white"
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

function SearchInput({
  query,
  onSearch,
}: {
  query: string;
  onSearch: (value: string) => void;
}) {
  return (
    <Command.Input
      value={query}
      onValueChange={onSearch}
      placeholder="Search documentation..."
      className="w-full text-base sm:text-lg px-4 sm:px-5 py-4 sm:py-5 outline-none border-b bg-white dark:bg-stone-950 text-stone-900 dark:text-stone-100 placeholder:text-stone-400 dark:placeholder:text-stone-500 focus-visible:ring-0"
    />
  );
}

function EmptyState() {
  return (
    <Command.Empty className="flex items-center justify-center h-16 text-sm">
      No results found.
    </Command.Empty>
  );
}

function MainResultItem({
  result,
  onSelect,
}: {
  result: PagefindResult;
  onSelect: () => void;
}) {
  return (
    <Command.Item
      value={result.meta.title}
      onSelect={onSelect}
      className="group relative flex items-center gap-2 sm:gap-3 px-3 sm:px-4 py-2 sm:py-3 cursor-pointer text-sm rounded-md mt-1 select-none
      transition-all duration-200 ease-in-out
      animate-in fade-in-0
      data-[selected=true]:bg-stone-100 dark:data-[selected=true]:bg-stone-925 hover:bg-stone-100 dark:hover:bg-stone-925 active:bg-stone-100 dark:active:bg-stone-925
      max-w-full"
    >
      <div className="min-w-0 flex-1">
        <h3 className="font-medium text-highlight truncate">
          {result.meta?.title || "No title"} ({(result.meta as any)?.framework})
        </h3>
        <HighlightedText text={result.excerpt || ""} />
      </div>
      <div className="absolute left-0 w-[3px] h-full bg-primary transition-opacity duration-200 ease-in-out opacity-0 group-data-[selected=true]:opacity-100" />
    </Command.Item>
  );
}

function SubResultItem({
  subResult,
  onSelect,
}: {
  subResult: NonNullable<PagefindResult["sub_results"]>[number];
  onSelect: () => void;
}) {
  return (
    <Command.Item
      key={subResult.id}
      value={subResult.title}
      onSelect={onSelect}
      className="group relative flex items-center gap-2 sm:gap-3 px-3 sm:px-4 py-2 sm:py-3 cursor-pointer text-sm rounded-md mt-1 select-none
      transition-all duration-200 ease-in-out
      animate-in fade-in-0
      data-[selected=true]:bg-stone-100 dark:data-[selected=true]:bg-stone-925 hover:bg-stone-100 dark:hover:bg-stone-925 active:bg-stone-100 dark:active:bg-stone-925
      max-w-full"
    >
      <div className="min-w-0 flex-1">
        <h4 className="text-sm font-medium truncate text-highlight">
          {subResult?.title || "No title"}
        </h4>
        <HighlightedText text={subResult?.excerpt || ""} />
      </div>
      <div className="absolute left-0 w-[3px] h-full bg-primary transition-opacity duration-200 ease-in-out opacity-0 group-data-[selected=true]:opacity-100" />
    </Command.Item>
  );
}

function SearchResults({
  results,
  setOpen,
  listRef,
}: {
  results: PagefindResult[];
  setOpen: (open: boolean) => void;
  listRef: React.RefObject<HTMLDivElement>;
}) {
  return (
    <Command.List
      ref={listRef}
      className="h-[50vh] sm:h-[300px] max-h-[60vh] sm:max-h-[400px] overflow-y-auto overflow-x-hidden overscroll-contain transition-all duration-100 ease-in p-2"
    >
      {results.length === 0 ? (
        <EmptyState />
      ) : (
        <Command.Group>
          {results.map((result) => (
            <SearchResult key={result.id} result={result} setOpen={setOpen} />
          ))}
        </Command.Group>
      )}
    </Command.List>
  );
}

function SearchResult({
  result,
  setOpen,
}: {
  result: PagefindResult;
  setOpen: (open: boolean) => void;
}) {
  if (!result) return null;

  const url = processUrl(result.url);

  const handleMainResultSelect = () => {
    navigateToUrl(url, setOpen);
  };

  const handleSubResultSelect = (
    subResult: NonNullable<PagefindResult["sub_results"]>[number],
  ) => {
    const { path, hash } = processSubUrl(subResult.url);
    navigateToUrl(`${path}${hash}`, setOpen);
  };

  return (
    <>
      <MainResultItem result={result} onSelect={handleMainResultSelect} />

      {result.sub_results && result.sub_results.length > 0 && (
        <div className="ml-4 border-l">
          {result.sub_results.map((subResult) => {
            // Avoid showing duplicate results
            if (subResult.title === result.meta.title) return null;

            return (
              <SubResultItem
                key={subResult.id}
                subResult={subResult}
                onSelect={() => handleSubResultSelect(subResult)}
              />
            );
          })}
        </div>
      )}
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
          const pagefindModule = await import(
            // @ts-expect-error - pagefind.js is generated after build and not available at compile time
            /* webpackIgnore: true */ "/_next/static/chunks/pages/pagefind/pagefind.js"
          );
          window.pagefind = pagefindModule.default || pagefindModule;

          // Configure ranking based on current framework context
          if (window.pagefind && window.pagefind.options) {
            await window.pagefind.options({
              ranking: {
                termFrequency: 0.8, // Reduce term frequency weight to favor content density
                pageLength: 0.6, // Reduce page length bias to favor comprehensive docs
                termSaturation: 1.2, // Allow more term repetition to boost framework-specific content
              },
            });
          }
        } catch (e) {
          window.pagefind = { search: async () => ({ results: [] }) };
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

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      setQuery("");
      setResults([]);
    }
    setOpen(open);
  };

  return (
    <Command.Dialog
      open={open}
      onOpenChange={handleOpenChange}
      label="Search"
      className="fixed top-[10%] sm:top-1/2 left-1/2 -translate-x-1/2 sm:-translate-y-1/2 w-full sm:w-auto z-20"
      shouldFilter={false}
      title="Search"
    >
      <div
        className="w-full sm:w-[640px] mx-auto max-w-[calc(100%-2rem)] overflow-hidden rounded-xl bg-white dark:bg-stone-950
          origin-center animate-in fade-in
          data-[state=open]:animate-in data-[state=closed]:animate-out
          data-[state=open]:scale-100 data-[state=closed]:scale-95
          data-[state=closed]:opacity-0 data-[state=open]:opacity-100
          transition-all duration-200 ease-in-out
          data-[state=open]:shadow-2xl data-[state=closed]:shadow-none
          shadow-lg ring-1 ring-stone-950/10 dark:ring-white/10
        "
      >
        <SearchInput query={query} onSearch={handleSearch} />
        <SearchResults results={results} setOpen={setOpen} listRef={listRef} />
      </div>
    </Command.Dialog>
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
