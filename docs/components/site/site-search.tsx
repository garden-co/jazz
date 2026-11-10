"use client";

import { type Dispatch, type SetStateAction, useEffect, useMemo, useRef } from "react";
import { useRouter } from "next/navigation";
import { track } from "@vercel/analytics";
import { searchEvent } from "@/lib/search-analytics";
import { Search } from "lucide-react";
import { Button } from "@astryxdesign/core/Button";
import { IconButton } from "@astryxdesign/core/IconButton";
import {
  CommandPalette,
  CommandPaletteInput,
  useCommandPaletteContext,
} from "@astryxdesign/core/CommandPalette";
import { Icon } from "@astryxdesign/core/Icon";
import { Kbd } from "@astryxdesign/core/Kbd";
import { Text } from "@astryxdesign/core/Text";
import type { SearchSource, SearchableItem } from "@astryxdesign/core/Typeahead";

type SearchResult = {
  id: string;
  url: string;
  type: "page" | "heading" | "text";
  content: string;
};

type SearchItem = SearchableItem<{
  type: SearchResult["type"];
  group: string;
  url: string;
  /** The hit text with the endpoint's `<mark>` around each match. */
  marked: string;
}>;

const MARK = /<\/?mark>/g;

/**
 * Groups each heading and text hit under the page result it follows. Several
 * hits can share a URL, so items are keyed by the result id.
 */
function toItems(results: SearchResult[]): SearchItem[] {
  let page = "Docs";
  return results.map((result) => {
    const label = result.content.replace(MARK, "").trim();
    if (result.type === "page") page = label;
    return {
      id: result.id,
      label,
      auxiliaryData: {
        type: result.type,
        group: page,
        url: result.url,
        marked: result.content.trim(),
      },
    };
  });
}

/** Renders the endpoint's `<mark>` matches as highlights, never as HTML. */
function Marked({ text }: { text: string }) {
  return text.split(/(<mark>.*?<\/mark>)/g).map((part, i) =>
    part.startsWith("<mark>") ? (
      <mark key={i} className="bg-transparent font-semibold text-inherit">
        {part.replace(MARK, "")}
      </mark>
    ) : (
      part
    ),
  );
}

/**
 * Search input that highlights the first result of each new query, so Enter
 * opens it without an ArrowDown first. Astryx `CommandPalette` leaves the
 * highlight at -1 after each search and has no option for this yet (an upstream
 * prop is the proper home); this uses only its public context.
 */
function SearchInput() {
  const palette = useCommandPaletteContext();
  const query = palette?.search ?? "";
  const items = palette?.selectableItems;
  const setHighlightedIndex = palette?.setHighlightedIndex;
  // The query whose first result was last highlighted. Results change more than
  // once per query (instant filtering, then the fetched results), and a later
  // change must not override an ArrowDown or hover the user already made.
  const highlightedFor = useRef<string | null>(null);

  useEffect(() => {
    if (!items || !setHighlightedIndex) return;
    const first = items.findIndex((item) => !item.disabled);
    if (query === "" || first < 0) {
      highlightedFor.current = null;
      setHighlightedIndex(-1);
      return;
    }
    if (highlightedFor.current === query) return;
    highlightedFor.current = query;
    setHighlightedIndex(first);
  }, [query, items, setHighlightedIndex]);

  return <CommandPaletteInput placeholder="Search the docs" />;
}

/** Search trigger styled as a field, for the end of the desktop top nav. */
export function SearchField({ onOpen }: { onOpen: () => void }) {
  return (
    <Button
      label="Search the docs"
      variant="secondary"
      size="lg"
      width={220}
      icon={<Icon icon={Search} size="sm" />}
      onClick={onOpen}
    >
      <span className="flex w-full items-center justify-between gap-3">
        <span>Search docs</span>
        <Kbd keys="mod+k" />
      </span>
    </Button>
  );
}

/** Compact search trigger for narrow screens. */
export function SearchIconButton({ onOpen }: { onOpen: () => void }) {
  return (
    <IconButton
      label="Search"
      variant="ghost"
      size="sm"
      icon={<Icon icon={Search} size="sm" />}
      onClick={onOpen}
    />
  );
}

/** How long the query must stay unchanged before it counts as one search. */
const SETTLE_MS = 1200;

/**
 * Records a search once the user stops typing (or picks a result), not on
 * every keystroke the palette sends to the endpoint. Each distinct query is
 * recorded at most once per open page.
 */
function useSearchTracking() {
  const state = useRef<{
    timer?: ReturnType<typeof setTimeout>;
    pending?: { query: string; results: number };
    sent: Set<string>;
  }>({ sent: new Set() });

  useEffect(() => () => clearTimeout(state.current.timer), []);

  return useMemo(() => {
    const flush = () => {
      const s = state.current;
      clearTimeout(s.timer);
      const pending = s.pending;
      s.pending = undefined;
      if (!pending) return;
      const event = searchEvent(pending.query, "ui", pending.results);
      if (!event || s.sent.has(event.properties.query)) return;
      s.sent.add(event.properties.query);
      track(event.name, event.properties);
    };
    return {
      searched(query: string, results: number) {
        const s = state.current;
        clearTimeout(s.timer);
        s.pending = { query, results };
        s.timer = setTimeout(flush, SETTLE_MS);
      },
      flush,
    };
  }, []);
}

/**
 * Site search on Astryx `CommandPalette`, backed by the Fumadocs search
 * endpoint (`/api/search`). The caller owns the open state and renders the
 * triggers; Cmd/Ctrl+K toggles it and picking a result navigates to it.
 */
export function SiteSearch({
  isOpen,
  setOpen,
}: {
  isOpen: boolean;
  setOpen: Dispatch<SetStateAction<boolean>>;
}) {
  const router = useRouter();
  // The palette reports the picked item's id; map it back to its URL.
  const urls = useRef(new Map<string, string>());
  const tracking = useSearchTracking();

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((open) => !open);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [setOpen]);

  const source = useMemo<SearchSource<SearchItem>>(() => {
    let controller: AbortController | undefined;
    return {
      async search(query) {
        controller?.abort();
        controller = new AbortController();
        try {
          const response = await fetch(`/api/search?query=${encodeURIComponent(query)}`, {
            signal: controller.signal,
          });
          if (!response.ok) return [];
          const items = toItems((await response.json()) as SearchResult[]);
          for (const item of items) urls.current.set(item.id, item.auxiliaryData!.url);
          tracking.searched(query, items.length);
          return items;
        } catch {
          return [];
        }
      },
      bootstrap: () => [],
      cancel: () => controller?.abort(),
    };
  }, [tracking]);

  return (
    <CommandPalette<SearchItem>
      isOpen={isOpen}
      onOpenChange={setOpen}
      searchSource={source}
      label="Search the docs"
      input={<SearchInput />}
      emptyBootstrapText="Type to search the docs"
      emptySearchText="No results"
      onValueChange={(id) => {
        tracking.flush();
        const url = urls.current.get(id);
        if (url) router.push(url);
      }}
      renderItem={(item) => (
        <Text
          display="block"
          color={item.auxiliaryData?.type === "page" ? "primary" : "secondary"}
          weight={item.auxiliaryData?.type === "page" ? "medium" : undefined}
          maxLines={1}
        >
          {item.auxiliaryData?.type === "heading" && "# "}
          <Marked text={item.auxiliaryData?.marked ?? item.label} />
        </Text>
      )}
    />
  );
}
