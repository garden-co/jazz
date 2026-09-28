"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Search } from "lucide-react";
import { Button } from "@astryxdesign/core/Button";
import { IconButton } from "@astryxdesign/core/IconButton";
import { CommandPalette, CommandPaletteInput } from "@astryxdesign/core/CommandPalette";
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
 * Site search on Astryx `CommandPalette`, backed by the Fumadocs search
 * endpoint (`/api/search`). Opens with the button or Cmd/Ctrl+K; picking a
 * result navigates to it.
 */
export function SiteSearch() {
  const router = useRouter();
  const [isOpen, setOpen] = useState(false);
  // The palette reports the picked item's id; map it back to its URL.
  const urls = useRef(new Map<string, string>());

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((open) => !open);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

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
          return items;
        } catch {
          return [];
        }
      },
      bootstrap: () => [],
      cancel: () => controller?.abort(),
    };
  }, []);

  return (
    <>
      <span className="contents max-md:hidden">
        <Button
          label="Search"
          variant="secondary"
          size="sm"
          icon={<Icon icon={Search} size="sm" />}
          onClick={() => setOpen(true)}
        >
          <span className="flex items-center gap-3">
            Search
            <Kbd keys="mod+k" />
          </span>
        </Button>
      </span>
      <span className="contents md:hidden">
        <IconButton
          label="Search"
          variant="ghost"
          size="sm"
          icon={<Icon icon={Search} size="sm" />}
          onClick={() => setOpen(true)}
        />
      </span>
      <CommandPalette<SearchItem>
        isOpen={isOpen}
        onOpenChange={setOpen}
        searchSource={source}
        label="Search the docs"
        input={<CommandPaletteInput placeholder="Search the docs" />}
        emptyBootstrapText="Type to search the docs"
        emptySearchText="No results"
        onValueChange={(id) => {
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
    </>
  );
}
