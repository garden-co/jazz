"use client";

import {
  Tab,
  Tabs as FumadocsTabs,
  TabsContent,
  TabsList,
  type TabsProps as FumadocsTabsProps,
  TabsTrigger,
} from "fumadocs-ui/components/tabs";
import type { ComponentType } from "react";
import { useEffect, useMemo, useState } from "react";

type TabsProps = FumadocsTabsProps & {
  groupId?: string;
  persist?: boolean;
  updateAnchor?: boolean;
};

const groupListeners = new Map<string, Set<(value: string) => void>>();
const ControlledFumadocsTabs = FumadocsTabs as ComponentType<
  FumadocsTabsProps & {
    value?: string;
    onValueChange?: (value: string) => void;
  }
>;

function tabValue(value: string) {
  return value.toLowerCase().replace(/\s/, "-");
}

// Tabs in different groups can represent the same framework: a Next.js project
// is a React project, a SvelteKit project is a Svelte project, and so on. When a
// group sync delivers a value a block doesn't have, it falls back to the
// equivalent it does have (see syncFromGroup) rather than blanking out. Values
// are compared as `tabValue(...)` (lower-cased).
const EQUIVALENT_TABS: string[][] = [
  ["react", "next.js"],
  ["svelte", "sveltekit"],
];

function equivalentTabValues(value: string): string[] {
  return EQUIVALENT_TABS.find((group) => group.includes(value)) ?? [value];
}

function fragmentValue(value: string) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function syncGroup(groupId: string, value: string, persist: boolean) {
  for (const listener of groupListeners.get(groupId) ?? []) listener(value);
  sessionStorage.setItem(groupId, value);
  if (persist) localStorage.setItem(groupId, value);
}

function valueFromHash(groupId: string, items: string[]) {
  const hash = window.location.hash.slice(1);
  const prefix = `${groupId}-`;
  if (!hash.startsWith(prefix)) return;

  const requested = hash.slice(prefix.length);
  return items.find((item) => {
    const value = tabValue(item);
    return requested === value || requested === fragmentValue(item);
  });
}

function anchorFor(groupId: string, value: string) {
  return `#${groupId}-${fragmentValue(value)}`;
}

export function Tabs({
  groupId,
  persist = false,
  updateAnchor = false,
  defaultIndex = 0,
  defaultValue,
  items,
  ...props
}: TabsProps) {
  const resolvedDefaultValue = defaultValue ?? (items ? tabValue(items[defaultIndex]) : undefined);
  const [value, setValue] = useState(resolvedDefaultValue);
  const itemValues = useMemo(() => items ?? [], [items]);

  useEffect(() => {
    if (!groupId) return;

    const applyHash = () => {
      const next = valueFromHash(groupId, itemValues);
      if (next) syncGroup(groupId, tabValue(next), persist);
    };

    const syncFromGroup = (next: string) => {
      const values = itemValues.map(tabValue);
      // Prefer a direct match; otherwise map to an equivalent framework
      // (e.g. "Next.js" → "React") so a sibling selection carries over instead
      // of blanking this block out.
      const match = values.includes(next)
        ? next
        : equivalentTabValues(next).find((value) => values.includes(value));
      if (match) setValue(match);
    };

    const listeners = groupListeners.get(groupId) ?? new Set<(value: string) => void>();
    listeners.add(syncFromGroup);
    groupListeners.set(groupId, listeners);

    applyHash();
    window.addEventListener("hashchange", applyHash);

    return () => {
      listeners.delete(syncFromGroup);
      window.removeEventListener("hashchange", applyHash);
    };
  }, [groupId, itemValues, persist]);

  return (
    <ControlledFumadocsTabs
      {...props}
      defaultValue={resolvedDefaultValue}
      groupId={groupId}
      items={items}
      persist={persist}
      updateAnchor={false}
      value={value}
      onValueChange={(nextValue: string) => {
        if (items && !items.some((item) => tabValue(item) === nextValue)) return;
        if (updateAnchor && groupId) {
          window.history.replaceState(null, "", anchorFor(groupId, nextValue));
        }
        if (groupId) syncGroup(groupId, nextValue, persist);
        else setValue(nextValue);
      }}
    />
  );
}

export { Tab, TabsContent, TabsList, TabsTrigger };
