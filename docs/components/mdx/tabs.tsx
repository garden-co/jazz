"use client";

import { Tab as AstryxTab, TabList } from "@astryxdesign/core/TabList";
import {
  type ReactNode,
  createContext,
  useContext,
  useEffect,
  useId,
  useMemo,
  useState,
} from "react";

type TabsProps = {
  items?: string[];
  groupId?: string;
  persist?: boolean;
  updateAnchor?: boolean;
  defaultIndex?: number;
  defaultValue?: string;
  children?: ReactNode;
};

const groupListeners = new Map<string, Set<(value: string) => void>>();

function tabValue(value: string) {
  return value.toLowerCase().replace(/\s/, "-");
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

function storedValue(groupId: string) {
  try {
    return sessionStorage.getItem(groupId) ?? localStorage.getItem(groupId);
  } catch {
    return null;
  }
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

const TabsContext = createContext<{ value: string | undefined; baseId: string } | null>(null);

/**
 * MDX `<Tabs>` on Astryx `TabList`. Keeps the Fumadocs authoring API
 * (`items`, `groupId`, `persist`, `updateAnchor`) so content is unchanged:
 * tabs sharing a `groupId` switch together, `persist` remembers the choice
 * across visits, and `updateAnchor` makes the choice linkable
 * (`#jazz-framework-vue`).
 */
export function Tabs({
  groupId,
  persist = false,
  updateAnchor = false,
  defaultIndex = 0,
  defaultValue,
  items,
  children,
}: TabsProps) {
  const baseId = useId();
  const itemValues = useMemo(() => items ?? [], [items]);
  const resolvedDefaultValue =
    defaultValue ?? (itemValues.length > 0 ? tabValue(itemValues[defaultIndex]) : undefined);
  const [value, setValue] = useState(resolvedDefaultValue);

  useEffect(() => {
    if (!groupId) return;

    const known = (next: string | null) =>
      next != null && itemValues.some((item) => tabValue(item) === next);
    const stored = storedValue(groupId);
    if (known(stored)) setValue(stored!);

    const applyHash = () => {
      const next = valueFromHash(groupId, itemValues);
      if (next) syncGroup(groupId, tabValue(next), persist);
    };

    const listeners = groupListeners.get(groupId) ?? new Set<(value: string) => void>();
    const listener = (next: string) => {
      if (known(next)) setValue(next);
    };
    listeners.add(listener);
    groupListeners.set(groupId, listeners);

    applyHash();
    window.addEventListener("hashchange", applyHash);

    return () => {
      listeners.delete(listener);
      window.removeEventListener("hashchange", applyHash);
    };
  }, [groupId, itemValues, persist]);

  const select = (next: string) => {
    if (!itemValues.some((item) => tabValue(item) === next)) return;
    if (updateAnchor && groupId) {
      window.history.replaceState(null, "", anchorFor(groupId, next));
    }
    if (groupId) syncGroup(groupId, next, persist);
    else setValue(next);
  };

  return (
    <div className="my-6">
      <TabList value={value ?? ""} onChange={select} role="tablist" hasDivider size="sm">
        {itemValues.map((item) => (
          <AstryxTab
            key={item}
            value={tabValue(item)}
            label={item}
            panelId={`${baseId}-${tabValue(item)}`}
          />
        ))}
      </TabList>
      <TabsContext.Provider value={{ value, baseId }}>{children}</TabsContext.Provider>
    </div>
  );
}

/** One panel of an MDX `<Tabs>`; `value` matches an entry of `items`. */
export function Tab({ value, children }: { value: string; children?: ReactNode }) {
  const tabs = useContext(TabsContext);
  if (!tabs) return <>{children}</>;
  const id = tabValue(value);
  return (
    <div
      role="tabpanel"
      id={`${tabs.baseId}-${id}`}
      hidden={tabs.value !== id}
      className="pt-4 [&>:first-child]:mt-0 [&>:last-child]:mb-0"
    >
      {children}
    </div>
  );
}
