export interface FrameworkChange {
  key: "framework";
  value: string;
  target?: string;
}

export interface OtherChange {
  key: Exclude<string, "framework">;
  value: string;
}

export function isFrameworkChange(
  d: TabChangeEventDetail,
): d is FrameworkChange {
  return d.key === "framework";
}

export type TabChangeEventDetail = FrameworkChange | OtherChange;

// Extend the global WindowEventMap to include the TAB_CHANGE_EVENT
declare global {
  interface WindowEventMap {
    [TAB_CHANGE_EVENT]: CustomEvent<TabChangeEventDetail>;
  }
}

export const TAB_CHANGE_EVENT = "tabbedCodeGroupChange";
