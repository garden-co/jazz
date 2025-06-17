import { useEffect, useState } from "react";

const STORAGE_KEY = "jazz-inspector-open";

export function useOpenInspector() {
  const [open, setOpen] = useState(() => {
    // Initialize from localStorage if available
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored ? JSON.parse(stored) : false;
  });

  // Update localStorage when open state changes
  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(open));
  }, [open]);

  return [open, setOpen];
}
