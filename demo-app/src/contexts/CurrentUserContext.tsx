import { createContext, useContext, useState, type ReactNode } from "react";
import type { ObjectId } from "@/generated/types";

interface CurrentUserContextValue {
  currentUserId: ObjectId | null;
  setCurrentUserId: (id: ObjectId | null) => void;
}

const CurrentUserContext = createContext<CurrentUserContextValue | null>(null);

export function CurrentUserProvider({ children }: { children: ReactNode }) {
  const [currentUserId, setCurrentUserId] = useState<ObjectId | null>(null);

  return (
    <CurrentUserContext.Provider value={{ currentUserId, setCurrentUserId }}>
      {children}
    </CurrentUserContext.Provider>
  );
}

export function useCurrentUser() {
  const context = useContext(CurrentUserContext);
  if (!context) {
    throw new Error("useCurrentUser must be used within a CurrentUserProvider");
  }
  return context;
}
