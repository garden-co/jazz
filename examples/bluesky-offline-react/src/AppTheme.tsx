import { Theme } from "@radix-ui/themes";
import type { ReactNode } from "react";

export function AppTheme({ children }: { children: ReactNode }) {
  return (
    <Theme appearance="light" accentColor="iris" grayColor="slate" radius="large" scaling="95%">
      {children}
    </Theme>
  );
}
