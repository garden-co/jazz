import { Theme } from "@radix-ui/themes";
import type { ReactNode } from "react";

export function AppTheme({ children }: { children: ReactNode }) {
  return (
    <Theme
      appearance="light"
      accentColor="plum"
      grayColor="mauve"
      panelBackground="translucent"
      radius="medium"
      scaling="100%"
    >
      {children}
    </Theme>
  );
}
