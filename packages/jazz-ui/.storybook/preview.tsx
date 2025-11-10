import type { Preview } from "@storybook/react";
import { withThemeByClassName } from "@storybook/addon-themes";
import React from "react";
import "../src/styles/index.css";
import "./storybook.css";

const preview: Preview = {
  parameters: {
    controls: { expanded: false },
    backgrounds: {
      disable: true,
    },
  },
  decorators: [
    withThemeByClassName({
      themes: {
        light: "",
        dark: "dark",
      },
      defaultTheme: "light",
    }),
    (Story) => {
      return (
        <div
          style={{
            backgroundColor: "var(--background)",
            color: "var(--foreground)",
            padding: "1rem",
            // minHeight: "100dvh",
          }}
        >
          <Story />
        </div>
      );
    },
  ],
};

export default preview;
