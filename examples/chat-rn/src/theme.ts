import { Theme } from "@react-navigation/native";

export const theme: Theme = {
  dark: false,
  colors: {
    primary: "#3b82f6", // blue color for active elements
    background: "#ffffff", // white background
    card: "#ffffff", // white background for cards
    text: "#000000", // black text
    border: "#d1d5db", // light gray border
    notification: "#ef4444", // red for notifications
  },
  fonts: {
    regular: {
      fontFamily: "System",
      fontWeight: "400" as const,
    },
    medium: {
      fontFamily: "System",
      fontWeight: "500" as const,
    },
    bold: {
      fontFamily: "System",
      fontWeight: "700" as const,
    },
    heavy: {
      fontFamily: "System",
      fontWeight: "900" as const,
    },
  },
};
