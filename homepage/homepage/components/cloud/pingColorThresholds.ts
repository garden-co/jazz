export const pingColorMap = {
  5: {
    light: "hsl(220,100%,55%)",
    dark: "hsl(220,100%,100%)",
  },
  10: {
    light: "hsl(220,95%,58%)",
    dark: "hsl(220,100%,85%)",
  },
  15: {
    light: "hsl(220,93%,61%)",
    dark: "hsl(220,100%,75%)",
  },
  25: {
    light: "hsl(220,90%,64%)",
    dark: "hsl(220,100%,65%)",
  },
  35: {
    light: "hsl(220,88%,67%)",
    dark: "hsl(220,100%,58%)",
  },
  45: {
    light: "hsl(220,85%,70%)",
    dark: "hsl(220,100%,51%)",
  },
  55: {
    light: "hsl(220,82%,73%)",
    dark: "hsl(220,100%,44%)",
  },
  65: {
    light: "hsl(220,80%,76%)",
    dark: "hsl(220,100%,39%)",
  },
  100: {
    light: "hsl(220,75%,79%)",
    dark: "hsl(220,100%,35%)",
  },
  150: {
    light: "hsl(220,70%,82%)",
    dark: "hsl(220,100%,28%)",
  },
  200: {
    light: "hsl(220,67%,85%)",
    dark: "hsl(220,100%,23%)",
  },
  300: {
    light: "hsl(220,63%,89%)",
    dark: "hsl(220,100%,19%)",
  },
  1000: {
    light: "hsl(220,60%,93%)",
    dark: "hsl(220,100%,15%)",
  },
};

export const pingColorThresholds = Object.entries(pingColorMap).map(
  ([ping, { light, dark }]) => ({
    ping: Number(ping),
    bgClass: `dark:bg-[${dark}] bg-[${light}]`,
    fill: light,
    darkFill: dark,
  }),
);
