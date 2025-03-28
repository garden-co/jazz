import { classNames } from "../utils.js";
import { ChevronDownIcon } from "./icons/chevron-down-icon.js";
import { DeleteIcon } from "./icons/delete-icon.js";
import { LinkIcon } from "./icons/link-icon.js";

const icons = {
  chevronDown: ChevronDownIcon,
  delete: DeleteIcon,
  link: LinkIcon,
};

// copied from tailwind line height https://tailwindcss.com/docs/font-size
const sizes = {
  "2xs": 14,
  xs: 16,
  sm: 20,
  md: 24,
  lg: 28,
  xl: 28,
  "2xl": 32,
  "3xl": 36,
  "4xl": 40,
  "5xl": 48,
  "6xl": 60,
  "7xl": 72,
  "8xl": 96,
  "9xl": 128,
};

const strokeWidths = {
  "2xs": 2.5,
  xs: 2,
  sm: 2,
  md: 1.5,
  lg: 1.5,
  xl: 1.5,
  "2xl": 1.25,
  "3xl": 1.25,
  "4xl": 1.25,
  "5xl": 1,
  "6xl": 1,
  "7xl": 1,
  "8xl": 1,
  "9xl": 1,
};

export function Icon({
  name,
  size = "md",
  className,
  ...svgProps
}: {
  name?: keyof typeof icons;
  size?: keyof typeof sizes;
  className?: string;
} & React.SVGProps<SVGSVGElement>) {
  if (!name || !icons.hasOwnProperty(name)) {
    throw new Error(`Icon not found: ${name}`);
  }

  // @ts-ignore
  const IconComponent = icons?.hasOwnProperty(name) ? icons[name] : icon;

  return (
    <IconComponent
      aria-hidden="true"
      size={sizes[size]}
      strokeWidth={strokeWidths[size]}
      strokeLinecap="round"
      className={classNames(className)}
      {...svgProps}
    />
  );
}
