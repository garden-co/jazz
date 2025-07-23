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

import { type PropType, defineComponent } from "vue";

export const Icon = defineComponent({
  name: "Icon",
  props: {
    name: {
      type: String as PropType<keyof typeof icons>,
      required: false,
    },
    size: {
      type: String as PropType<keyof typeof sizes>,
      default: "md",
    },
    class: String,
    style: [String, Object],
  },
  setup(props, { attrs }) {
    return () => {
      if (!props.name || !icons.hasOwnProperty(props.name)) {
        throw new Error(`Icon not found: ${props.name}`);
      }

      const IconComponent = icons[props.name];

      return (
        <IconComponent
          {...attrs}
          aria-hidden="true"
          size={sizes[props.size]}
          strokeWidth={strokeWidths[props.size]}
          strokeLinecap="round"
          class={props.class}
          style={props.style}
        />
      );
    };
  },
});
