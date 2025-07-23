import { defineComponent } from "vue";

export const ChevronDownIcon = defineComponent({
  name: "ChevronDownIcon",
  props: {
    size: Number,
    strokeWidth: Number,
    strokeLinecap: String as () => "round" | "butt" | "square",
    class: String,
    style: [String, Object],
  },
  setup(props, { attrs }) {
    return () => (
      <svg
        {...attrs}
        xmlns="http://www.w3.org/2000/svg"
        width={props.size || 24}
        height={props.size || 24}
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width={props.strokeWidth}
        stroke-linecap={props.strokeLinecap}
        stroke-linejoin="round"
        class={`lucide lucide-chevron-down ${props.class || ""}`}
        style={props.style}
      >
        <path d="m6 9 6 6 6-6" />
      </svg>
    );
  },
});
