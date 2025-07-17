import { type SVGAttributes, defineComponent } from "vue";

export const DeleteIcon = defineComponent({
  name: "DeleteIcon",
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
        class={`lucide lucide-trash-icon lucide-trash ${props.class || ""}`}
        style={props.style}
      >
        <path d="M3 6h18" />
        <path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" />
        <path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" />
      </svg>
    );
  },
});
