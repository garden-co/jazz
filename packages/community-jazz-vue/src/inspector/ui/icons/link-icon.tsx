import { defineComponent } from "vue";

export const LinkIcon = defineComponent({
  name: "LinkIcon",
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
        fill="none"
        viewBox="0 0 24 24"
        stroke-width={props.strokeWidth || 1.5}
        stroke="currentColor"
        width={props.size || 24}
        height={props.size || 24}
        class={props.class}
        style={props.style}
      >
        <path
          stroke-linecap={props.strokeLinecap || "round"}
          stroke-linejoin="round"
          d="M13.19 8.688a4.5 4.5 0 0 1 1.242 7.244l-4.5 4.5a4.5 4.5 0 0 1-6.364-6.364l1.757-1.757m13.35-.622 1.757-1.757a4.5 4.5 0 0 0-6.364-6.364l-4.5 4.5a4.5 4.5 0 0 0 1.242 7.244"
        />
      </svg>
    );
  },
});
