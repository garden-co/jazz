import { defineComponent } from "vue";
import "../styles.css";

export const GlobalStyles = defineComponent({
  name: "GlobalStyles",
  setup(_, { slots }) {
    return () => slots.default?.();
  },
});
