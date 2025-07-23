import { type PropType, computed, defineComponent } from "vue";

export const Grid = defineComponent({
  name: "Grid",
  props: {
    cols: {
      type: Number as PropType<1 | 2 | 3>,
      required: true,
    },
    class: String,
    style: [String, Object] as PropType<string | Record<string, any>>,
  },
  setup(props, { slots, attrs }) {
    const gridClasses = computed(() => {
      const classes = [`jazz-grid-${props.cols}`];
      if (props.class) {
        classes.push(props.class);
      }
      return classes.join(" ");
    });

    return () => (
      <div {...attrs} class={gridClasses.value} style={props.style as any}>
        {slots.default?.()}
      </div>
    );
  },
});
