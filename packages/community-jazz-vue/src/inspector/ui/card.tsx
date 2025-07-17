import { computed, defineComponent } from "vue";

export const Card = defineComponent({
  name: "Card",
  props: {
    class: String,
    style: [String, Object],
    as: {
      type: String,
      default: "div",
    },
  },
  setup(props, { slots, attrs }) {
    const cardClasses = computed(() => {
      const classes = ["jazz-card"];
      if (props.class) {
        classes.push(props.class);
      }
      return classes.join(" ");
    });

    return () => {
      const Component = props.as as any;
      return (
        <Component {...attrs} class={cardClasses.value} style={props.style}>
          {slots.default?.()}
        </Component>
      );
    };
  },
});

export const CardHeader = defineComponent({
  name: "CardHeader",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    const headerClasses = computed(() => {
      const classes = ["jazz-card-header"];
      if (props.class) {
        classes.push(props.class);
      }
      return classes.join(" ");
    });

    return () => (
      <div {...attrs} class={headerClasses.value} style={props.style}>
        {slots.default?.()}
      </div>
    );
  },
});

export const CardBody = defineComponent({
  name: "CardBody",
  props: {
    class: String,
    style: [String, Object],
  },
  setup(props, { slots, attrs }) {
    const bodyClasses = computed(() => {
      const classes = ["jazz-card-body"];
      if (props.class) {
        classes.push(props.class);
      }
      return classes.join(" ");
    });

    return () => (
      <div {...attrs} class={bodyClasses.value} style={props.style}>
        {slots.default?.()}
      </div>
    );
  },
});
