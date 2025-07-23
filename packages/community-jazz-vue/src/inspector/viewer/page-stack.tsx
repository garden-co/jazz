import { CoID, LocalNode, RawCoValue } from "cojson";
import { type PropType, computed, defineComponent } from "vue";
import { Page } from "./page.js";

// Define the structure of a page in the path
interface PageInfo {
  coId: CoID<RawCoValue>;
  name?: string;
}

// Props for the PageStack component
interface PageStackProps {
  path: PageInfo[];
  node?: LocalNode | null;
  goBack: () => void;
  addPages: (pages: PageInfo[]) => void;
}

export const PageStack = defineComponent({
  name: "PageStack",
  props: {
    path: {
      type: Array as PropType<PageInfo[]>,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode | null>,
      default: null,
    },
    goBack: {
      type: Function as PropType<() => void>,
      required: true,
    },
    addPages: {
      type: Function as PropType<(pages: PageInfo[]) => void>,
      required: true,
    },
  },
  setup(props, { slots }) {
    const page = computed(() => props.path[props.path.length - 1]);
    const index = computed(() => props.path.length - 1);

    return () => (
      <>
        <div class="jazz-page-stack">
          {slots.default?.()}
          {props.node && page.value && (
            <Page
              coId={page.value.coId}
              node={props.node}
              name={page.value.name || page.value.coId}
              onHeaderClick={props.goBack}
              onNavigate={props.addPages}
              isTopLevel={index.value === props.path.length - 1}
            />
          )}
        </div>
      </>
    );
  },
});
