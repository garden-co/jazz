import { type PropType, defineComponent } from "vue";
import { Button } from "../ui/button.js";
import { PageInfo } from "./types.js";

interface BreadcrumbsProps {
  path: PageInfo[];
  onBreadcrumbClick: (index: number) => void;
}

export const Breadcrumbs = defineComponent({
  name: "Breadcrumbs",
  props: {
    path: {
      type: Array as PropType<PageInfo[]>,
      required: true,
    },
    onBreadcrumbClick: {
      type: Function as PropType<(index: number) => void>,
      required: true,
    },
  },
  setup(props) {
    return () => (
      <div class="jazz-breadcrumbs">
        <Button
          variant="link"
          style={{ padding: "0 0.25rem" }}
          onClick={() => props.onBreadcrumbClick(-1)}
        >
          Home
        </Button>
        {props.path.map((page, index) => (
          <>
            <span class="jazz-breadcrumb-separator" aria-hidden>
              /
            </span>
            <Button
              variant="link"
              style={{ padding: "0 0.25rem" }}
              onClick={() => props.onBreadcrumbClick(index)}
              key={page.coId}
            >
              {index === 0 ? page.name || "Root" : page.name}
            </Button>
          </>
        ))}
      </div>
    );
  },
});
