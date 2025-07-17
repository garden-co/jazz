import { JsonObject } from "cojson";
import { type PropType, computed, defineComponent, ref, watch } from "vue";
import { Button } from "../ui/button.js";
import { Card, CardBody, CardHeader } from "../ui/card.js";
import { Text } from "../ui/text.js";
import { ValueRenderer } from "./value-renderer.js";

const CopyButton = defineComponent({
  name: "CopyButton",
  props: {
    data: {
      type: Object as PropType<JsonObject>,
      required: true,
    },
  },
  setup(props) {
    const copyCount = ref(0);
    const copied = computed(() => copyCount.value > 0);
    const stringifiedData = computed(() => JSON.stringify(props.data));

    watch(copyCount, (newCount) => {
      if (newCount > 0) {
        const timeout = setTimeout(() => {
          copyCount.value = 0;
        }, 1000);
        return () => {
          clearTimeout(timeout);
        };
      }
    });

    return () => (
      <Button
        style={{
          position: "absolute",
          top: "10px",
          right: "10px",
        }}
        onClick={() => {
          window.navigator.clipboard
            .writeText(stringifiedData.value)
            .then(() => {
              copyCount.value += 1;
            });
        }}
        variant="secondary"
      >
        {copied.value ? "Copied" : "Copy"}
      </Button>
    );
  },
});

export function RawDataCard({ data }: { data: JsonObject }) {
  return (
    <Card style={{ position: "relative" }}>
      <CardHeader>
        <Text strong>Raw data</Text>
        <CopyButton data={data} />
      </CardHeader>
      <CardBody>
        <ValueRenderer json={data} />
      </CardBody>
    </Card>
  );
}
