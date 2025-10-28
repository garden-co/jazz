import { JsonObject } from "cojson";
import { useEffect, useState } from "react";
import { Button } from "../ui/button.js";
import { Card, CardBody, CardHeader } from "../ui/card.js";
import { Text } from "../ui/text.js";
import { ValueRenderer } from "./value-renderer.js";

function CopyButton({ data }: { data: JsonObject }) {
  const [copyCount, setCopyCount] = useState(0);
  const copied = copyCount > 0;
  const stringifiedData = JSON.stringify(data);

  useEffect(() => {
    if (copyCount > 0) {
      const timeout = setTimeout(() => setCopyCount(0), 1000);
      return () => {
        clearTimeout(timeout);
      };
    }
  }, [copyCount]);

  return (
    <Button
      style={{
        position: "absolute",
        top: "10px",
        right: "10px",
      }}
      onClick={() => {
        window.navigator.clipboard.writeText(stringifiedData).then(() => {
          setCopyCount((count) => count + 1);
        });
      }}
      variant="secondary"
    >
      {copied ? "Copied" : "Copy"}
    </Button>
  );
}

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
