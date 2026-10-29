import { Card } from "@astryxdesign/core/Card";
import { Grid } from "@astryxdesign/core/Grid";
import { Link } from "@astryxdesign/core/Link";
import { VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { memoryCeilings, type MemoryCeiling } from "@/lib/showcase/field-metrics";

const repo = "https://github.com/garden-co/jazz";

function Harness({ path }: { path: string | null }) {
  if (!path) return null;
  return (
    <Link href={`${repo}/tree/main/${path}`} isExternalLink hasUnderline>
      How it is measured
    </Link>
  );
}

function CeilingCard({ ceiling }: { ceiling: MemoryCeiling }) {
  const value = `${ceiling.ceiling.toLocaleString("en-US")} ${ceiling.unit}`;
  return (
    <Card className="h-full">
      <VStack gap={1}>
        <Text type="supporting" display="block">
          {ceiling.scenario}
        </Text>
        <Text size="2xl" weight="medium" hasTabularNumbers display="block">
          {value}
        </Text>
        {ceiling.previous && (
          <Text type="supporting" hasTabularNumbers display="block">
            {ceiling.previous.label}: {ceiling.previous.ceiling.toLocaleString("en-US")}{" "}
            {ceiling.unit}
          </Text>
        )}
        <Text as="p" display="block">
          First to run out: {ceiling.firstToCross}
        </Text>
      </VStack>
    </Card>
  );
}

function MemoryCeilings({ ceilings }: { ceilings: MemoryCeiling[] }) {
  return (
    <VStack gap={2}>
      <Heading level={4}>Memory ceiling</Heading>
      <Text as="p" color="secondary" display="block" className="max-w-3xl">
        The most data the app holds before its server or a client crosses its memory budget:{" "}
        {memoryCeilings.budgets}.
      </Text>
      <VStack gap={3}>
        {ceilings.map((ceiling) => (
          <CeilingCard key={ceiling.scenario} ceiling={ceiling} />
        ))}
      </VStack>
      <Text type="supporting" display="block">
        Measured {memoryCeilings.measured} on {memoryCeilings.build}. {memoryCeilings.method}
      </Text>
      <Harness path={memoryCeilings.harness} />
    </VStack>
  );
}

/** Memory ceilings for one example, when it has been measured. */
export function FieldMetrics({ exampleId }: { exampleId: string }) {
  const ceilings = memoryCeilings.byExample[exampleId] ?? [];
  if (!ceilings.length) return null;
  return (
    <Grid columns={{ minWidth: 320, max: 2 }} gap={6}>
      <MemoryCeilings ceilings={ceilings} />
    </Grid>
  );
}
