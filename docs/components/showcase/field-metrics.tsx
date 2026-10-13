import { Card } from "@astryxdesign/core/Card";
import { Grid } from "@astryxdesign/core/Grid";
import { Link } from "@astryxdesign/core/Link";
import { VStack } from "@astryxdesign/core/Stack";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Heading, Text } from "@astryxdesign/core/Text";
import {
  loadTimes,
  memoryCeilings,
  type LoadTime,
  type MemoryCeiling,
} from "@/lib/showcase/field-metrics";

const repo = "https://github.com/garden-co/jazz";

export function seconds(ms: number): string {
  return `${(ms / 1000).toFixed(1)} s`;
}

function Harness({ path }: { path: string | null }) {
  if (!path) return null;
  return (
    <Link href={`${repo}/tree/main/${path}`} isExternalLink hasUnderline>
      How it is measured
    </Link>
  );
}

function LoadTimes({ rows }: { rows: LoadTime[] }) {
  return (
    <VStack gap={2}>
      <Heading level={4}>Load times</Heading>
      <Table density="compact" verticalAlign="top">
        <TableHeader>
          <TableRow>
            <TableHeaderCell>What the user does</TableHeaderCell>
            <TableHeaderCell>Local</TableHeaderCell>
            <TableHeaderCell>100 ms latency</TableHeaderCell>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.scenario}>
              <TableCell>
                <Text>{row.scenario}</Text>
              </TableCell>
              <TableCell>
                <Text weight="medium" hasTabularNumbers>
                  {seconds(row.localMs)}
                </Text>
              </TableCell>
              <TableCell>
                <Text hasTabularNumbers>{row.rttMs === null ? "—" : seconds(row.rttMs)}</Text>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      <Text type="supporting" display="block">
        Measured {loadTimes.measured} on {loadTimes.build}. {loadTimes.method}
      </Text>
      <Harness path={loadTimes.harness} />
    </VStack>
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

/** Load times and memory ceilings for one example, when it has been measured. */
export function FieldMetrics({ exampleId }: { exampleId: string }) {
  const times = loadTimes.byExample[exampleId] ?? [];
  const ceilings = memoryCeilings.byExample[exampleId] ?? [];
  if (!times.length && !ceilings.length) return null;
  return (
    <Grid columns={{ minWidth: 320, max: 2 }} gap={6}>
      {times.length > 0 && <LoadTimes rows={times} />}
      {ceilings.length > 0 && <MemoryCeilings ceilings={ceilings} />}
    </Grid>
  );
}
