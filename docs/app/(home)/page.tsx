import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Card } from "@astryxdesign/core/Card";
import { Heading } from "@astryxdesign/core/Heading";
import { Link as AstryxLink } from "@astryxdesign/core/Link";
import {
  Table,
  TableBody,
  TableCell,
  TableHeader,
  TableHeaderCell,
  TableRow,
} from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { AppLink } from "@/components/design/app-link";
import {
  BackendDiagram,
  ConsistencyDiagram,
  HistoryDiagram,
  PermissionsDiagram,
  SchemaDiagram,
  StackDiagram,
} from "@/components/home/diagrams";
import { HomeCode } from "@/components/home/home-code";
import { PricingCalculator } from "@/components/home/pricing-calculator";
import { CreateJazzCommand } from "@/components/home/create-jazz-command";
import { pricingMeters } from "@/lib/home-pricing";

export const metadata: Metadata = {
  title: "Jazz - The database that syncs.",
};

const schemaCode = `import { schema as s } from "jazz-tools";

const schema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      owner_id: s.uuid(),
    },
    {},
  ),
};

export const app = s.defineApp(schema);`;

const permissionsCode = `import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(
  app,
  ({ policy, session }) => {
    const mine = { owner_id: session.user.account };

    policy.todos.allowRead.where(mine);
    policy.todos.allowInsert.where(mine);
    policy.todos.allowUpdate
      .whereOld(mine)
      .whereNew(mine);
    policy.todos.allowDelete.where(mine);
  },
);`;

const componentCode = `import { useAll, useDb } from "jazz-tools/react";
import { app } from "./schema";

export function OpenTodos() {
  const db = useDb();
  const { data: todos = [] } = useAll(
    app.todos.where({ done: false }),
  );

  return todos.map((todo) => (
    <label key={todo.id}>
      <input
        type="checkbox"
        onChange={() =>
          db.update(app.todos, todo.id, { done: true })
        }
      />
      {todo.title}
    </label>
  ));
}`;

const principles = [
  {
    title: "Local copies",
    body: "Every client and server keeps the rows its queries need, so reads never wait on the network.",
  },
  {
    title: "Partial sync",
    body: "Only the rows a query touches move, on demand. First load is fast, later loads are instant.",
  },
  {
    title: "One source of truth",
    body: "Core authorizes every write and stores it durably. Everyone converges on the same data.",
  },
];

const features: {
  id: string;
  title: string;
  body: ReactNode;
  link: { label: string; href: string };
  diagram: ReactNode;
}[] = [
  {
    id: "consistency",
    title: "Local-first data with tunable consistency",
    body: (
      <>
        <p>
          Like an embedded database, Jazz brings durable data directly into your frontend and
          backend, and syncs it to the cloud. It feels like shared reactive state that abstracts
          away networking.
        </p>
        <p>
          Jazz is eventually consistent by default. Where transactionality matters, trade latency
          for globally consistent transactions, in the same database.
        </p>
      </>
    ),
    link: { label: "How sync works", href: "/docs/concepts/how-sync-works" },
    diagram: <ConsistencyDiagram />,
  },
  {
    id: "permissions",
    title: "Row-level security and per-query auth",
    body: (
      <>
        <p>
          Permissions are policies over your data and the user&apos;s JWT claims, defined in code
          and testable like the rest of your app.
        </p>
        <p>
          Jazz optimizes each query together with the policies that apply to it, which removes work
          from your backend and gives you zero-roundtrip security.
        </p>
      </>
    ),
    link: { label: "Permissions", href: "/docs/auth/permissions" },
    diagram: <PermissionsDiagram />,
  },
  {
    id: "history",
    title: "Real-time collaboration and deep edit histories",
    body: (
      <>
        <p>
          People and agents now edit the same data at a much higher rate, and you need to reason
          about it afterwards.
        </p>
        <p>
          Every row has a full, git-like branching history, with APIs for historical data, drafts
          and complex collaboration traces.
        </p>
      </>
    ),
    link: { label: "Branches", href: "/docs/concepts/branches" },
    diagram: <HistoryDiagram />,
  },
  {
    id: "schema",
    title: "Fluid schema evolution for fast teams",
    body: (
      <>
        <p>
          Instead of stop-the-world migrations, Jazz migrations are live compatibility layers that
          translate between the versions of your app.
        </p>
        <p>
          Old clients keep working, new features ship without a maintenance window, and apps with
          many feature flags stay safe to change.
        </p>
      </>
    ),
    link: { label: "Migrations", href: "/docs/schemas/migrations" },
    diagram: <SchemaDiagram />,
  },
  {
    id: "backend",
    title: "Slims your backend and simplifies your infra",
    body: (
      <>
        <p>
          Networking, permissions, auth integration and a collaboration-native data model are
          standard in Jazz, so your backend holds business logic instead of glue.
        </p>
        <p>
          Jazz also covers roles that usually need their own infrastructure or vendors: blob
          storage, file and image CDN, durable streams and real-time message queues.
        </p>
      </>
    ),
    link: { label: "Server setup", href: "/docs/getting-started/server-setup" },
    diagram: <BackendDiagram />,
  },
];

const hostingRows = [
  {
    topic: "Setup",
    selfHosted: "One open-source, single-tenant server binary",
    cloud: "Zero config; create an app from the CLI or dashboard",
  },
  {
    topic: "Topology",
    selfHosted: "Runs where you deploy it",
    cloud: "Globally distributed and geo-optimized",
  },
  {
    topic: "Reliability",
    selfHosted: "You operate backups and failover",
    cloud: "Fault-tolerant, with backups included",
  },
  {
    topic: "Scaling",
    selfHosted: "Size the instance yourself",
    cloud: "Scales more granularly than instance-based databases",
  },
  {
    topic: "Billing",
    selfHosted: "Open source; you pay only for your own hosting",
    cloud: "Usage-based, scales to zero",
  },
];

function SectionHeader({
  id,
  title,
  children,
}: {
  id?: string;
  title: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-12">
      <Heading level={2} type="display-3" id={id} className="home-anchor lg:col-span-6">
        {title}
      </Heading>
      {children ? (
        <div className="home-prose lg:col-span-5 lg:col-start-8 lg:pt-2">{children}</div>
      ) : null}
    </div>
  );
}

function Figure({ children, className }: { children: ReactNode; className?: string }) {
  return <figure className={`home-figure ${className ?? ""}`}>{children}</figure>;
}
export default function HomePage() {
  return (
    <div className="w-full">
      <section className="h-[80vh] w-full">
        <div className="mx-auto flex h-full w-full max-w-(--fd-layout-width) items-end px-4 relative">
          <aside className="absolute right-4 top-4 z-30 max-w-sm">
            <Card className="border-fd-border/70 shadow dark:border-white/50">
              <Text as="p" display="block" className="leading-relaxed">
                Announcing the Jazz v2 alpha!
              </Text>
              <Text as="p" display="block" className="leading-relaxed">
                See the{" "}
                <AppLink href="/blog/what-is-jazz" color="inherit" className="font-medium">
                  announcement post
                </AppLink>
                .
              </Text>
              <Text as="p" display="block" color="secondary" className="leading-relaxed">
                (Looking for{" "}
                <AstryxLink href="https://classic.jazz.tools" color="inherit">
                  classic Jazz
                </AstryxLink>
                ?)
              </Text>
            </Card>
          </aside>
          <div className="w-full max-w-[42rem] space-y-6 pb-2 sm:space-y-10">
            <Heading level={1} type="display-1">
              <span className="block">the</span>
              <span className="block -ml-[0.04em]">database</span>
              <span className="block">that syncs</span>
            </Heading>
            <Text as="p" display="block" className="max-w-[40em] text-xl leading-relaxed">
              Jazz is a local-first relational database. It runs across your frontend, backend and
              our global storage cloud. Sync partial tables, durable streams and files, fast. Feels
              like simple reactive state.
            </Text>
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="how-it-works" title="One database, from the client to the cloud">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              Jazz runs inside your apps, your servers and the cloud at once. Each keeps a synced
              copy of the rows it uses, and Core keeps everyone consistent and secure.
            </Text>
          </SectionHeader>
          <Figure className="home-figure-wide mt-12">
            <StackDiagram />
          </Figure>
          <div className="home-facts mt-10">
            {principles.map((item) => (
              <div key={item.title} className="home-fact">
                <Heading level={3}>{item.title}</Heading>
                <Text as="p" display="block" color="secondary" className="mt-2">
                  {item.body}
                </Text>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="code" title="Feels like simple reactive state">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              Define tables and permissions in TypeScript, then query from any component. Writes
              apply locally at once and sync in the background.
            </Text>
          </SectionHeader>
          <div className="mt-12 grid gap-4 lg:grid-cols-3">
            <HomeCode title="schema.ts" language="ts" code={schemaCode} />
            <HomeCode title="permissions.ts" language="ts" code={permissionsCode} />
            <HomeCode title="OpenTodos.tsx" language="tsx" code={componentCode} />
          </div>
          <Text as="p" display="block" color="secondary" className="mt-6">
            Also for Vue, Svelte, Solid, React Native, plain TypeScript and Rust.{" "}
            <AppLink href="/docs/install/client">Install guides</AppLink>
          </Text>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="features" title="Built into the database">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              The hard parts of shared, live data, handled once in the database instead of in every
              app.
            </Text>
          </SectionHeader>
          <div className="mt-12">
            {features.map((feature) => (
              <article key={feature.id} className="home-feature">
                <div className="home-feature-text">
                  <Heading level={3} id={feature.id} className="home-anchor">
                    {feature.title}
                  </Heading>
                  <div className="home-prose mt-4">{feature.body}</div>
                  <AppLink href={feature.link.href} className="mt-5 inline-block font-medium">
                    {feature.link.label} →
                  </AppLink>
                </div>
                <Figure className="home-feature-figure">{feature.diagram}</Figure>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container grid gap-12 lg:grid-cols-12">
          <div className="lg:col-span-5">
            <Heading level={2} type="display-3" id="cloud" className="home-anchor">
              A globally synced, auto-scaling database cloud
            </Heading>
            <div className="home-prose mt-6">
              <p>
                The single-tenant Jazz server will always be open source and is easy to self-host.
                Jazz Cloud is the same database on infrastructure built for it.
              </p>
              <p>
                It&apos;s zero-config to set up, works from your first experiment and scales much
                more granularly than traditional instance-based databases.
              </p>
            </div>
          </div>
          <div className="home-table lg:col-span-7 lg:col-start-6">
            <Table density="balanced" verticalAlign="top">
              <TableHeader>
                <TableRow>
                  <TableHeaderCell> </TableHeaderCell>
                  <TableHeaderCell>Self-hosted</TableHeaderCell>
                  <TableHeaderCell>Jazz Cloud</TableHeaderCell>
                </TableRow>
              </TableHeader>
              <TableBody>
                {hostingRows.map((row) => (
                  <TableRow key={row.topic}>
                    <TableCell>
                      <Text weight="medium">{row.topic}</Text>
                    </TableCell>
                    <TableCell>
                      <Text color="secondary">{row.selfHosted}</Text>
                    </TableCell>
                    <TableCell>{row.cloud}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="pricing" title="Simple billing that scales to zero">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              We bill for the things that are irreducibly hard, and make no assumptions about your
              app or your users. Global infrastructure, billed in predictable units.
            </Text>
          </SectionHeader>
          <div className="home-meters mt-12">
            {pricingMeters.map((meter) => (
              <div key={meter.name} className="home-meter">
                <Text as="p" display="block" type="label" color="secondary">
                  {meter.name}
                </Text>
                <Heading level={3} type="display-3" className="mt-3">
                  {meter.price}
                </Heading>
                <Text as="p" display="block" weight="medium" className="mt-1">
                  {meter.unit}
                </Text>
                <Text as="p" display="block" type="supporting" color="secondary" className="mt-4">
                  {meter.note} {meter.included}.
                </Text>
              </div>
            ))}
          </div>
          <div className="mt-12 grid gap-8 lg:grid-cols-12">
            <div className="lg:col-span-4">
              <Heading level={3}>Estimate your bill</Heading>
              <Text as="p" display="block" color="secondary" className="mt-3">
                Move the sliders to match your app. The estimate uses the public meters above.
              </Text>
            </div>
            {/* Not ported to Astryx yet. A data-astryx-theme attribute ends the
                theme's @scope, so its element resets (p, h1-h6, code) leave the
                calculator's Tailwind typography alone. */}
            <div className="lg:col-span-8" data-astryx-theme="none">
              <PricingCalculator />
            </div>
          </div>
        </div>
      </section>

      <section className="home-section home-cta">
        <div className="home-container grid gap-8 lg:grid-cols-12 lg:items-end">
          <div className="lg:col-span-7">
            <Heading level={2} type="display-2">
              npm create jazz
            </Heading>
            <Text as="p" display="block" type="large" color="secondary" className="mt-4">
              Scaffold a synced app in one command, with auth and hosting set up for you.
            </Text>
          </div>
          <div className="flex flex-col gap-4 lg:col-span-5 lg:items-end">
            <CreateJazzCommand />
            <Text as="p" display="block" color="secondary">
              Read the <AppLink href="/docs/quickstart">quickstart</AppLink> or join the{" "}
              <AstryxLink href="https://discord.gg/RN9UKh52be">Jazz Discord</AstryxLink>.
            </Text>
          </div>
        </div>
      </section>
    </div>
  );
}
