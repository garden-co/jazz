import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Button } from "@astryxdesign/core/Button";
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
  LargeValuesDiagram,
  PermissionsDiagram,
  SchemaDiagram,
  StackDiagram,
} from "@/components/home/diagrams";
import { AdopterQuotes } from "@/components/home/adopter-quotes";
import { CodeWindow } from "@/components/home/code-window";
import { PricingCalculator } from "@/components/home/pricing-calculator";
import { CreateJazzCommand } from "@/components/home/create-jazz-command";
import { FrameworkLogos } from "@/components/home/framework-logos";
import { pricingMeters } from "@/lib/home-pricing";
import { adopterQuotes } from "@/lib/home-quotes";
import { blogSource } from "@/lib/source";

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
  links: { label: string; href: string }[];
  diagram: ReactNode;
  caption: string;
}[] = [
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
    links: [{ label: "Permissions", href: "/docs/auth/permissions" }],
    diagram: <PermissionsDiagram />,
    caption: "A query and its read policy, planned together",
  },
  {
    id: "data",
    title: "Blobs, streams, JSON, richtext? To Jazz it's all just columns.",
    body: (
      <>
        <p>
          Jazz is designed to efficiently handle all kinds, sizes, shapes and intensities of data,
          while still representing everything in a simple relational model.
        </p>
        <ul>
          <li>Durable streams? Just keep appending to a column, or stream out of one.</li>
          <li>Binary blobs? Just put 2GB in a column, and read ranges.</li>
          <li>Giant JSON document? Just put it in a column and stream parts with JSON pointers.</li>
          <li>Long markdown document? It&apos;s just a very big string column.</li>
        </ul>
        <p>
          This allows you to truly keep all data in one place, with no external links and no
          separate systems. And permission policies apply exactly like on normal data.
        </p>
      </>
    ),
    links: [{ label: "Column types", href: "/docs/schemas/column-types" }],
    diagram: <LargeValuesDiagram />,
    caption:
      "Under the hood, Jazz uses prolly trees when values get large, ensuring appends, point edits, partial and streaming reads stay fast. Jazz syncs only the parts you need and any queries you run over large data operate in a streaming fashion.",
  },
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
    links: [
      { label: "How sync works", href: "/docs/concepts/how-sync-works" },
      { label: "Durability tiers", href: "/docs/reference/durability-tiers" },
    ],
    diagram: <ConsistencyDiagram />,
    caption: "A write's local state, its sync message, and its confirmed fate",
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
          Jazz gives you flexible, branch-like views over your data. Build anything from drafts to
          complex git-like workflows on them, including permissions over branches.
        </p>
      </>
    ),
    links: [
      { label: "Branches", href: "/docs/concepts/branches" },
      { label: "Edit metadata", href: "/docs/reading/queries#magic-columns" },
    ],
    diagram: <HistoryDiagram />,
    caption: "The history of one row, with a draft branch",
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
          Old clients keep working, new features ship without complicated rollouts, and apps with
          many feature flags stay safe to change.
        </p>
      </>
    ),
    links: [{ label: "Migrations", href: "/docs/schemas/migrations" }],
    diagram: <SchemaDiagram />,
    caption: "One raw table, read as two app versions through migration lenses",
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
    links: [
      { label: "Server setup", href: "/docs/getting-started/server-setup" },
      { label: "Auth providers", href: "/docs/recipes/auth/auth-provider-integration" },
      { label: "Permissions", href: "/docs/auth/permissions" },
      { label: "How sync works", href: "/docs/concepts/how-sync-works" },
    ],
    diagram: <BackendDiagram />,
    caption: "What moves out of your code into Jazz",
  },
];

const hostingRows: {
  topic: string;
  selfHosted: ReactNode;
  cloud: ReactNode;
  enterprise: ReactNode;
}[] = [
  {
    topic: "Setup",
    selfHosted: "One open-source, single-tenant server binary",
    cloud: "Zero config; create an app from the CLI or dashboard",
    enterprise: "Dedicated deployment, set up with you",
  },
  {
    topic: "Topology",
    selfHosted: "Runs where you deploy it",
    cloud: "Globally distributed and geo-optimized",
    enterprise: "Regions and data residency to your requirements",
  },
  {
    topic: "Reliability",
    selfHosted: "You operate backups and failover",
    cloud: "Fault-tolerant, with backups included",
    enterprise: "Uptime SLA and direct support",
  },
  {
    topic: "Scaling",
    selfHosted: "Size the instance yourself",
    cloud: "Scales more granularly than instance-based databases",
    enterprise: "Capacity planned with you",
  },
  ...pricingMeters.map((meter) => ({
    topic: meter.name,
    selfHosted: {
      Compute: "Your own servers; one instance runs a whole app",
      Storage: "Your own disks; keep room for row history",
      Egress: "Your hosting provider's rates",
    }[meter.name],
    cloud: (
      <>
        <Text as="p" display="block" weight="medium">
          {meter.price} {meter.unit}
        </Text>
        <Text as="p" display="block" type="supporting" color="secondary" className="mt-1">
          {meter.note} {meter.included}.
        </Text>
      </>
    ),
    enterprise: "Volume pricing",
  })),
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
    <div className="grid gap-4">
      <Heading level={2} type="display-3" id={id} className="home-anchor max-w-3xl">
        {title}
      </Heading>
      {children ? <div className="home-prose max-w-2xl">{children}</div> : null}
    </div>
  );
}

function Figure({
  children,
  caption,
  className,
  number,
  after,
}: {
  children: ReactNode;
  caption?: string;
  className?: string;
  number?: number;
  /** Rendered below the caption, such as a follow-up link. */
  after?: ReactNode;
}) {
  return (
    <figure className={`home-figure-frame ${className ?? ""}`}>
      <div className="home-figure">{children}</div>
      {caption ? (
        <figcaption className="home-figcaption">
          {number ? <span className="home-figcaption-number">Fig. {number}</span> : null}
          {caption}
        </figcaption>
      ) : null}
      {after}
    </figure>
  );
}

const dateFormatter = new Intl.DateTimeFormat("en-US", { dateStyle: "medium" });

function latestPosts(count: number) {
  return [...blogSource.getPages()]
    .sort((left, right) => new Date(right.data.date).getTime() - new Date(left.data.date).getTime())
    .slice(0, count);
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
              Jazz is a relational database built on real-time sync. It runs distributed across the
              cloud, your backend, frontend, native apps, CLIs and agent sandboxes. Mix and match
              ACID and local-first.
            </Text>
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="how-it-works" title="One database from the client to the cloud" />
          <Figure
            className="home-figure-wide mt-6"
            number={1}
            caption="Jazz uses the same Rust database engine core everywhere: as WebAssembly in the browser, as a native module in React Native and Node, and as the database server in Jazz Cloud and the jazz-tools CLI."
          >
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
          <SectionHeader id="code" title="In the client: feels like simple reactive state">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              Define tables and permissions in TypeScript, then query from any component. Writes
              apply locally at once and sync in the background.
            </Text>
          </SectionHeader>
          <div className="home-code-grid mt-6">
            <CodeWindow
              files={[
                { name: "OpenTodos.tsx", language: "tsx", code: componentCode },
                { name: "schema.ts", language: "ts", code: schemaCode },
                { name: "permissions.ts", language: "ts", code: permissionsCode },
              ]}
            />
            <Figure
              className="home-video-figure"
              number={2}
              caption="The todo example on two devices, recorded from the running app."
              after={
                <AppLink href="/examples" className="home-video-link font-medium">
                  More examples and benchmarks →
                </AppLink>
              }
            >
              <video
                className="home-video"
                src="/examples/videos/todo-two-devices.mp4"
                poster="/examples/videos/todo-two-devices.jpg"
                autoPlay
                muted
                loop
                playsInline
                aria-label="Two browser windows running the todo example. A todo added or checked off in one appears in the other."
              />
            </Figure>
          </div>
          <Text as="p" display="block" color="secondary" className="mt-6">
            Also for Vue, Svelte, Solid, React Native, plain TypeScript and Rust.{" "}
            <AppLink href="/docs/install/client">Install guides</AppLink>
          </Text>
        </div>
      </section>

      {adopterQuotes.length > 0 ? (
        <section className="home-section" aria-label="What adopters say">
          <div className="home-container">
            <AdopterQuotes quotes={adopterQuotes} />
          </div>
        </section>
      ) : null}

      <section className="home-section">
        <div className="home-container">
          <Heading level={2} type="display-3" id="features" className="home-anchor home-statement">
            Built into the database.{" "}
            <span className="home-statement-muted">
              The hard parts of shared, live data, handled once instead of in every app.
            </span>
          </Heading>
          <div className="mt-6">
            {features.map((feature, index) => (
              <article key={feature.id} className="home-feature">
                <div className="home-feature-text">
                  <Heading level={3} id={feature.id} className="home-anchor">
                    {feature.title}
                  </Heading>
                  <div className="home-prose mt-4">{feature.body}</div>
                  <div className="mt-5 flex flex-wrap gap-x-6 gap-y-2">
                    {feature.links.map((link) => (
                      <AppLink key={link.href} href={link.href} className="font-medium">
                        {link.label} →
                      </AppLink>
                    ))}
                  </div>
                </div>
                <Figure
                  className="home-feature-figure"
                  number={index + 3}
                  caption={feature.caption}
                >
                  {feature.diagram}
                </Figure>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="cloud" title="Self-host or use Jazz Cloud">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              The single-tenant Jazz server will always be open source and is easy to self-host.
              Jazz Cloud is the same database, running on global infrastructure tailored for it.
            </Text>
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              As developers, we hate pricing that makes limiting assumptions about your app and your
              users, so we bill across simple metrics for the things that are irreducibly hard.
            </Text>
          </SectionHeader>
          <div id="pricing" className="home-table home-anchor mt-6">
            <Table density="balanced" verticalAlign="top">
              <TableHeader>
                <TableRow>
                  <TableHeaderCell> </TableHeaderCell>
                  <TableHeaderCell>Self-hosted</TableHeaderCell>
                  <TableHeaderCell>Jazz Cloud</TableHeaderCell>
                  <TableHeaderCell>Enterprise</TableHeaderCell>
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
                    <TableCell>{row.enterprise}</TableCell>
                  </TableRow>
                ))}
                <TableRow>
                  <TableCell> </TableCell>
                  <TableCell>
                    <Button
                      label="How to self-host"
                      variant="secondary"
                      href="/docs/getting-started/server-setup#self-hosted-database-server"
                    />
                  </TableCell>
                  <TableCell>
                    <Button
                      label="Generate API key"
                      variant="primary"
                      href="https://v2.dashboard.jazz.tools"
                    />
                  </TableCell>
                  <TableCell>
                    <Button
                      label="Book a call"
                      variant="primary"
                      href="https://cal.com/anselm-io/cloud-pro-intro"
                    />
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </div>
          <div className="mt-12 grid gap-8 lg:grid-cols-12">
            <div className="lg:col-span-4">
              <Heading level={3}>Estimate your Jazz Cloud bill</Heading>
              <Text as="p" display="block" color="secondary" className="mt-3">
                Move the sliders to match your app. The estimate uses the Jazz Cloud prices above.
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

      <section className="home-section">
        <div className="home-container">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <Heading level={2} type="display-3" id="blog" className="home-anchor">
              From the blog
            </Heading>
            <AppLink href="/blog" className="font-medium">
              All posts →
            </AppLink>
          </div>
          <ol className="home-posts mt-10">
            {latestPosts(3).map((post) => (
              <li key={post.url} className="home-post">
                <Text as="p" display="block" type="supporting" color="secondary">
                  <time dateTime={new Date(post.data.date).toISOString().slice(0, 10)}>
                    {dateFormatter.format(new Date(post.data.date))}
                  </time>{" "}
                  · {post.data.author}
                </Text>
                <Heading level={3} className="mt-3">
                  <a href={post.url} className="home-post-link">
                    {post.data.title}
                  </a>
                </Heading>
                {post.data.description ? (
                  <Text as="p" display="block" color="secondary" className="mt-2">
                    {post.data.description}
                  </Text>
                ) : null}
              </li>
            ))}
          </ol>
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
        <div className="home-container mt-10">
          <FrameworkLogos />
        </div>
      </section>
    </div>
  );
}
