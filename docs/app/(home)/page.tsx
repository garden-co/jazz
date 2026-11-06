import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Button } from "@astryxdesign/core/Button";
import { Heading } from "@astryxdesign/core/Heading";
import { Link as AstryxLink } from "@astryxdesign/core/Link";
import { Text } from "@astryxdesign/core/Text";
import { AppLink } from "@/components/design/app-link";
import {
  BackendDiagram,
  FrontendDiagram,
  HistoryDiagram,
  LargeValuesDiagram,
  PermissionsDiagram,
  SchemaDiagram,
} from "@/components/home/diagrams";
import { AdopterQuotes } from "@/components/home/adopter-quotes";
import { DeploymentCarousel } from "@/components/home/deployment-carousel";
import { CodeWindow } from "@/components/home/code-window";
import { PricingCalculator } from "@/components/home/pricing-calculator";
import { jazzTiers } from "@/lib/home-pricing";
import { AgentPrompt } from "@/components/home/agent-prompt";
import { FrameworkLogos } from "@/components/home/framework-logos";
import { WalkthroughVideo } from "@/components/showcase/walkthrough-video";
import { adopterQuotes } from "@/lib/home-quotes";
import { blogSource } from "@/lib/source";
import { StippleCanvas, heroPattern } from "@garden-co/design/stipple";

/** The agent quickstart, shown in full in the call to action. */
const quickstart = readFileSync(join(process.cwd(), "public/quickstart.md"), "utf8").trim();

/**
 * The quickstart is hard-wrapped at 100 columns, which wraps again in a
 * narrow box. For display, join each paragraph or list item back into one
 * line; headings, list markers and "Default:" / "See" lines start new ones.
 */
function unwrap(markdown: string) {
  const out: string[] = [];
  for (const line of markdown.split("\n")) {
    const text = line.trim();
    const previous = out[out.length - 1];
    const startsBlock = /^(#|- |\d+\. |Default:|See )/.test(text);
    if (text && previous?.trim() && !startsBlock && !previous.trim().startsWith("#")) {
      out[out.length - 1] = `${previous} ${text}`;
    } else {
      out.push(line);
    }
  }
  return out.join("\n");
}

export const metadata: Metadata = {
  title: "Jazz - The database that syncs.",
};

const serverCode = `import { createJazz, vercel } from "jazz-tools/backend";
import { app, permissions } from "./schema";

export const jazz = createJazz({
  app,
  permissions,
  // Also: cloudflare() (coming soon), and plain sqlite()
  // for local dev and your own servers
  store: vercel(),
  // Better Auth, WorkOS, many other providers or any custom auth:
  // pass on the user ID and permission-relevant claims, or null
  authenticate: (request) => getSignedInUser(request),
});`;

const routeCode = `import { jazz } from "@/jazz.server";
import { app } from "@/schema";

export async function GET(request: Request) {
  // Runs as the signed-in user: only their todos
  // according to permissions defined in schema.ts
  const db = await jazz.forReqUser(request);
  const todos = await db.all(app.todos.where({ done: false }));
  return Response.json(todos);
}`;

const mountCode = `// Sync for browsers, on your own origin
import { jazz } from "@/jazz.server";

export const { GET, POST, DELETE } = jazz.next();`;

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

export const app = s.defineApp(schema);

export const permissions = s.definePermissions(
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

const componentCode = `import { useAll } from "jazz-tools/react";
import { app } from "./schema";

export function OpenTodos() {
  // As an alternative to traditional querying via api/todos
  // you can query and write to Jazz directly from the client
  // which will do real-time sync through the /jazz route
  // either through a thin client or fully local-first with offline support
  const { data: todos = [], db } = useAll(
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
    title: "Inside your backend",
    body: "Jazz is a library your server imports, wherever you deploy it, and keeps its data on that platform's storage. Its schema and permissions are the code you deploy, with no database server to run.",
  },
  {
    title: "Live queries everywhere",
    body: "Traditional clients call your API routes as usual. Browsers, mobile apps and backend clients can also live-sync on your own origin, querying straight from a component. Results stay up to date as anyone writes.",
  },
  {
    title: "Local-first when you want it",
    body: "Clients start thin. Switch one to local-first for offline use and instant reads: it runs the same Rust engine on the device, with the same queries.",
  },
];

const features: {
  id: string;
  title: string;
  body: ReactNode;
  links: { label: string; href: string }[];
  diagram: ReactNode;
}[] = [
  {
    id: "permissions",
    title: "Declarative permissions and per-query auth",
    body: (
      <>
        <p>
          Declarative permissions are policies over your data and the user your backend signed in,
          defined in code and testable like the rest of your app.
        </p>
        <p>
          Jazz optimizes each query together with the policies that apply to it, so the same rules
          hold whether a query runs in a route handler or comes straight from the browser.
        </p>
      </>
    ),
    links: [{ label: "Permissions", href: "/docs/schemas/policies" }],
    diagram: <PermissionsDiagram />,
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
        <p>
          Under the hood, large values are stored as prolly trees, so appends, point edits, partial
          and streaming reads stay fast. Jazz syncs only the parts you need, and queries over large
          data run in a streaming fashion.
        </p>
      </>
    ),
    links: [{ label: "Column types", href: "/docs/schemas/column-types" }],
    diagram: <LargeValuesDiagram />,
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
      { label: "Branches", href: "/docs/recipes/drafts-and-branches" },
      { label: "Edit metadata", href: "/docs/reading/metadata" },
    ],
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
          Old clients keep working, new features ship without complicated rollouts, and apps with
          many feature flags stay safe to change.
        </p>
      </>
    ),
    links: [{ label: "Migrations", href: "/docs/schemas/migrations" }],
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
          storage, durable streams and real-time messaging, all under the same permissions.
        </p>
      </>
    ),
    links: [
      { label: "Main backend", href: "/docs/setup/deployment/own-server" },
      { label: "Auth", href: "/docs/setup/auth/backend" },
      { label: "Permissions", href: "/docs/schemas/policies" },
      { label: "How sync works", href: "/docs/how-jazz-works" },
    ],
    diagram: <BackendDiagram />,
  },
  {
    id: "frontend",
    title: "Slims your frontend, from thin client to local-first",
    body: (
      <>
        <p>
          Even as a thin client, Jazz replaces the request/response loop and all the client state
          around it. A component asks for the rows it needs with one reactive query hook and stays
          live as they change: no fetch calls per endpoint, no cache to invalidate, no refetching.
        </p>
        <p>
          Turn a client local-first and Jazz also takes over the client logic that is usually the
          hardest to hand-build: optimistic updates, sync and offline support. It keeps a durable
          copy on the device, so reads never wait on the network and writes show up at once.
        </p>
      </>
    ),
    links: [
      { label: "Thin client", href: "/docs/setup/web-client/thin-client" },
      { label: "Local-first client", href: "/docs/setup/web-client/local-first-client" },
      { label: "How sync works", href: "/docs/how-jazz-works" },
    ],
    diagram: <FrontendDiagram />,
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
  className,
  after,
}: {
  children: ReactNode;
  className?: string;
  /** Rendered below the figure, such as a follow-up link. */
  after?: ReactNode;
}) {
  return (
    <figure className={`home-figure-frame ${className ?? ""}`}>
      <div className="home-figure">{children}</div>
      {after}
    </figure>
  );
}

const dateFormatter = new Intl.DateTimeFormat("en-US", { dateStyle: "medium" });

/** The "From the blog" section is hidden for now; flip to show it again. */
const SHOW_BLOG = false;

function latestPosts(count: number) {
  return [...blogSource.getPages()]
    .sort((left, right) => new Date(right.data.date).getTime() - new Date(left.data.date).getTime())
    .slice(0, count);
}
export default function HomePage() {
  return (
    <div className="w-full">
      <section className="home-hero h-[80vh] w-full">
        <StippleCanvas pattern={heroPattern} className="home-hero-pattern" />
        <div aria-hidden className="home-hero-fade" />
        <div className="mx-auto flex h-full w-full max-w-(--fd-layout-width) items-end px-4 relative">
          <div className="w-full max-w-[42rem] space-y-6 pb-2 sm:space-y-10">
            <Heading level={1} type="display-1">
              <span className="block">the</span>
              <span className="block -ml-[0.04em]">database</span>
              <span className="block">that syncs</span>
            </Heading>
            <Text as="p" display="block" className="max-w-[40em] text-xl leading-relaxed">
              Jazz is a relational database that runs inside your backend, with live queries,
              permissions and collaboration features built in. Query it from your server and
              straight from your frontend with thin or local-first clients.
            </Text>
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader
            id="how-it-works"
            title="One database, from your backend to every client"
          />
          <Figure className="home-figure-wide mt-6">
            <DeploymentCarousel />
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
          <SectionHeader id="code" title="Mount it in your backend, query it from anywhere">
            <Text as="p" display="block" type="large" color="secondary" weight="normal">
              Define tables and permissions in TypeScript and mount Jazz in your server. Then query
              from route handlers, or from any component as simple reactive state. Writes show up at
              once and sync in the background.
            </Text>
          </SectionHeader>
          <div className="home-code-grid mt-6">
            <CodeWindow
              files={[
                { name: "api/todos/route.ts", language: "ts", code: routeCode },
                { name: "schema.ts", language: "ts", code: schemaCode },
                { name: "jazz.server.ts", language: "ts", code: serverCode },
                {
                  name: "jazz/[...path]/route.ts",
                  language: "ts",
                  code: mountCode,
                },
                { name: "OpenTodos.tsx", language: "tsx", code: componentCode },
              ]}
            />
            <Figure
              className="home-video-figure"
              after={
                <AppLink href="/examples" className="home-video-link font-medium">
                  More examples and benchmarks →
                </AppLink>
              }
            >
              <WalkthroughVideo
                className="home-video"
                src="/examples/videos/stage-plan-two-devices.mp4"
                poster="/examples/videos/stage-plan-two-devices.jpg"
                width={1280}
                height={926}
                aria-label="Two laptops running the StagePlan example, a crew chief and a crew member on the same show board. A task added or a card moved on one appears on the other, and a comment shows up live in the task the other has open. Edits made with the laptop's Wi-Fi turned off arrive when it is turned back on."
              />
            </Figure>
          </div>
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
            A database responsible for more.
          </Heading>
          <div className="mt-6">
            {features.map((feature) => (
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
                <Figure className="home-feature-figure">{feature.diagram}</Figure>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section">
        <div className="home-container">
          <SectionHeader id="pricing" title="Priced for flexible self-hosting & BYOC" />
          <div className="mt-8 grid gap-6 border-t pt-6 sm:grid-cols-2 lg:grid-cols-4">
            {(["oss", "pro", "scale", "custom"] as const).map((id) => (
              <div key={id} className="grid content-start gap-2">
                <Text as="p" display="block" weight="semibold">
                  {jazzTiers[id].name}
                </Text>
                <Text as="p" display="block" type="large">
                  {jazzTiers[id].price}
                  {jazzTiers[id].priceExtra ? (
                    <>
                      <br />
                      {jazzTiers[id].priceExtra}
                    </>
                  ) : null}
                </Text>
                <Text as="p" display="block" color="secondary">
                  {jazzTiers[id].summary}
                </Text>
                {jazzTiers[id].note ? (
                  <Text as="p" display="block" type="supporting" color="secondary">
                    {jazzTiers[id].note}
                  </Text>
                ) : null}
                {id === "custom" ? (
                  <div className="mt-2">
                    <Button
                      label="Talk to us"
                      variant="secondary"
                      href="https://cal.com/anselm-io/cloud-pro-intro"
                    />
                  </div>
                ) : null}
              </div>
            ))}
          </div>
          <div className="mt-12 grid gap-8">
            <Heading level={3}>What it costs on each platform</Heading>
            {/* Not ported to Astryx yet. A data-astryx-theme attribute ends the
                theme's @scope, so its element resets (p, h1-h6, code) leave the
                calculator's Tailwind typography alone. */}
            <div data-astryx-theme="none">
              <PricingCalculator />
            </div>
          </div>
        </div>
      </section>

      {SHOW_BLOG ? (
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
      ) : null}

      <section className="home-section home-cta">
        <div className="home-container grid gap-8 lg:grid-cols-12 lg:items-start">
          <div className="lg:col-span-6">
            <Heading level={2} type="display-2">
              Start with your agent
            </Heading>
            <Text as="p" display="block" type="large" color="secondary" className="mt-4">
              Paste this into your coding agent. It works for a new project and for an app you
              already have: the agent asks what it needs and sets Jazz up in your backend.
            </Text>
            <div className="mt-8">
              <FrameworkLogos />
            </div>
          </div>
          <div className="flex flex-col gap-4 lg:col-span-6 lg:items-end">
            <AgentPrompt quickstart={quickstart} display={unwrap(quickstart)} />
            <Text as="p" display="block" color="secondary">
              Or have it run <code>npm create jazz</code>. Read the{" "}
              <AppLink href="/docs/setup/run-locally">quickstart</AppLink> or join the{" "}
              <AstryxLink href="https://discord.gg/RN9UKh52be">Jazz Discord</AstryxLink>.
            </Text>
          </div>
        </div>
      </section>
    </div>
  );
}
