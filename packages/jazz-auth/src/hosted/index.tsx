import type * as React from "react";
import * as LabelPrimitive from "@radix-ui/react-label";
import { Slot } from "@radix-ui/react-slot";
import { memoryAdapter, type MemoryDB } from "better-auth/adapters/memory";
import { betterAuth } from "better-auth";
import { nextCookies, toNextJsHandler } from "better-auth/next-js";
import { jwt } from "better-auth/plugins";
import { cva, type VariantProps } from "class-variance-authority";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50",
  {
    defaultVariants: {
      size: "default",
      variant: "default",
    },
    variants: {
      size: {
        default: "h-11",
        sm: "h-9 px-3 text-xs",
      },
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/90",
        ghost: "bg-transparent text-foreground hover:bg-accent hover:text-accent-foreground",
        outline: "border border-border bg-background text-foreground hover:bg-accent",
      },
    },
  },
);

function Button({
  asChild = false,
  className,
  size,
  variant,
  ...props
}: React.ComponentProps<"button"> &
  VariantProps<typeof buttonVariants> & {
    asChild?: boolean;
  }) {
  const Comp = asChild ? Slot : "button";

  return <Comp className={cn(buttonVariants({ className, size, variant }))} {...props} />;
}

function Card({ className, ...props }: React.ComponentProps<"section">) {
  return (
    <section
      className={cn(
        "rounded-[28px] border border-border/70 bg-card/90 shadow-[0_32px_90px_-48px_rgba(15,23,42,0.55)] backdrop-blur",
        className,
      )}
      {...props}
    />
  );
}

function CardHeader({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("space-y-2 p-8 pb-4", className)} {...props} />;
}

function CardTitle({ className, ...props }: React.ComponentProps<"h1">) {
  return (
    <h1
      className={cn("text-3xl font-semibold tracking-tight text-foreground", className)}
      {...props}
    />
  );
}

function CardDescription({ className, ...props }: React.ComponentProps<"p">) {
  return <p className={cn("text-sm leading-6 text-muted-foreground", className)} {...props} />;
}

function CardContent({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("space-y-6 p-8 pt-2", className)} {...props} />;
}

function Input({ className, ...props }: React.ComponentProps<"input">) {
  return (
    <input
      className={cn(
        "flex h-11 w-full rounded-xl border border-border bg-background px-4 py-2 text-sm text-foreground shadow-sm outline-none transition focus:border-primary focus:ring-4 focus:ring-primary/10",
        className,
      )}
      {...props}
    />
  );
}

function Label({ className, ...props }: React.ComponentProps<typeof LabelPrimitive.Root>) {
  return (
    <LabelPrimitive.Root
      className={cn("text-sm font-medium text-foreground", className)}
      {...props}
    />
  );
}

function AuthShell({
  badge,
  brandName,
  children,
  description,
  title,
}: {
  badge?: string;
  brandName: string;
  children: React.ReactNode;
  description: string;
  title: string;
}) {
  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(20,184,166,0.18),_transparent_32%),linear-gradient(180deg,_rgba(255,255,255,0.96),_rgba(244,247,245,0.98))] px-6 py-12 text-foreground">
      <div className="mx-auto flex min-h-[calc(100vh-6rem)] max-w-5xl items-center justify-center">
        <div className="grid w-full gap-8 lg:grid-cols-[1.1fr_0.9fr]">
          <section className="hidden rounded-[32px] border border-white/60 bg-white/75 p-10 shadow-[0_28px_70px_-50px_rgba(15,23,42,0.45)] backdrop-blur lg:flex lg:flex-col lg:justify-between">
            <div className="space-y-5">
              <span className="inline-flex w-fit rounded-full border border-primary/15 bg-primary/8 px-4 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-primary">
                {badge ?? "Hosted auth"}
              </span>
              <h2 className="max-w-sm text-5xl font-semibold tracking-tight text-balance text-slate-900">
                {brandName}
              </h2>
              <p className="max-w-md text-base leading-7 text-slate-600">
                Jazz-hosted auth for local-first apps. Keep your product UI in your app, and hand
                off only the credential boundary.
              </p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-2xl border border-border/70 bg-background/85 p-4">
                <p className="text-sm font-medium text-foreground">Redirect-first UX</p>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">
                  Your app keeps the button design and simply opens the hosted auth flow when it
                  matters.
                </p>
              </div>
              <div className="rounded-2xl border border-border/70 bg-background/85 p-4">
                <p className="text-sm font-medium text-foreground">Stable Jazz principal</p>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">
                  JWTs carry a single Jazz principal id so the sync server only needs JWKS
                  verification.
                </p>
              </div>
            </div>
          </section>

          <Card className="overflow-hidden">
            <CardHeader>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-primary">
                {brandName}
              </p>
              <CardTitle>{title}</CardTitle>
              <CardDescription>{description}</CardDescription>
            </CardHeader>
            <CardContent>{children}</CardContent>
          </Card>
        </div>
      </div>
    </main>
  );
}

function InlineError({ error }: { error?: string | null }) {
  if (!error) {
    return null;
  }

  return (
    <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {error}
    </div>
  );
}

export interface JazzHostedPageProps {
  action: string;
  brandName?: string;
  error?: string | null;
  redirectTo?: string | null;
  signInHref?: string;
  signUpHref?: string;
}

function buildHostedNavHref(path: string, redirectTo?: string | null): string {
  if (!redirectTo) {
    return path;
  }

  const url = new URL(path, "https://jazz-auth.local");
  url.searchParams.set("redirectTo", redirectTo);
  return `${url.pathname}${url.search}`;
}

export function JazzHostedSignInPage({
  action,
  brandName = "Jazz Auth",
  error,
  redirectTo,
  signUpHref,
}: JazzHostedPageProps) {
  return (
    <AuthShell
      badge="Sign in"
      brandName={brandName}
      description="Use the hosted Jazz Auth flow, then redirect back to your app with a fresh session."
      title="Sign in"
    >
      <form action={action} className="space-y-5" method="post">
        <InlineError error={error} />
        <input name="redirectTo" type="hidden" value={redirectTo ?? ""} />
        <div className="space-y-2">
          <Label htmlFor="email">Email</Label>
          <Input
            autoComplete="email"
            id="email"
            name="email"
            placeholder="alice@example.com"
            required
            type="email"
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="password">Password</Label>
          <Input
            autoComplete="current-password"
            id="password"
            name="password"
            required
            type="password"
          />
        </div>
        <Button className="w-full" type="submit">
          Continue
        </Button>
      </form>
      <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
        <span>Need an account?</span>
        <Button asChild size="sm" variant="ghost">
          <a href={signUpHref ?? buildHostedNavHref("/auth/sign-up", redirectTo)}>Create one</a>
        </Button>
      </div>
    </AuthShell>
  );
}

export function JazzHostedSignUpPage({
  action,
  brandName = "Jazz Auth",
  error,
  redirectTo,
  signInHref,
}: JazzHostedPageProps) {
  return (
    <AuthShell
      badge="Create account"
      brandName={brandName}
      description="New accounts can start syncing immediately after the hosted flow completes."
      title="Create your account"
    >
      <form action={action} className="space-y-5" method="post">
        <InlineError error={error} />
        <input name="redirectTo" type="hidden" value={redirectTo ?? ""} />
        <div className="space-y-2">
          <Label htmlFor="name">Display name</Label>
          <Input
            autoComplete="name"
            id="name"
            name="name"
            placeholder="Alice"
            required
            type="text"
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="email">Email</Label>
          <Input
            autoComplete="email"
            id="email"
            name="email"
            placeholder="alice@example.com"
            required
            type="email"
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="password">Password</Label>
          <Input
            autoComplete="new-password"
            id="password"
            minLength={8}
            name="password"
            required
            type="password"
          />
        </div>
        <Button className="w-full" type="submit">
          Create account
        </Button>
      </form>
      <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
        <span>Already have an account?</span>
        <Button asChild size="sm" variant="ghost">
          <a href={signInHref ?? buildHostedNavHref("/auth/sign-in", redirectTo)}>Sign in</a>
        </Button>
      </div>
    </AuthShell>
  );
}

export interface JazzHostedAuthOptions {
  apiBasePath?: string;
  baseURL: string;
  database?: Parameters<typeof betterAuth>[0]["database"];
  emailAndPassword?: Parameters<typeof betterAuth>[0]["emailAndPassword"];
  hostedBasePath?: string;
  issuer?: string;
  secret: string;
  trustedOrigins?: string[];
  useNextCookies?: boolean;
}

export interface JazzHostedAuth {
  apiBasePath: string;
  auth: ReturnType<typeof betterAuth>;
  handlers: ReturnType<typeof toNextJsHandler>;
  hostedBasePath: string;
}

export function getJazzHostedAuthHandlers(hosted: JazzHostedAuth): JazzHostedAuth["handlers"] {
  return hosted.handlers;
}

function createMemoryDatabase(): MemoryDB {
  return {
    account: [],
    jwks: [],
    rateLimit: [],
    session: [],
    user: [],
    verification: [],
  };
}

function resolveJazzPrincipalId(user: Record<string, unknown>): string {
  const principalId =
    (typeof user.jazzPrincipalId === "string" && user.jazzPrincipalId) ||
    (typeof user.principalId === "string" && user.principalId) ||
    (typeof user.id === "string" && user.id);

  if (!principalId) {
    throw new Error("Jazz Auth expected a stable principal id on the Better Auth user record.");
  }

  return principalId;
}

export function createJazzHostedAuth(options: JazzHostedAuthOptions): JazzHostedAuth {
  const apiBasePath = options.apiBasePath ?? "/api/auth";
  const hostedBasePath = options.hostedBasePath ?? "/auth";
  const plugins = [];

  if (options.useNextCookies !== false) {
    plugins.push(nextCookies());
  }

  plugins.push(
    jwt({
      jwks: {
        keyPairConfig: {
          alg: "ES256",
        },
      },
      jwt: {
        definePayload: ({ user }) => ({
          email: user.email,
          jazz_principal_id: resolveJazzPrincipalId(user as Record<string, unknown>),
          name: user.name,
        }),
        expirationTime: "15m",
        getSubject: ({ user }) => resolveJazzPrincipalId(user as Record<string, unknown>),
        issuer: options.issuer ?? options.baseURL,
      },
    }),
  );

  const auth = betterAuth({
    basePath: apiBasePath,
    baseURL: options.baseURL,
    database: options.database ?? memoryAdapter(createMemoryDatabase()),
    emailAndPassword: {
      autoSignIn: true,
      enabled: true,
      minPasswordLength: 8,
      requireEmailVerification: false,
      ...options.emailAndPassword,
    },
    plugins,
    secret: options.secret,
    trustedOrigins: options.trustedOrigins ?? [options.baseURL],
  }) as unknown as ReturnType<typeof betterAuth>;

  return {
    apiBasePath,
    auth,
    handlers: toNextJsHandler(auth),
    hostedBasePath,
  };
}

function copySetCookieHeaders(source: Headers, target: Headers): void {
  const setCookies =
    typeof source.getSetCookie === "function"
      ? source.getSetCookie()
      : source.get("set-cookie")
        ? [source.get("set-cookie")!]
        : [];

  if (setCookies.length === 0) {
    return;
  }

  target.delete("set-cookie");
  for (const setCookie of setCookies) {
    target.append("set-cookie", setCookie);
  }
}

function redirectResponse(location: URL | string): Response {
  return new Response(null, {
    headers: {
      location: String(location),
    },
    status: 303,
  });
}

function withRedirectTo(url: URL, redirectTo?: string | null, error?: string | null): URL {
  if (redirectTo) {
    url.searchParams.set("redirectTo", redirectTo);
  }
  if (error) {
    url.searchParams.set("error", error);
  }
  return url;
}

async function readErrorMessage(response: Response): Promise<string | null> {
  try {
    const payload = (await response.json()) as { error?: string; message?: string };
    return payload.message ?? payload.error ?? null;
  } catch {
    return null;
  }
}

async function finalizeAuthRedirect(
  authResponse: Response,
  request: Request,
  redirectTo?: string | null,
): Promise<Response> {
  let nextLocation = redirectTo ?? "/";

  try {
    const payload = (await authResponse.clone().json()) as { url?: string; redirect?: boolean };
    if (payload.redirect && payload.url) {
      nextLocation = payload.url;
    }
  } catch {}

  const response = redirectResponse(new URL(nextLocation, request.url));
  copySetCookieHeaders(authResponse.headers, response.headers);
  return response;
}

async function forwardJsonRequest(
  hosted: JazzHostedAuth,
  request: Request,
  endpoint: string,
  body: Record<string, unknown>,
): Promise<Response> {
  const headers = new Headers();
  request.headers.forEach((value, key) => {
    if (key.toLowerCase() === "content-length") {
      return;
    }
    headers.set(key, value);
  });
  headers.set("accept", "application/json");
  headers.set("content-type", "application/json");

  return hosted.auth.handler(
    new Request(new URL(`${hosted.apiBasePath}${endpoint}`, request.url), {
      body: JSON.stringify(body),
      headers,
      method: "POST",
    }),
  );
}

export async function handleJazzHostedSignIn(
  hosted: JazzHostedAuth,
  request: Request,
): Promise<Response> {
  const formData = await request.formData();
  const redirectTo = String(formData.get("redirectTo") ?? "") || "/";
  const authResponse = await forwardJsonRequest(hosted, request, "/sign-in/email", {
    callbackURL: redirectTo,
    email: String(formData.get("email") ?? ""),
    password: String(formData.get("password") ?? ""),
    rememberMe: true,
  });

  if (!authResponse.ok) {
    return redirectResponse(
      withRedirectTo(
        new URL(`${hosted.hostedBasePath}/sign-in`, request.url),
        redirectTo,
        await readErrorMessage(authResponse),
      ),
    );
  }

  return finalizeAuthRedirect(authResponse, request, redirectTo);
}

export async function handleJazzHostedSignUp(
  hosted: JazzHostedAuth,
  request: Request,
): Promise<Response> {
  const formData = await request.formData();
  const redirectTo = String(formData.get("redirectTo") ?? "") || "/";
  const authResponse = await forwardJsonRequest(hosted, request, "/sign-up/email", {
    callbackURL: redirectTo,
    email: String(formData.get("email") ?? ""),
    name: String(formData.get("name") ?? ""),
    password: String(formData.get("password") ?? ""),
    rememberMe: true,
  });

  if (!authResponse.ok) {
    return redirectResponse(
      withRedirectTo(
        new URL(`${hosted.hostedBasePath}/sign-up`, request.url),
        redirectTo,
        await readErrorMessage(authResponse),
      ),
    );
  }

  return finalizeAuthRedirect(authResponse, request, redirectTo);
}

export async function handleJazzHostedSignOut(
  hosted: JazzHostedAuth,
  request: Request,
): Promise<Response> {
  const requestUrl = new URL(request.url);
  const redirectTo = requestUrl.searchParams.get("redirectTo") ?? "/";
  const authResponse = await hosted.auth.handler(
    new Request(new URL(`${hosted.apiBasePath}/sign-out`, request.url), {
      headers: request.headers,
      method: "POST",
    }),
  );

  if (!authResponse.ok) {
    return redirectResponse(
      withRedirectTo(new URL(`${hosted.hostedBasePath}/sign-in`, request.url), redirectTo),
    );
  }

  return finalizeAuthRedirect(authResponse, request, redirectTo);
}

export {
  startJazzHostedAuthServer,
  type JazzHostedAuthServerHandle,
  type JazzHostedAuthServerOptions,
} from "./server.js";
