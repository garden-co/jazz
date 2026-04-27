import express, { type ErrorRequestHandler, type Express } from "express";
import { join } from "node:path";
import { verifyLocalFirstIdentityProof } from "jazz-napi";
import {
  openBackendRepository as defaultOpenBackendRepository,
  openRepository as defaultOpenRepository,
} from "../db.js";
import { exportMarkdownTodo as defaultExportMarkdownTodo } from "../domain/markdown.js";
import type {
  IssueItem,
  ItemStatus,
  ListedItem,
  ListFilters,
  VerifiedUser,
} from "../repository.js";
import {
  exchangeDeviceCode as defaultExchangeDeviceCode,
  fetchGitHubUser as defaultFetchGitHubUser,
  type GitHubUser,
} from "./github.js";

class VerifierHttpError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
  ) {
    super(message);
  }
}

export interface VerifierRepository {
  upsertVerifiedUser(user: VerifiedUser): Promise<unknown>;
}

export interface VerifierDependencies {
  github?: {
    exchangeDeviceCode?: (deviceCode: string) => Promise<{ accessToken: string }>;
    fetchUser?: (accessToken: string) => Promise<GitHubUser>;
  };
  verifyJazzProof?: (proof: string) => Promise<{ jazzUserId: string }>;
  openBackendRepository?: () => Promise<VerifierRepository>;
}

export interface SkillIssuesRepository {
  upsertItem(item: IssueItem): Promise<unknown>;
  listItems(filters?: ListFilters): Promise<ListedItem[]>;
  assignMe(slug: string): Promise<unknown>;
  setStatus(slug: string, status: ItemStatus): Promise<unknown>;
}

export interface SkillIssuesServerDependencies {
  openRepository?: () => Promise<SkillIssuesRepository>;
  exportMarkdownTodo?: typeof defaultExportMarkdownTodo;
  cwd?: string;
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isItemKind(value: unknown): value is IssueItem["kind"] {
  return value === "idea" || value === "issue";
}

function isItemStatus(value: unknown): value is ItemStatus {
  return value === "open" || value === "in_progress" || value === "done";
}

function requireString(record: Record<string, unknown>, name: string): string {
  const value = record[name];
  if (typeof value !== "string" || !value) {
    throw new VerifierHttpError(400, `${name} is required`);
  }
  return value;
}

function isMalformedJsonError(error: unknown): boolean {
  return error instanceof SyntaxError && isRecord(error) && error.status === 400 && "body" in error;
}

async function verifyJazzProof(proof: string): Promise<{ jazzUserId: string }> {
  const result = verifyLocalFirstIdentityProof(proof, "skill-issues-github");
  if (!result.ok) {
    throw new VerifierHttpError(401, "Invalid Jazz identity proof");
  }
  return { jazzUserId: result.id };
}

export function createVerifierApp(deps: VerifierDependencies = {}): Express {
  const app = express();
  const exchangeDeviceCode =
    deps.github?.exchangeDeviceCode ??
    ((deviceCode: string) =>
      defaultExchangeDeviceCode({
        clientId: requiredEnv("GITHUB_CLIENT_ID"),
        deviceCode,
      }));
  const fetchUser = deps.github?.fetchUser ?? defaultFetchGitHubUser;
  const proofVerifier = deps.verifyJazzProof ?? verifyJazzProof;
  const openBackendRepository = deps.openBackendRepository ?? defaultOpenBackendRepository;

  app.use(express.json());

  app.post("/auth/github/complete", async (request, response, next) => {
    try {
      const body: unknown = request.body;
      const deviceCode = isRecord(body) ? body.deviceCode : undefined;
      const jazzProof = isRecord(body) ? body.jazzProof : undefined;

      if (
        typeof deviceCode !== "string" ||
        !deviceCode ||
        typeof jazzProof !== "string" ||
        !jazzProof
      ) {
        response.status(400).json({ error: "deviceCode and jazzProof are required" });
        return;
      }

      let proof: { jazzUserId: string };
      try {
        proof = await proofVerifier(jazzProof);
      } catch {
        throw new VerifierHttpError(401, "Invalid Jazz identity proof");
      }

      let accessToken: string;
      try {
        ({ accessToken } = await exchangeDeviceCode(deviceCode));
      } catch {
        throw new VerifierHttpError(400, "GitHub authorization is not complete");
      }

      const githubUser = await fetchUser(accessToken);
      const repo = await openBackendRepository();
      const verifiedUser: VerifiedUser = {
        id: proof.jazzUserId,
        githubUserId: githubUser.id,
        githubLogin: githubUser.login,
        verifiedAt: new Date().toISOString(),
      };

      await repo.upsertVerifiedUser(verifiedUser);

      response.json({
        id: verifiedUser.id,
        githubLogin: verifiedUser.githubLogin,
      });
    } catch (error) {
      next(error);
    }
  });

  const errorHandler: ErrorRequestHandler = (error, _request, response, _next) => {
    if (error instanceof VerifierHttpError) {
      response.status(error.statusCode).json({ error: error.message });
      return;
    }

    response.status(500).json({ error: "Verifier request failed" });
  };
  app.use(errorHandler);

  return app;
}

export function createSkillIssuesServer(deps: SkillIssuesServerDependencies = {}): Express {
  const app = express();
  const openRepository =
    deps.openRepository ?? (() => defaultOpenRepository(process.cwd(), process.env));
  const exportMarkdownTodo = deps.exportMarkdownTodo ?? defaultExportMarkdownTodo;
  const cwd = deps.cwd ?? process.cwd();

  app.use("/api", express.json());

  app.get("/api/items", async (_request, response, next) => {
    try {
      const repo = await openRepository();
      response.json(await repo.listItems({}));
    } catch (error) {
      next(error);
    }
  });

  app.post("/api/items", async (request, response, next) => {
    try {
      const body: unknown = request.body;
      if (!isRecord(body) || !isItemKind(body.kind)) {
        throw new VerifierHttpError(400, "kind is required");
      }

      const item: IssueItem = {
        kind: body.kind,
        slug: requireString(body, "slug"),
        title: requireString(body, "title"),
        description: requireString(body, "description"),
      };
      const repo = await openRepository();
      await repo.upsertItem(item);
      response.status(201).json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  app.post("/api/items/:slug/assign-me", async (request, response, next) => {
    try {
      const repo = await openRepository();
      await repo.assignMe(request.params.slug);
      response.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  app.post("/api/items/:slug/status", async (request, response, next) => {
    try {
      const body: unknown = request.body;
      if (!isRecord(body) || !isItemStatus(body.status)) {
        throw new VerifierHttpError(400, "status is required");
      }

      const repo = await openRepository();
      await repo.setStatus(request.params.slug, body.status);
      response.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  app.post("/api/export", async (_request, response, next) => {
    try {
      const repo = await openRepository();
      const items = await repo.listItems({});
      const plainItems: IssueItem[] = items.map(({ kind, slug, title, description }) => ({
        kind,
        slug,
        title,
        description,
      }));
      await exportMarkdownTodo(join(cwd, "todo"), plainItems);
      response.json({ ok: true });
    } catch (error) {
      next(error);
    }
  });

  const errorHandler: ErrorRequestHandler = (error, _request, response, _next) => {
    if (isMalformedJsonError(error)) {
      response.status(400).json({ error: "Malformed JSON" });
      return;
    }

    if (error instanceof VerifierHttpError) {
      response.status(error.statusCode).json({ error: error.message });
      return;
    }

    response.status(500).json({ error: "Skill issues request failed" });
  };
  app.use(errorHandler);
  app.use(createVerifierApp());

  return app;
}
