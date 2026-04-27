import express, { type ErrorRequestHandler, type Express } from "express";
import { verifyLocalFirstIdentityProof } from "jazz-napi";
import { openBackendRepository as defaultOpenBackendRepository } from "../db.js";
import type { VerifiedUser } from "../repository.js";
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
