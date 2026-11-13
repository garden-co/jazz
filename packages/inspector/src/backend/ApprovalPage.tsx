// Where a person approves an agent token that `jazz-tools inspect login` or
// the MCP server's `request_access` asked for. The request rides in the URL
// fragment (`#grant=`); the person is already signed in through the app's
// `inspect` hook, so the token can be no stronger than their own access.
//
// The token goes back by itself only to a loopback URL on this computer
// (`isLoopbackCallback`), which is where the asking terminal listens.
// Anything else and the page shows the token to paste instead.

import {
  cleanTokenLabel,
  isLoopbackCallback,
  type InspectGrantRequest,
  type InspectWhoami,
} from "jazz-tools/inspect";
import { useState } from "react";
import appStyles from "../App.module.css";
import type { InspectorCall } from "./api.js";
import styles from "./backend.module.css";

function describeTtl(seconds: number): string {
  if (seconds % 86400 === 0) return `${seconds / 86400} day${seconds === 86400 ? "" : "s"}`;
  if (seconds % 3600 === 0) return `${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`;
  if (seconds % 60 === 0) return `${seconds / 60} minutes`;
  return `${seconds} seconds`;
}

type Step =
  | { kind: "ask" }
  | { kind: "minting" }
  | { kind: "handing-over" }
  | { kind: "paste"; token: string; expiresAt: number }
  | { kind: "denied" }
  | { kind: "failed"; message: string };

/** Where the app is, as a person recognizes it: the host its Jazz endpoint is at. */
function appHost(endpoint: string): string {
  try {
    return new URL(endpoint).host;
  } catch {
    return endpoint;
  }
}

export function ApprovalPage({
  grant,
  whoami,
  call,
  endpoint,
  navigate = (url) => window.location.assign(url),
  onDone,
}: {
  grant: InspectGrantRequest;
  whoami: InspectWhoami;
  call: InspectorCall;
  /** The app's Jazz URL, for the `--app` of the paste command. */
  endpoint: string;
  navigate?: (url: string) => void;
  onDone?: () => void;
}) {
  const [step, setStep] = useState<Step>({ kind: "ask" });
  const write = grant.write === true;
  const ttl = grant.ttlSeconds ?? 3600;
  const loopback = grant.callback !== undefined && isLoopbackCallback(grant.callback);
  const cannotWrite = write && !whoami.write;
  // Shown as the token will keep it.
  const label = cleanTokenLabel(grant.label);
  // Checked here, before anyone approves: the backend would refuse to mint it.
  const maxTtl = whoami.maxTokenTtlSeconds;
  const badTtl =
    !Number.isSafeInteger(ttl) || ttl < 60
      ? "This request asks for a token that lasts less than a minute, which this app doesn't issue."
      : maxTtl !== undefined && ttl > maxTtl
        ? `This request asks for a token that lasts ${describeTtl(ttl)}, but this app's tokens last at most ${describeTtl(maxTtl)}. Ask again with a shorter --ttl.`
        : null;

  const approve = async () => {
    setStep({ kind: "minting" });
    try {
      const minted = await call<{ token: string; expiresAt: number }>({
        op: "mint_token",
        write,
        ttlSeconds: ttl,
        ...(grant.tables ? { tables: grant.tables } : {}),
        ...(label ? { label } : {}),
      });
      if (loopback && grant.callback) {
        setStep({ kind: "handing-over" });
        const url = new URL(grant.callback);
        url.searchParams.set("state", grant.state);
        url.searchParams.set("token", minted.token);
        navigate(url.toString());
        return;
      }
      setStep({ kind: "paste", token: minted.token, expiresAt: minted.expiresAt });
    } catch (error) {
      setStep({ kind: "failed", message: error instanceof Error ? error.message : String(error) });
    }
  };

  return (
    <main className={appStyles.statePage}>
      <section className={appStyles.stateCard} aria-labelledby="jazz-approve-title">
        <h2 id="jazz-approve-title" className={appStyles.stateTitle}>
          Approve agent access
        </h2>
        {step.kind === "ask" || step.kind === "minting" ? (
          <>
            <ul className={styles.grantList}>
              <li>
                <strong>{label || "An agent"}</strong> asks for{" "}
                <strong>{write ? "read and write" : "read-only"}</strong> access to the app at{" "}
                <strong>{appHost(endpoint)}</strong>{" "}
                <span className={styles.note}>(app id {whoami.appId})</span>.
              </li>
              <li>Tables: {grant.tables?.length ? grant.tables.join(", ") : "all"}</li>
              <li>For {describeTtl(ttl)}, then the token stops working.</li>
              <li>Approved by: {whoami.by}</li>
            </ul>
            <p className={styles.warning}>
              Approve only a request you started yourself, in a terminal or agent you trust. The
              agent can then read every row of{" "}
              {grant.tables?.length ? "those tables" : "every table"}
              {write ? " and change or delete any of them" : ""}, permissions not checked, until the
              token expires. Every call it makes is logged by the app&apos;s backend.
            </p>
            <p className={styles.note}>
              {loopback
                ? `Approving hands the token to the terminal on this computer that asked (port ${new URL(grant.callback!).port}).`
                : "This request has no terminal on this computer to hand the token to, so you will get a token to paste where the agent runs."}
            </p>
            {cannotWrite ? (
              <p className={appStyles.errorText}>
                Your own access is read-only, so you can't approve write access.
              </p>
            ) : null}
            {badTtl ? (
              <p className={appStyles.errorText} role="alert">
                {badTtl}
              </p>
            ) : null}
            <div className={appStyles.actionRow}>
              <button
                type="button"
                className={appStyles.actionButton}
                disabled={cannotWrite || badTtl !== null || step.kind === "minting"}
                onClick={() => void approve()}
              >
                {step.kind === "minting" ? "Approving…" : "Approve"}
              </button>
              <button
                type="button"
                className={appStyles.actionButtonSecondary}
                disabled={step.kind === "minting"}
                onClick={() => setStep({ kind: "denied" })}
              >
                Deny
              </button>
            </div>
          </>
        ) : null}
        {step.kind === "handing-over" ? (
          <p className={styles.note}>Handing the token to your terminal…</p>
        ) : null}
        {step.kind === "paste" ? (
          <>
            <p className={styles.note}>
              Approved. Run this where the agent runs. The token works until{" "}
              {new Date(step.expiresAt * 1000).toLocaleString()}; anyone who has it gets this
              access, so don&apos;t share it further.
            </p>
            <pre className={styles.code}>
              {`npx jazz-tools inspect login --app ${endpoint} --token ${step.token}`}
            </pre>
          </>
        ) : null}
        {step.kind === "denied" ? (
          <>
            <p className={styles.note}>Denied. Nothing was granted.</p>
            {onDone ? (
              <button type="button" className={appStyles.actionButton} onClick={onDone}>
                Open the Inspector
              </button>
            ) : null}
          </>
        ) : null}
        {step.kind === "failed" ? <p className={appStyles.errorText}>{step.message}</p> : null}
      </section>
    </main>
  );
}
