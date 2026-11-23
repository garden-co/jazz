import { useEffect, useState } from "react";
import {
  Badge,
  Banner,
  Button,
  Card,
  Heading,
  HStack,
  List,
  ListItem,
  Text,
  TextArea,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import {
  failureOf,
  useDeviceState,
  useDeviceStateIfAny,
  type DeviceInfo,
  type EncryptionHandle,
  type Failure,
} from "../lib/device-state";
import { endOtherSignIns } from "../lib/end-other-sign-ins";
import { clearLeaveRecord, writeLeaveRecord } from "../lib/leave-record";

/** How BandChat names itself on the recovery passkey. */
const APP_NAME = "BandChat";

/**
 * This browser's devices and recovery: its status, approving a new browser,
 * removing a lost device, leaving this browser, and recovery.
 */
export function DevicesPanel({ accountName }: { accountName: string }) {
  // Only where the app watches this browser's devices (see App.tsx).
  return useDeviceStateIfAny() ? <DevicesAndRecovery accountName={accountName} /> : null;
}

function DevicesAndRecovery({ accountName }: { accountName: string }) {
  const state = useDeviceState();
  return (
    <VStack gap={4} padding={4}>
      <Heading level={2}>Devices and recovery</Heading>
      <ThisBrowserStatus />
      {state.status === "active" ? (
        <>
          <DeviceList />
          <RecoverySetup accountName={accountName} />
        </>
      ) : state.status !== null ? (
        <RestoreAccess />
      ) : null}
    </VStack>
  );
}

function ThisBrowserStatus() {
  const { status, leaving, failure, readyFailed, retryReady } = useDeviceState();
  return (
    <VStack gap={2}>
      {leaving === "pending" ? (
        <Banner
          status="info"
          container="section"
          collapsible={false}
          title="Removal pending"
          description="Waiting for another of your devices to remove this browser. Once one is online it does so by itself; then this browser's keys are erased and it signs out. You can keep using BandChat here until then."
        />
      ) : leaving === "unfinished" ? null : status === "awaiting-approval" ||
        status === "reenrolled" ? (
        <Banner
          status="warning"
          container="section"
          collapsible={false}
          title="This browser is waiting for approval"
          description="Messages stay locked here until another of your devices approves this browser, or you restore access with your recovery phrase or passkey."
        />
      ) : null}
      {readyFailed ? (
        <Banner
          status="error"
          container="section"
          collapsible={false}
          title="This browser could not check its keys"
          description={readyFailed.message}
          endContent={<Button label="Check again" onClick={() => void retryReady()} />}
        />
      ) : null}
      {failure && leaving !== "unfinished" ? (
        <Banner
          status="error"
          container="section"
          collapsible={false}
          title="Could not read this browser's devices"
          description={failure.message}
        />
      ) : null}
    </VStack>
  );
}

function shortId(id: string): string {
  return id.length > 10 ? `${id.slice(0, 10)}…` : id;
}

function DeviceList() {
  const { devices } = useDeviceState();
  if (!devices) return <Text color="secondary">Loading devices…</Text>;
  return (
    <VStack gap={2}>
      <Heading level={3}>Your devices</Heading>
      <List>
        {devices.map((device) => (
          <DeviceRow key={device.id} device={device} />
        ))}
      </List>
    </VStack>
  );
}

function DeviceRow({ device }: { device: DeviceInfo }) {
  const { leaving } = useDeviceState();
  const label = device.isThisDevice ? "This browser" : `Device ${shortId(device.id)}`;
  const badge =
    device.isThisDevice && leaving === "pending" ? (
      <Badge variant="warning" label="Removal pending" />
    ) : device.status === "waiting" ? (
      <Badge variant="warning" label="Waiting for approval" />
    ) : (
      <Badge variant="success" label="Active" />
    );
  return (
    <ListItem
      data-device-id={device.id}
      label={label}
      description={
        <VStack gap={2}>
          <HStack gap={2}>{badge}</HStack>
          {device.isThisDevice ? (
            leaving === "none" ? (
              <LeaveThisBrowser device={device} />
            ) : null
          ) : device.status === "active" ? (
            // A device that is not active cannot be removed, and the
            // sign-outs that come first would be for nothing.
            <RemoveLostDevice device={device} />
          ) : (
            <ApproveDevice device={device} />
          )}
        </VStack>
      }
    />
  );
}

function Confirm({
  title,
  description,
  actionLabel,
  isBusy,
  onConfirm,
  onCancel,
}: {
  title: string;
  description: string;
  actionLabel: string;
  isBusy: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <Card width="100%">
      <VStack gap={2} role="alertdialog" aria-label={title}>
        <Text weight="semibold">{title}</Text>
        <Text>{description}</Text>
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onCancel} isDisabled={isBusy} />
          <Button
            label={actionLabel}
            variant="destructive"
            isLoading={isBusy}
            onClick={onConfirm}
          />
        </HStack>
      </VStack>
    </Card>
  );
}

function ErrorText({ children }: { children: string }) {
  return (
    <span role="alert">
      <Banner status="error" container="section" collapsible={false} title={children} />
    </span>
  );
}

/** Leaving is a removal request that another of the user's devices completes. */
function LeaveThisBrowser({ device }: { device: DeviceInfo }) {
  const { encryption, accountId, refresh, reloadLeaveRecord } = useDeviceState();
  const [isConfirming, setConfirming] = useState(false);
  const [isBusy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function leave() {
    setBusy(true);
    setError(null);
    try {
      // A tab can close before the authority's answer reaches it. Keep the
      // intent so the next page can sign out after Jazz changes its device ID.
      writeLeaveRecord({ accountId, deviceId: device.id });
      reloadLeaveRecord();
      await encryption.devices.requestRemoval();
    } catch (cause) {
      const failed = failureOf(cause);
      if (
        failed.code === "no-other-device" ||
        failed.code === "device-not-active" ||
        (failed.code === "database" && failed.message.startsWith("WriteRejected:"))
      ) {
        clearLeaveRecord();
        reloadLeaveRecord();
      }
      setError(
        failed.code === "no-other-device"
          ? "This is your only device, so it can't leave: no other device would hold your keys. Nothing was removed, and your messages stay readable here."
          : `Could not ask to leave: ${failed.message}`,
      );
    } finally {
      setConfirming(false);
      setBusy(false);
      void refresh();
    }
  }

  return (
    <VStack gap={2}>
      {isConfirming ? (
        <Confirm
          title="Leave this browser?"
          description="Another of your devices removes this browser the next time it's online. Then this browser's keys for your account are erased and it signs out; until then you can keep using it. Your other devices will then list this browser as a new device waiting for approval: ignore it, or approve it to let this browser back in."
          actionLabel="Leave this browser"
          isBusy={isBusy}
          onConfirm={() => void leave()}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <HStack>
          <Button label="Leave this browser" size="sm" onClick={() => setConfirming(true)} />
        </HStack>
      )}
      {error ? <ErrorText>{error}</ErrorText> : null}
    </VStack>
  );
}

/**
 * Removing a device the user no longer controls. Every other sign-in of the
 * user ends first, so no open connection of that device stays signed in;
 * then the device is removed. Each attempt runs both steps.
 */
function RemoveLostDevice({ device }: { device: DeviceInfo }) {
  const { encryption, refresh } = useDeviceState();
  const [isConfirming, setConfirming] = useState(false);
  const [isBusy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function remove() {
    setBusy(true);
    setError(null);
    try {
      try {
        await endOtherSignIns();
      } catch (cause) {
        setError(
          `Sign-out could not be confirmed. Your other browsers may have been signed out. ${failureOf(cause).message} Try again to finish removing the device.`,
        );
        return;
      }
      try {
        await encryption.devices.remove(device.id);
      } catch (cause) {
        setError(
          `Your other browsers were signed out, but the device was not removed: ${failureOf(cause).message} Remove it again to finish.`,
        );
        return;
      }
      void refresh();
    } finally {
      setBusy(false);
      setConfirming(false);
    }
  }

  return (
    <VStack gap={2}>
      {isConfirming ? (
        <Confirm
          title="Remove this lost device?"
          description="Every other browser where you're signed in to BandChat is signed out first and has to sign in again. They keep their keys and need no new approval, but changes they haven't synced yet may be lost. The removed device gets no new keys; it keeps the messages it already has."
          actionLabel="Sign out other browsers and remove"
          isBusy={isBusy}
          onConfirm={() => void remove()}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <HStack>
          <Button
            label="Remove lost device"
            size="sm"
            variant="destructive"
            onClick={() => setConfirming(true)}
          />
        </HStack>
      )}
      {error ? <ErrorText>{error}</ErrorText> : null}
    </VStack>
  );
}

type ApprovalSession = Awaited<ReturnType<EncryptionHandle["devices"]["challenge"]>>;

/** Approving a waiting browser: challenge it, then type the code it shows. */
function ApproveDevice({ device }: { device: DeviceInfo }) {
  const { encryption, refresh } = useDeviceState();
  const [session, setSession] = useState<ApprovalSession | null>(null);
  const [answered, setAnswered] = useState(false);
  const [code, setCode] = useState("");
  const [isBusy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // The new browser answers on its own screen; wait for its answer.
  useEffect(() => {
    if (!session || answered) return;
    let stopped = false;
    const look = async () => {
      try {
        if ((await session.code()) !== null && !stopped) setAnswered(true);
      } catch (cause) {
        if (!stopped) setError(failureOf(cause).message);
      }
    };
    void look();
    const timer = setInterval(() => void look(), 1500);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [session, answered]);

  async function start() {
    setBusy(true);
    setError(null);
    setAnswered(false);
    setCode("");
    try {
      setSession(await encryption.devices.challenge(device.id));
    } catch (cause) {
      setError(`Could not start the approval: ${failureOf(cause).message}`);
    } finally {
      setBusy(false);
    }
  }

  async function approve() {
    if (!session) return;
    setBusy(true);
    setError(null);
    try {
      await session.approve(code.replace(/\s/g, ""));
      setSession(null);
      void refresh();
    } catch (cause) {
      const failed = failureOf(cause);
      setError(
        failed.code === "approval-code-mismatch"
          ? "That code doesn't match. Nothing was approved. Check the code on the new browser and type it again, or start again."
          : `Could not approve: ${failed.message}`,
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <VStack gap={2}>
      {!session ? (
        <HStack>
          <Button
            label="Approve"
            size="sm"
            variant="primary"
            isLoading={isBusy}
            onClick={() => void start()}
          />
        </HStack>
      ) : !answered ? (
        <VStack gap={2}>
          <Text>
            Open BandChat on the new browser. It shows a six-digit code once it has answered.
          </Text>
          <HStack>
            <Button label="Start again" size="sm" variant="ghost" onClick={() => void start()} />
          </HStack>
        </VStack>
      ) : (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void approve();
          }}
        >
          <VStack gap={2}>
            <TextInput
              label="Code shown on the new browser"
              value={code}
              onChange={setCode}
              isRequired
            />
            <HStack gap={2}>
              <Button
                label="Approve this code"
                type="submit"
                size="sm"
                variant="primary"
                isLoading={isBusy}
              />
              <Button label="Start again" size="sm" variant="ghost" onClick={() => void start()} />
            </HStack>
          </VStack>
        </form>
      )}
      {error ? <ErrorText>{error}</ErrorText> : null}
    </VStack>
  );
}

/**
 * Recovery, so the account survives losing every device: one button. The
 * secret goes into a passkey where the browser offers one; otherwise, or if
 * the user declines, it is shown once as a phrase. The phrase lives only in
 * this component's state, and is gone once the user has written it down.
 */
function RecoverySetup({ accountName }: { accountName: string }) {
  const { devices, encryption } = useDeviceState();
  const [count, setCount] = useState<number | null>(null);
  const [result, setResult] = useState<
    { method: "passkey" } | { method: "phrase"; phrase: string } | null
  >(null);
  const [isBusy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let stopped = false;
    encryption.recovery
      .list()
      .then((roots) => {
        if (stopped) return;
        setCount(roots.length);
        if (roots.length === 0) setResult(null);
      })
      .catch((cause: unknown) => !stopped && setError(failureOf(cause).message));
    return () => {
      stopped = true;
    };
  }, [devices, encryption]);

  async function enable() {
    setBusy(true);
    setError(null);
    try {
      setResult(await encryption.recovery.enable({ appName: APP_NAME, accountName }));
      setCount((previous) => (previous ?? 0) + 1);
    } catch (cause) {
      setError(`Could not set up recovery: ${failureOf(cause).message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <VStack gap={2}>
      <Heading level={3}>Recovery</Heading>
      {result?.method === "phrase" ? (
        <Card width="100%">
          <VStack gap={2}>
            <Text weight="semibold">Your recovery phrase</Text>
            <Text>
              Write these 24 words down and keep them safe. BandChat shows them only now and keeps
              no copy. With them you can read your messages on a new browser when no other device is
              around.
            </Text>
            <Text data-recovery-phrase>{result.phrase}</Text>
            <HStack>
              <Button
                label="I've written it down"
                variant="primary"
                onClick={() => setResult(null)}
              />
            </HStack>
          </VStack>
        </Card>
      ) : result?.method === "passkey" ? (
        <Text>
          Your recovery key is saved in a passkey. On a new browser, choose “Restore with passkey”
          while it waits for approval.
        </Text>
      ) : count === null ? null : count > 0 ? (
        <Text color="secondary">Recovery is set up for your account.</Text>
      ) : (
        <VStack gap={2}>
          <Text color="secondary">
            Without recovery, losing every device loses your messages for good.
          </Text>
          <HStack>
            <Button label="Set up recovery" isLoading={isBusy} onClick={() => void enable()} />
          </HStack>
        </VStack>
      )}
      {error ? <ErrorText>{error}</ErrorText> : null}
    </VStack>
  );
}

/** On a browser waiting for approval: the code to show, and both ways back in. */
function RestoreAccess() {
  const { encryption, approval, answerChallenge, refresh } = useDeviceState();
  const [phrase, setPhrase] = useState("");
  const [isBusy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function run(restore: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await restore();
      await refresh();
    } catch (cause) {
      setError(restoreMessage(failureOf(cause)));
    } finally {
      setBusy(false);
    }
  }

  return (
    <VStack gap={4}>
      <VStack gap={2}>
        <Heading level={3}>Approve from another device</Heading>
        <ApprovalCode approval={approval} />
        <HStack>
          <Button label="Show approval code" size="sm" onClick={() => void answerChallenge()} />
        </HStack>
      </VStack>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          const words = phrase.trim();
          // Gone from the page as soon as it is used.
          setPhrase("");
          void run(() => encryption.recovery.restore(words));
        }}
      >
        <VStack gap={2}>
          <Heading level={3}>Restore with your recovery phrase</Heading>
          <TextArea label="Recovery phrase" value={phrase} onChange={setPhrase} rows={3} />
          <HStack>
            <Button
              label="Restore with phrase"
              type="submit"
              isLoading={isBusy}
              isDisabled={!phrase.trim()}
            />
          </HStack>
        </VStack>
      </form>
      <VStack gap={2}>
        <Heading level={3}>Restore with your passkey</Heading>
        <HStack>
          <Button
            label="Restore with passkey"
            isLoading={isBusy}
            onClick={() =>
              void run(() => encryption.recovery.restoreFromPasskey({ appName: APP_NAME }))
            }
          />
        </HStack>
      </VStack>
      {error ? <ErrorText>{error}</ErrorText> : null}
    </VStack>
  );
}

export function ApprovalCode({
  approval,
}: {
  approval: ReturnType<typeof useDeviceState>["approval"];
}) {
  if (!approval) {
    return (
      <Text>
        On a browser where you can read your messages, open Your profile and choose Approve next to
        this browser. This browser then shows a code to type there.
      </Text>
    );
  }
  if ("code" in approval) {
    return (
      <Text>
        Type this code on your other device: <Text weight="semibold">{approval.code}</Text>
      </Text>
    );
  }
  return <ErrorText>{approvalMessage(approval.failure)}</ErrorText>;
}

export function approvalMessage(failure: Failure): string {
  switch (failure.code) {
    case "no-challenge":
      return "Your other device hasn't started the approval yet. Choose Approve next to this browser there, then show the code again.";
    case "approval-answered":
      return "This browser already answered that approval, perhaps before a reload. Type the code it showed then, or choose Start again on your other device and show the code again.";
    default:
      return `Could not answer the approval: ${failure.message}`;
  }
}

function restoreMessage(failure: Failure): string {
  if (failure.code === "recovery-root-mismatch")
    return "That recovery phrase or passkey doesn't belong to this account. Nothing changed.";
  return `Could not restore access: ${failure.message}`;
}
