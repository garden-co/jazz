import { Banner, Button, HStack, VStack } from "@astryxdesign/core";
import { useDeviceState } from "../lib/device-state";
import { ApprovalCode } from "./DevicesPanel";

/**
 * What needs the user's attention about this browser's keys, shown above
 * BandChat wherever the user is: waiting for approval (with the code to type
 * on another device), a key check that gave up, and leaving that did not
 * finish.
 */
export function EncryptionNotices({ signOutFailure }: { signOutFailure?: React.ReactNode }) {
  const { status, leaving, readyFailed, approval, answerChallenge, retryReady } = useDeviceState();
  return (
    <VStack gap={0}>
      {leaving === "unfinished" ? (
        <Banner
          status="error"
          container="section"
          collapsible={false}
          title="Leaving this browser did not finish"
          description="Your other device removed this browser, but its keys could not be erased, so they may still be here. BandChat stays signed in until they are gone. Reloading checks again."
          endContent={<Button label="Try again" onClick={() => window.location.reload()} />}
        />
      ) : null}
      {signOutFailure}
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
      {leaving !== "unfinished" && (status === "awaiting-approval" || status === "reenrolled") ? (
        <Banner
          status="warning"
          container="section"
          collapsible={false}
          title="This browser is waiting for approval"
          description={
            <VStack gap={2}>
              <ApprovalCode approval={approval} />
              <span>
                No other device? Open Your profile to restore with your recovery phrase or passkey.
              </span>
            </VStack>
          }
          endContent={
            <HStack>
              <Button label="Show approval code" onClick={() => void answerChallenge()} />
            </HStack>
          }
        />
      ) : null}
    </VStack>
  );
}
