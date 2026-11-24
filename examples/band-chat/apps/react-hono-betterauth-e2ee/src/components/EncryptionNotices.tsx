import { Banner, Button, HStack, VStack } from "@astryxdesign/core";
import { isWaitingForApproval, useDeviceState } from "../lib/device-state";
import { ApprovalCode } from "./DevicesPanel";
import { KeyCheckFailedBanner, WaitingForApprovalBanner } from "./DeviceBanners";

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
      {readyFailed ? <KeyCheckFailedBanner failure={readyFailed} retry={retryReady} /> : null}
      {leaving !== "unfinished" && isWaitingForApproval(status) ? (
        <WaitingForApprovalBanner
          description={
            <VStack gap={2}>
              <ApprovalCode approval={approval} />
              <span>
                No other device? Open Your profile to restore with your recovery phrase or passkey.
              </span>
            </VStack>
          }
          action={
            <HStack>
              <Button label="Show approval code" onClick={() => void answerChallenge()} />
            </HStack>
          }
        />
      ) : null}
    </VStack>
  );
}
