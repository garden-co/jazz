import type { ReactNode } from "react";
import { Banner, Button } from "@astryxdesign/core";
import type { Failure } from "../lib/device-state";

export function WaitingForApprovalBanner({
  description,
  action,
}: {
  description: ReactNode;
  action?: ReactNode;
}) {
  return (
    <Banner
      status="warning"
      container="section"
      collapsible={false}
      title="This browser is waiting for approval"
      description={description}
      endContent={action}
    />
  );
}

export function KeyCheckFailedBanner({ failure, retry }: { failure: Failure; retry: () => void }) {
  return (
    <Banner
      status="error"
      container="section"
      collapsible={false}
      title="This browser could not check its keys"
      description={failure.message}
      endContent={<Button label="Check again" onClick={() => void retry()} />}
    />
  );
}
