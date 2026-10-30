"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";

export function LoadingScreen({ label }: { label: string }) {
  return (
    <Center minHeight="100dvh" padding={4}>
      <Spinner size="lg" label={label} />
    </Center>
  );
}

export function ErrorScreen({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <Center minHeight="100dvh" padding={4}>
      <div role="alert">
        <Banner
          status="error"
          title="Your Jazz account could not be opened"
          description={message}
          endContent={<Button label="Retry" onClick={onRetry} />}
        />
      </div>
    </Center>
  );
}
