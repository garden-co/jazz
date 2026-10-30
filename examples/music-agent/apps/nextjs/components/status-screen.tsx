"use client";

import { Banner } from "@astryxdesign/core/Banner";
import { Center } from "@astryxdesign/core/Center";
import { Spinner } from "@astryxdesign/core/Spinner";

/** Full-height loading or error state shown while the workspace opens. */
export function StatusScreen({ message, error }: { message?: string; error?: string }) {
  return (
    <Center height="100vh" padding={4}>
      {error ? (
        <Banner status="error" title="Could not open MusicAgent" description={error} />
      ) : (
        <Spinner size="lg" label={message} />
      )}
    </Center>
  );
}
