import { SessionPage } from "@/components/session-page";

export default async function SequencerSessionPage({
  params,
}: {
  params: Promise<{ sessionId: string }>;
}) {
  const { sessionId } = await params;
  return <SessionPage sessionId={sessionId} />;
}
