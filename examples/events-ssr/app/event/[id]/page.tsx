import { startWorker } from "jazz-nodejs";
import { serializePrefetched } from "jazz-react";
import { ID } from "jazz-tools";

import { Event } from "@/schema";
import { cookies } from "next/headers";
import EventComponent from "./eventComponent";

const workerPool = {} as {
  getWorkerFor(
    credentialsFromCookie: string | "public",
  ): Promise<{ worker: Worker; done: () => {} }>;
};

export default async function EventPage({
  params,
}: { params: { id: ID<Event> } }) {
  const { worker, done } = await workerPool.getWorkerFor(
    (await cookies()).get("jazz_credentials")?.value ?? "public",
  );

  const { id } = await params;
  const event = await Event.load(id, worker, {});

  if (!event) {
    return <div>Event not found</div>;
  }

  done();

  return <EventComponent prefetchedEvent={serializePrefetched(event)} />;
}
