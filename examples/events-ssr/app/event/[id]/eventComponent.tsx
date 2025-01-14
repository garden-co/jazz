"use client";

import { Prefetched, useHydratedCoState } from "jazz-react";
import { Event } from "../../schema";

export default function EventComponent({
  prefetchedEvent,
}: { prefetchedEvent: Prefetched<Event> }) {
  const event = useHydratedCoState(Event, {}, prefetchedEvent);

  return (
    <div>
      <h1 className="text-2xl font-bold">{event.name}</h1>
      <p>{event.description}</p>
      <p>{event.location}</p>
      <p>{event.date.toLocaleString()}</p>
    </div>
  );
}
