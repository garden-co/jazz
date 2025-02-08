"use client";

import { Button } from "@/components/ui/button";
import { useAccount } from "jazz-react";
import Link from "next/link";

export default function EventList() {
  const { me } = useAccount({ root: { events: [{}] } });

  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-2xl font-bold">Events</h2>
      <Button asChild>
        <Link href="/event/create">Create Event</Link>
      </Button>
      <div className="flex flex-col gap-2">
        {me?.root.events.map((event) => (
          <Link
            className="text-lg underline"
            key={event.id}
            href={`/event/${event.id}`}
          >
            {event.name}
          </Link>
        ))}
      </div>
    </div>
  );
}
