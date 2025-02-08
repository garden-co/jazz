"use client";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Event } from "@/schema";
import { useAccount } from "jazz-react";
import { Group } from "jazz-tools";
import { useRouter } from "next/navigation";
import { useState } from "react";

export default function CreateEvent() {
  const [date, setDate] = useState<Date | undefined>(new Date());
  const { me } = useAccount({ root: { events: [] } });
  const router = useRouter();

  function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    console.log(e.target);

    if (!me) return;

    const form = e.target as HTMLFormElement & {
      name: HTMLInputElement;
      description: HTMLTextAreaElement;
      time: HTMLInputElement;
      location: HTMLInputElement;
    };

    if (!date) return;

    // create datetime based on date and time
    const dateTime = new Date(date);
    dateTime.setHours(parseInt(form.time.value.split(":")[0]));
    dateTime.setMinutes(parseInt(form.time.value.split(":")[1]));
    dateTime.setSeconds(0);

    const event = Event.create(
      {
        name: form.name.value,
        description: form.description.value,
        date: dateTime,
        location: form.location.value,
      },
      { owner: Group.create().addMember("everyone", "reader") },
    );

    me.root.events.push(event);

    router.push(`/event/${event.id}`);
  }

  return (
    <form className="flex flex-col gap-2" onSubmit={handleSubmit}>
      <h1 className="text-2xl font-bold">Create Event</h1>
      <Input type="text" name="name" placeholder="Event Name" />
      <Textarea
        type="text"
        name="description"
        placeholder="Event Description"
      />
      <Calendar
        className="rounded-md border"
        selected={date}
        onSelect={setDate}
      />
      <Input type="time" name="time" placeholder="Event Time" />
      <Input type="text" name="location" placeholder="Event Location" />
      <Button type="submit">Create Event</Button>
    </form>
  );
}
