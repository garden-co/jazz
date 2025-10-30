// [!code hide]
import { useState } from "react";
import { co } from "jazz-tools";
import { experimental_useInboxSender } from "jazz-tools/react";
import { BookTicketMessage, Event } from "@/lib/schema";

function EventComponent({ event }: { event: co.loaded<typeof Event> }) {
  const sendInboxMessage = experimental_useInboxSender(process.env.WORKER_ID);
  const [isLoading, setIsLoading] = useState(false);

  const onBookTicketClick = async () => {
    setIsLoading(true);

    const ticketId = await sendInboxMessage(
      BookTicketMessage.create({
        type: "bookTicket",
        event: event,
      }),
    );

    alert(`Ticket booked: ${ticketId}`);
  };

  return (
    <button onClick={onBookTicketClick} disabled={isLoading}>
      Book Ticket
    </button>
  );
}
