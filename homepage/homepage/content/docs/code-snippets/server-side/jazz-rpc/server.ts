// #region Basic
import { jazzServer } from "@/jazzServer";
import { Ticket } from "@/lib/schema";
import { bookEventTicket } from "@/bookEventTicket";
import { Group, JazzRequestError } from "jazz-tools";

// @ts-expect-error
export async function POST(request: Request) {
  return bookEventTicket.handle(
    request,
    jazzServer.worker,
    async ({ event }, madeBy) => {
      const ticketGroup = Group.create(jazzServer.worker);
      const ticket = Ticket.create({
        account: madeBy,
        event,
      });

      // Give access to the ticket to the client
      ticketGroup.addMember(madeBy, "reader");

      event.reservations.$jazz.push(ticket);

      return {
        ticket,
      };
    },
  );
}
// #endregion

// #region ErrorHandling
// @ts-expect-error
export async function POST(request: Request) {
  return bookEventTicket.handle(
    request,
    jazzServer.worker,
    async ({ event }, madeBy) => {
      // Check if the event is full
      if (event.reservations.length >= event.capacity) {
        // The JazzRequestError is propagated to the client, use it for any validation errors
        throw new JazzRequestError("Event is full", 400);
      }

      const ticketGroup = Group.create(jazzServer.worker);
      const ticket = Ticket.create({
        account: madeBy,
        event,
      });

      // Give access to the ticket to the client
      ticketGroup.addMember(madeBy, "reader");

      event.reservations.$jazz.push(ticket);

      return {
        ticket,
      };
    },
  );
}
// #endregion
