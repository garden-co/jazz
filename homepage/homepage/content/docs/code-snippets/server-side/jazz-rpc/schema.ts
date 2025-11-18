import { co, z } from "jazz-tools";

const Reservation = co.map({});

export const Event = co.map({
  reservations: co.list(Reservation),
  capacity: z.number(),
});

// #region BookTicketMessageSchema
export const BookTicketMessage = co.map({
  type: z.literal("bookTicket"),
  event: Event,
});
// #endregion

export const Ticket = co.map({});
