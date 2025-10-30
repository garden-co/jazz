import { co, z } from "jazz-tools";

const Reservation = co.map({});

export const Event = co.map({
  reservations: co.list(Reservation),
});

// #region BookTicketMessageSchema
export const BookTicketMessage = co.map({
  type: z.literal("bookTicket"),
  event: Event,
});
// #endregion

export const Ticket = co.map({});

// #region UnionSchema
const CancelReservationMessage = co.map({
  type: z.literal("cancelReservation"),
  event: Event,
  ticket: Ticket,
});

export const InboxMessage = co.discriminatedUnion("type", [
  BookTicketMessage,
  CancelReservationMessage,
]);
// #endregion
