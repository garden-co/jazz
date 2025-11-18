// #region Basic
import { experimental_defineRequest, z } from "jazz-tools";
import { Event, Ticket } from "@/lib/schema";

const workerId = process.env.NEXT_PUBLIC_JAZZ_WORKER_ACCOUNT!;

export const bookEventTicket = experimental_defineRequest({
  url: "/api/book-event-ticket",
  // The id of the worker Account or Group
  workerId,
  // The schema definition of the data we send to the server
  request: {
    schema: {
      event: Event,
    },
    // The data that will be considered as "loaded" in the server input
    resolve: {
      event: { reservations: true },
    },
  },
  // The schema definition of the data we expect to receive from the server
  response: {
    schema: { ticket: Ticket },
    // The data that will be considered as "loaded" in the client response
    // It defines the content that the server directly sends to the client, without involving the sync server
    resolve: { ticket: true },
  },
});
// #endregion

//
