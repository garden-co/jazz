// #region Basic
import { Account, co, Group } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { BookTicketMessage, Ticket } from "@/lib/schema";

const {
  worker,
  experimental: { inbox },
} = await startWorker({
  accountID: process.env.JAZZ_WORKER_ACCOUNT,
  accountSecret: process.env.JAZZ_WORKER_SECRET,
  syncServer: "wss://cloud.jazz.tools/?key=your-api-key",
});

inbox.subscribe(BookTicketMessage, async (message, senderID) => {
  const madeBy = await co.account().load(senderID, { loadAs: worker });

  const { event } = await message.$jazz.ensureLoaded({
    resolve: {
      event: {
        reservations: true,
      },
    },
  });

  const ticketGroup = Group.create(worker);
  const ticket = Ticket.create({
    account: madeBy,
    event,
  });

  if (madeBy.$isLoaded) {
    // Give access to the ticket to the client
    ticketGroup.addMember(madeBy, "reader");
    event.reservations.$jazz.push(ticket);
  }

  return ticket;
});
// #endregion

// #region WithUnionSchema
import { InboxMessage } from "@/lib/schema";

inbox.subscribe(InboxMessage, async (message, senderID) => {
  switch (message.type) {
    case "bookTicket":
      return await handleBookTicket(message, senderID);
    case "cancelReservation":
      return await handleCancelReservation(message, senderID);
  }
});
// #endregion

// MOCKS
async function handleBookTicket(
  message: co.loaded<typeof InboxMessage>,
  senderID: string,
) {
  return undefined;
}
async function handleCancelReservation(
  message: co.loaded<typeof InboxMessage>,
  senderID: string,
) {
  return undefined;
}
