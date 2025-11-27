// #region Basic
import { bookEventTicket } from "@/bookEventTicket";
import { Event } from "@/lib/schema";
import { co, isJazzRequestError } from "jazz-tools";

// @ts-expect-error duplicate
export async function sendEventBookingRequest(event: co.loaded<typeof Event>) {
  const { ticket } = await bookEventTicket.send({ event });

  return ticket;
}
// #endregion

// #region ErrorHandling
// @ts-expect-error duplicate
export async function sendEventBookingRequest(event: co.loaded<typeof Event>) {
  try {
    const { ticket } = await bookEventTicket.send({ event });

    return ticket;
  } catch (error) {
    // This works as a type guard, so you can easily get the error message and details
    if (isJazzRequestError(error)) {
      alert(error.message);
      return;
    }
  }
}
// #endregion
