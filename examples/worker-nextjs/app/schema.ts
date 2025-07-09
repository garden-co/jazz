import { co, z } from "jazz-tools";
import { experimental_defineRequest } from "jazz-tools/worker";

export const Event = co.map({
  title: z.string(),
  capacity: z.number(),
  attendees: co.list(co.account()),
  minAge: z.number(),
});

export const RegisterRequest = experimental_defineRequest(
  {
    event: Event,
  },
  {
    resolve: {
      event: {
        attendees: true,
      },
    },
    paramsSchema: z.object({
      age: z.number(),
    }),
    responseSchema: z.object({
      success: z.boolean(),
      reason: z.string().optional(),
    }),
    url: "/api/register",
  },
);
