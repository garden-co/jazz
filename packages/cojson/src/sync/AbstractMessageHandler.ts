import { ParallelQueueRunner } from "../utils/parallelQueueRunner.js";
import { MessageHandlerInput, MessageHandlerInterface } from "./types.js";

export abstract class AbstractMessageHandler
  implements MessageHandlerInterface
{
  private readonly queuesRunner = new ParallelQueueRunner();

  handle({ msg, peer, entry }: MessageHandlerInput) {
    this.queuesRunner.defferPer(msg.id, () =>
      this.routeMessage({ msg, peer, entry }),
    );
  }

  protected routeMessage({ msg, peer, entry }: MessageHandlerInput) {
    switch (entry.state.type) {
      case "available":
        return this.handleAvailable({ msg, peer, entry });
      case "loading":
        return this.handleLoading({ msg, peer, entry });
      case "unknown":
      case "unavailable":
        return this.handleUnknown({ msg, peer, entry });
    }
  }
  abstract handleAvailable(input: MessageHandlerInput): Promise<unknown>;
  abstract handleLoading(input: MessageHandlerInput): Promise<unknown>;
  abstract handleUnknown(input: MessageHandlerInput): Promise<unknown>;
}
