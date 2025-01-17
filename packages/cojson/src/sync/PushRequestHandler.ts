import { CoValueCore, isTryAddTransactionsException } from "../coValueCore.js";
import { CoValueAvailableState } from "../coValueEntry.js";
import { Peers } from "../peer/index.js";
import { AbstractMessageHandler } from "./AbstractMessageHandler.js";
import { DependencyService } from "./DependencyService.js";
import { SyncService } from "./SyncService.js";
import { PushMessageHandlerInput, emptyKnownState } from "./types.js";

/**
 * "Push" request
 * - must be followed by "ack" message response according to the protocol.
 * - may carry along new data txs to be added
 */
export class PushRequestHandler extends AbstractMessageHandler {
  constructor(
    private readonly syncService: SyncService,
    private readonly dependencyService: DependencyService,
    private readonly peers: Peers,
  ) {
    super();
  }

  async handleAvailable(input: PushMessageHandlerInput): Promise<unknown> {
    const { coValue } = input.entry.state as CoValueAvailableState;
    await this.dependencyService.loadUnknownDependencies(input);

    return this.addData(coValue, input);
  }

  async handleUnknown(input: PushMessageHandlerInput) {
    const { msg, entry, peer } = input;
    if (!msg.header) {
      console.error(`Unexpected unavailable state for coValue ${input.msg.id}`);
    }
    entry.moveToLoadingState([peer]);

    return this.routeMessage(input);
  }

  async handleLoading(input: PushMessageHandlerInput) {
    if (!input.msg.header) {
      console.error(`Unexpected loading state for coValue ${input.msg.id}`);
      return;
    }

    await this.dependencyService.MakeAvailableWithDependencies(input);

    return this.routeMessage(input);
  }

  private async addData(coValue: CoValueCore, input: PushMessageHandlerInput) {
    const { msg, peer, entry } = input;

    const knownState = coValue.knownState();
    const isEmptyKnownState =
      !knownState.header ||
      !knownState.sessions ||
      !Object.keys(knownState.sessions).length;

    const assumedPeerKnownState = isEmptyKnownState
      ? emptyKnownState(knownState.id)
      : { ...knownState };

    try {
      const anyMissedTransaction = coValue.addNewContent(msg);

      anyMissedTransaction
        ? await peer.send.pull({ knownState: coValue.knownState() })
        : await peer.send.ack({ knownState: coValue.knownState() });
    } catch (e) {
      if (isTryAddTransactionsException(e)) {
        const { message, error } = e;
        console.error(peer.id, message, error);

        peer.erroredCoValues.set(msg.id, error);
      } else {
        console.error("Unknown error", peer.id, e);
      }

      return;
    }

    const peers = this.peers.getInPriorityOrder({
      excludedId: peer.id,
    });

    await this.syncService.syncCoValue(entry, assumedPeerKnownState, peers);
  }
}
