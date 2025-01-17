import { RawCoID } from "../ids.js";
type DeferredFn = () => Promise<unknown>;

export class ParallelQueueRunner {
  private queueIds: Map<RawCoID, { queue: DeferredFn[]; locked: boolean }> =
    new Map();

  defferPer(queueId: RawCoID, fn: () => Promise<unknown>) {
    const item = this.queueIds.get(queueId);
    if (item) {
      item.queue.push(fn);
    } else {
      this.queueIds.set(queueId, { queue: [fn], locked: false });
    }

    void this.processQueue(queueId);
  }

  private async processQueue(queueId: RawCoID) {
    const queueEntry = this.queueIds.get(queueId)!;

    if (queueEntry.locked) return;
    queueEntry.locked = true;

    while (queueEntry.queue.length) {
      try {
        await queueEntry.queue.shift()!();
      } catch (e) {
        console.error(`Error while processing queue for ${queueId} ${e}`);
      }
    }

    queueEntry.locked = false;
  }
}

// Test it
// function sleep(ms: number) {
//   return new Promise((resolve) => setTimeout(resolve, ms));
// }
//
// function test(id: RawCoID, msg: string, ms: number) {
//   queue.deffer(id, async () => {
//     console.log(id, msg, "start");
//     await sleep(ms);
//     console.log(id, msg, "end");
//   });
// }
//
// const queue = new QueueRunner();
//
// test("co_zXkkbcca9nkdfJHBo4RHhX22Tf", "1", 400);
// test("co_zXkkbcca9nkdfJHBo4RHhX22Tf", "2", 300);
// test("111", "1", 200);
// test("111", "2", 200);
// test("111", "3", 200);
// test("co_zXkkbcca9nkdfJHBo4RHhX22Tf", "3", 200);
// test("co_zXkkbcca9nkdfJHBo4RHhX22Tf", "4", 200);
// test("co_zXkkbcca9nkdfJHBo4RHhX22Tf", "5", 200);
