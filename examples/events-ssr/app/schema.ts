import { Account, CoList, CoMap, co } from "jazz-tools";

export class Event extends CoMap {
  name = co.string;
  description = co.string;
  location = co.string;
  date = co.Date;
}

class EventList extends CoList.Of(co.ref(Event)) {}

class EventAccountRoot extends CoMap {
  events = co.ref(EventList);
}

export class EventAccount extends Account {
  root = co.ref(EventAccountRoot);

  migrate() {
    if (this.root === undefined) {
      this.root = EventAccountRoot.create(
        {
          events: EventList.create([], { owner: this }),
        },
        { owner: this },
      );
    }
  }
}
