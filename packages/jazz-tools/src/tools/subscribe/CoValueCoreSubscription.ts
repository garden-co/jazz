import { CoValueCore, LocalNode, RawCoMap, RawCoValue } from "cojson";

export class CoValueCoreSubscription {
  _unsubscribe: () => void = () => {};
  unsubscribed = false;

  value: RawCoMap | undefined;

  constructor(
    public node: LocalNode,
    public id: string,
    public listener: (value: RawCoValue | "unavailable") => void,
    public skipRetry?: boolean,
  ) {
    const entry = this.node.getCoValue(this.id as any);

    if (entry?.isAvailable()) {
      this.subscribe(entry.getCurrentContent());
    } else {
      this.node
        .loadCoValueCore(this.id as any, undefined, skipRetry)
        .then((value) => {
          if (this.unsubscribed) return;

          if (value.isAvailable()) {
            this.subscribe(value.getCurrentContent());
          } else {
            this.subscribeToState();
            this.listener("unavailable");
          }
        });
    }
  }

  subscribeToState() {
    const entry = this.node.getCoValue(this.id as any);
    const handleStateChange = (
      core: CoValueCore,
      unsubFromStateChange: () => void,
    ) => {
      if (this.unsubscribed) {
        unsubFromStateChange();
        return;
      }

      if (core.isAvailable()) {
        this.subscribe(core.getCurrentContent());
        unsubFromStateChange();
      }
    };

    const unsubFromStateChange = entry.subscribe(handleStateChange);

    this._unsubscribe = () => {
      unsubFromStateChange();
    };
  }

  subscribe(value: RawCoValue) {
    if (this.unsubscribed) return;

    this._unsubscribe = value.subscribe((value) => {
      this.listener(value);
    });

    this.listener(value);
  }

  unsubscribe() {
    if (this.unsubscribed) return;
    this.unsubscribed = true;
    this._unsubscribe();
  }
}
