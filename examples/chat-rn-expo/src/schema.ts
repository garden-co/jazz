import { CoList, CoMap, coField } from "jazz-tools";

export class Message extends CoMap {
  text = coField.string;
}

export class Chat extends CoList.Of(coField.ref(Message)) {}
