import { CoList, CoMap, CoPlainText, co } from "jazz-tools";

export class Message extends CoMap {
  text = co.ref(CoPlainText);
}

export class Chat extends CoList.Of(co.ref(Message)) {}
