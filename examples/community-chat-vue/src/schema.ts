import { CoList, CoMap, CoPlainText, coField } from "jazz-tools";

export class Message extends CoMap {
  text = coField.ref(CoPlainText);
}

export class Chat extends CoList.Of(coField.ref(Message)) {}
