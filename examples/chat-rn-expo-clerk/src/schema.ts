import { CoList, CoMap, CoPlainText, ImageDefinition, co } from "jazz-tools";

export class Message extends CoMap {
  text = co.ref(CoPlainText);
  image = co.optional.ref(ImageDefinition);
}

export class Chat extends CoList.Of(co.ref(Message)) {}
