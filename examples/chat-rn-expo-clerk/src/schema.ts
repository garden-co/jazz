import {
  CoList,
  CoMap,
  CoPlainText,
  ImageDefinition,
  coField,
  zodSchemaToCoSchema,
} from "jazz-tools";

export class Message extends CoMap {
  text = coField.ref(CoPlainText);
  image = coField.optional.ref(zodSchemaToCoSchema(ImageDefinition));
}

export class Chat extends CoList.Of(coField.ref(Message)) {}
