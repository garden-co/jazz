import { co } from "jazz-tools";
import { CojsonCoreRn } from "cojson-core-rn";

export const Message = co.map({
  text: co.plainText(),
});

export const Chat = co.list(Message);
