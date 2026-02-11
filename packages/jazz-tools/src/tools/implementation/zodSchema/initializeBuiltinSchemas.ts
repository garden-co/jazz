import { CoMap } from "../../coValues/coMap.js";
import { Account } from "../../coValues/account.js";
import { coMapDefiner } from "./zodCo.js";
import { coAccountDefiner } from "./zodCo.js";

CoMap.coValueSchema = coMapDefiner({});
Account.coValueSchema = coAccountDefiner();
