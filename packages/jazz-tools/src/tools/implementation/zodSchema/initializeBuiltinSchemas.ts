import { CoMap } from "../../coValues/coMap.js";
import { Account } from "../../coValues/account.js";
import { Profile } from "../../coValues/profile.js";
import { coMapDefiner, coProfileDefiner } from "./zodCo.js";
import { coAccountDefiner } from "./zodCo.js";

CoMap.coValueSchema = coMapDefiner({});
Account.coValueSchema = coAccountDefiner();
Profile.coValueSchema = coProfileDefiner();
