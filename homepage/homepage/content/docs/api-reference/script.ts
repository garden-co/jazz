import { co } from "jazz-tools";
//Object.keys(co).map((v) => console.log(v));

function getAllKeys(obj: any) {
  const keys: (string | symbol)[] = [];
  let current = obj;

  while (current) {
    keys.push(...Object.getOwnPropertyNames(current));
    keys.push(...Object.getOwnPropertySymbols(current));
    if (Object.getPrototypeOf(current) === Object.prototype) {
      break;
    }
    current = Object.getPrototypeOf(current); // Don't care about pure object prototype
  }

  return Array.from(new Set(keys)); // remove duplicates
}
const act = co.account();
getAllKeys(act)
  .sort()
  .forEach((el) => console.log(el));

//act.constructor
/* Account
builtin
coValueClass
collaborative
constructor
create
createAs
getCoValueClass
getDefinition
getMe
load
optional
resolveQuery
resolved
shape
subscribe
unstable_merge
withMigration
*/
/*
account
discriminatedUnion
feed
fileStream
group
image
list
map
optional
plainText
profile
record
richText
vector
 */
