import { Account, ID, SessionID } from "jazz-tools";

const animals = [
  "elephant",
  "penguin",
  "giraffe",
  "octopus",
  "kangaroo",
  "dolphin",
  "cheetah",
  "koala",
  "platypus",
  "pangolin",
  "tiger",
  "zebra",
  "panda",
  "lion",
  "honey badger",
  "hippo",
];

/**
 * Get a psuedo-random username.
 * @param str The string to get the username from.
 * @returns A psuedo-random username.
 */
export function getRandomUsername(str: string) {
  return `Anonymous ${animals[Math.abs(str.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0)) % animals.length]}`;
}

/**
 * Get the name of a user. If the name is "Anonymous user" or not set, return a random username.
 * @param name The name of the user.
 * @param id The id of the user.
 * @returns The psuedo-random name of the user.
 */
export function getName(
  name: string | undefined,
  id: ID<Account> | SessionID | undefined,
) {
  if (name === "Anonymous user" || !name || !id)
    return getRandomUsername(id ?? "");
  return name;
}
