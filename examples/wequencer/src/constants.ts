export const DEFAULT_BEAT_COUNT = 16;

export const NAMES = [
  "Amazing Alpaca",
  "Bubbly Baboon",
  "Charming Camel",
  "Dazzling Donkey",
  "Elegant Elephant",
  "Fancy Fox",
  "Gentle Giraffe",
  "Happy Hippo",
  "Jolly Jaguar",
  "Kooky Koala",
  "Lively Lion",
  "Mischievous Monkey",
  "Nifty Narwhal",
  "Oily Otter",
  "Peppy Penguin",
  "Quirky Quokka",
  "Racy Rhino",
  "Silly Seal",
  "Tall Tiger",
  "Uptight Unicorn",
  "Vibrant Vulture",
  "Wacky Walrus",
  "Xtra X-ray",
  "Yummy Yak",
  "Zany Zebra",
];

export function getRandomName(): string {
  const first = NAMES[Math.floor(Math.random() * NAMES.length)].split(" ")[0];
  const last = NAMES[Math.floor(Math.random() * NAMES.length)].split(" ")[1];
  return `${first} ${last}`;
}

export function getStableHue(input: string): number {
  if (!input) return 0;
  let sum = 0;
  for (let i = 0; i < input.length; i++) {
    sum += input.charCodeAt(i);
  }
  return sum % 360;
}
