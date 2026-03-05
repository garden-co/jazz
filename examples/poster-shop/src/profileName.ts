const NAMES = [
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
  return NAMES[Math.floor(Math.random() * NAMES.length)];
}
