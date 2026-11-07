import type { Storyboard } from "../storyboard.ts";

export default {
  id: "jamazon",
  summary:
    "A live catalogue search, an item added on the laptop appearing in the cart on her phone, an edit made on the phone with the Wi-Fi off, then checkout and the order's status updating live on both screens.",
  devices: {
    a: { name: "Ada's laptop", address: "jamazon.example.com" },
    p: { kind: "phone", color: "#ea580c", viewport: { width: 340, height: 652 } },
  },
  // Ada creates her account on the laptop and signs in on her phone, which has
  // her (empty) cart open. Compiling every route first keeps the video smooth.
  offCamera: [
    { do: "compileRoutes", on: "a", args: [["/", "/cart", "/sign-in", "/checkout", "/orders"]] },
    { do: "createAccount", on: "a", args: ["Ada"] },
    { do: "signIn", on: "p" },
    { do: "openCart", on: "p" },
    { do: "openStore", on: "a" },
  ],
  // Laptop and phone together; the phone keeps its own size.
  opening: [
    {
      show: [
        { id: "a", x: 20, y: 20, w: 860, h: 760, scale: 0.72 },
        { id: "p", x: 912, y: 20, w: 348, h: 760 },
      ],
    },
  ],
  beats: [
    {
      title: "Jamazon",
      text: "A music-gear store: catalogue, cart, checkout and orders. Next.js + Better Auth + Jazz.",
      hold: 2600,
    },
    { caption: "Ada is signed in on her laptop and her phone, with her cart open there" },
    { wait: 1600 },
    { caption: "Search is a live query over the local catalogue: each key narrows it" },
    { do: "search", on: "a", args: ["snare"] },
    { wait: 1200 },
    { caption: "" },
    { do: "openProduct", on: "a" },
    { caption: "She adds the snare on the laptop…" },
    { do: "addToCart", on: "a" },
    { do: "waitForCartItem", on: "p" },
    { caption: "…and it shows up in the cart on her phone", hold: 2200 },
    { do: "openCartLink", on: "a" },
    { caption: "She bumps the quantity on the laptop, and the phone follows" },
    { do: "setQuantity", on: "a", args: [2] },
    { do: "waitForQuantity", on: "p", args: [2] },
    { poster: true },
    { wait: 1600 },

    { caption: "The phone's Wi-Fi drops…" },
    { wifi: "off", on: "p" },
    { caption: "…and she changes the quantity there, offline" },
    { do: "setQuantity", on: "p", args: [3] },
    { wait: 1500 },
    { do: "expectQuantity", on: "a", args: [2] },
    { caption: "The laptop still says 2", hold: 1800 },
    { caption: "Wi-Fi back on…" },
    { wifi: "on", on: "p" },
    { do: "waitForQuantity", on: "a", args: [3, 45_000] },
    { caption: "…and the laptop catches up", hold: 2200 },

    { do: "openOrdersFromMenu", on: "p" },
    { caption: "Checkout on the laptop. Her phone has her order list open." },
    { do: "checkOut", on: "a" },
    { do: "placeOrder", on: "a" },
    { see: "Awaiting payment", on: "p" },
    { caption: "The order shows up on her phone, live: a query of her own orders", hold: 2400 },

    { caption: "Sandbox payments: first the card is declined…" },
    { do: "pay", on: "a", args: ["Decline"] },
    { see: "Payment failed", on: "a" },
    { wait: 1600 },
    { caption: "…then approved" },
    { do: "pay", on: "a", args: ["Approve"] },
    { see: "Paid", on: "p", exact: true },
    { caption: "A backend worker subscribes to paid orders and ships them. Both screens follow." },
    { see: "Shipped", on: "p", exact: true, timeout: 60_000 },
    { wait: 2400 },
    { caption: "" },
  ],
} satisfies Storyboard;
