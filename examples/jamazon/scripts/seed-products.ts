import { createDb } from "jazz-tools/backend";
import { app } from "../schema/app";
import { ADMIN_SECRET, APP_ID, JAZZ_SERVER_PORT } from "@/config";

type SeedProduct = {
  name: string;
  brand: string;
  category: string;
  description: string;
  image_url: string;
  price_cents: number;
  rating: number;
  in_stock: number;
};

const seedProducts: SeedProduct[] = [
  {
    name: "Stratocaster Electric Guitar",
    brand: "Fender",
    category: "Guitars",
    description: "Classic single-coil tone with smooth tremolo bridge.",
    image_url: "https://images.unsplash.com/photo-1510915361894-db8b60106cb1?w=1200",
    price_cents: 119900,
    rating: 4.8,
    in_stock: 12,
  },
  {
    name: "Les Paul Studio",
    brand: "Gibson",
    category: "Guitars",
    description: "Warm humbucker sound for rock and modern blues.",
    image_url: "https://images.unsplash.com/photo-1511379938547-c1f69419868d?w=1200",
    price_cents: 159900,
    rating: 4.7,
    in_stock: 6,
  },
  {
    name: "Digital Stage Piano P125",
    brand: "Yamaha",
    category: "Keys",
    description: "Weighted keys with rich grand piano samples.",
    image_url: "https://images.unsplash.com/photo-1520523839897-bd0b52f945a0?w=1200",
    price_cents: 69900,
    rating: 4.6,
    in_stock: 15,
  },
  {
    name: "88-Key MIDI Controller",
    brand: "Arturia",
    category: "Keys",
    description: "Controller keyboard for studio production workflows.",
    image_url: "https://images.unsplash.com/photo-1557672172-298e090bd0f1?w=1200",
    price_cents: 49900,
    rating: 4.5,
    in_stock: 20,
  },
  {
    name: 'Maple Snare Drum 14"',
    brand: "Pearl",
    category: "Drums",
    description: "Sharp attack and controlled sustain for live gigs.",
    image_url: "https://images.unsplash.com/photo-1519892300165-cb5542fb47c7?w=1200",
    price_cents: 32900,
    rating: 4.4,
    in_stock: 10,
  },
  {
    name: "5-Piece Drum Kit",
    brand: "Tama",
    category: "Drums",
    description: "All-in-one setup with hardware and cymbal starter pack.",
    image_url: "https://images.unsplash.com/photo-1507838153414-b4b713384a76?w=1200",
    price_cents: 89900,
    rating: 4.5,
    in_stock: 5,
  },
  {
    name: "Live Vocal Microphone",
    brand: "Shure",
    category: "Audio",
    description: "Cardioid dynamic mic built for stage reliability.",
    image_url: "https://images.unsplash.com/photo-1516280440614-37939bbacd81?w=1200",
    price_cents: 9900,
    rating: 4.9,
    in_stock: 30,
  },
  {
    name: "USB Audio Interface 2x2",
    brand: "Focusrite",
    category: "Audio",
    description: "Low-latency recording interface for vocals and guitars.",
    image_url: "https://images.unsplash.com/photo-1598488035139-bdbb2231ce04?w=1200",
    price_cents: 17900,
    rating: 4.7,
    in_stock: 18,
  },
];

const db = await createDb({
  appId: APP_ID,
  env: "dev",
  userBranch: "main",
  adminSecret: ADMIN_SECRET,
  serverUrl: `http://localhost:${JAZZ_SERVER_PORT}`,
  localAuthToken: ADMIN_SECRET,
  localAuthMode: "anonymous",
});

const existing = await db.all(app.products, { tier: "global" });

if (existing.length > 0) {
  console.log("Products already exist. Exiting.");
  process.exit(0);
}

let inserted = 0;
for (const product of seedProducts) {
  console.log("inserting", product.name);
  await db.insertDurable(app.products, product, { tier: "global" });
  inserted += 1;
}

await db.shutdown();
console.log(`Jamazon seed complete. Inserted ${inserted} product(s).`);
process.exit(0);
