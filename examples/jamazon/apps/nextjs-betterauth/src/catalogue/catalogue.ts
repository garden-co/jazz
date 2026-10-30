import type { Art, Hue } from "@/schema";

/**
 * The deterministic, synthetic Jamazon catalogue. Jamazon Warehouse stocks the
 * same company's goods: JAM-001 to JAM-003 are the SKUs its fixtures and
 * benchmarks use (Jazzmaster strings, cable and picks). Everything here is
 * made up; brands are fictional.
 */
export type SeedCategory = { slug: string; name: string; blurb: string };
export type SeedProduct = {
  sku: string;
  slug: string;
  name: string;
  brand: string;
  category: string;
  priceCents: number;
  summary: string;
  description: string;
  specs: [label: string, value: string][];
  hue: Hue;
  art: Art;
  onHand: number;
};

export const CATEGORIES: SeedCategory[] = [
  {
    slug: "guitars",
    name: "Guitars",
    blurb: "Electric, acoustic and everything with six strings.",
  },
  { slug: "basses", name: "Basses", blurb: "Four, five and fretless." },
  { slug: "keys", name: "Keys and synths", blurb: "Stage pianos, synthesizers and controllers." },
  { slug: "drums", name: "Drums", blurb: "Kits, snares and cymbals." },
  { slug: "studio", name: "Studio", blurb: "Microphones, monitoring and amplification." },
  { slug: "accessories", name: "Accessories", blurb: "Strings, cables, picks and pedals." },
];

export const PRODUCTS: SeedProduct[] = [
  // Accessories first: these three SKUs are shared with Jamazon Warehouse.
  {
    sku: "JAM-001",
    slug: "jazzmaster-strings",
    name: "Jazzmaster strings",
    brand: "Offset & Co.",
    category: "accessories",
    priceCents: 2500,
    summary: "Flatwound 11–50 set for offset guitars.",
    description:
      "A warm, smooth flatwound set tuned for offset-body guitars with long floating bridges. The heavier top strings stay seated on the bridge saddles.",
    specs: [
      ["Gauge", "11–50"],
      ["Winding", "Flatwound"],
      ["Core", "Hex steel"],
      ["Strings", "6"],
    ],
    hue: "blue",
    art: "strings",
    onHand: 140,
  },
  {
    sku: "JAM-002",
    slug: "instrument-cable-3m",
    name: "Instrument cable, 3 m",
    brand: "Lineout",
    category: "accessories",
    priceCents: 1000,
    summary: "Braided jack-to-jack cable with a right-angle end.",
    description:
      "Low-capacitance copper with a braided jacket that doesn't kink. One straight and one right-angle plug for pedalboards.",
    specs: [
      ["Length", "3 m"],
      ["Connectors", "6.35 mm jack, straight and right-angle"],
      ["Capacitance", "95 pF/m"],
    ],
    hue: "teal",
    art: "cable",
    onHand: 60,
  },
  {
    sku: "JAM-003",
    slug: "celluloid-picks-12",
    name: "Celluloid picks, 12 pack",
    brand: "Offset & Co.",
    category: "accessories",
    priceCents: 500,
    summary: "Medium 0.71 mm picks in mixed colours.",
    description: "Classic 351 shape with a textured grip. Twelve picks in a tin.",
    specs: [
      ["Thickness", "0.71 mm"],
      ["Shape", "351"],
      ["Quantity", "12"],
    ],
    hue: "orange",
    art: "picks",
    onHand: 4,
  },
  {
    sku: "JAM-004",
    slug: "blue-note-overdrive",
    name: "Blue note overdrive",
    brand: "Stompworks",
    category: "accessories",
    priceCents: 12900,
    summary: "Transparent overdrive with a three-band EQ.",
    description:
      "Adds grit without hiding your guitar. A bass, middle and treble stack lets it sit under a clean amp or push a dirty one.",
    specs: [
      ["Controls", "Level, gain, bass, middle, treble"],
      ["Power", "9 V DC centre negative"],
      ["Bypass", "True bypass"],
    ],
    hue: "blue",
    art: "pedal",
    onHand: 18,
  },
  {
    sku: "JAM-005",
    slug: "tape-echo-delay",
    name: "Tape echo delay",
    brand: "Stompworks",
    category: "accessories",
    priceCents: 18900,
    summary: "Warm, modulated repeats up to 900 ms.",
    description:
      "Digital delay voiced like a worn tape loop, with tap tempo and a wow and flutter knob.",
    specs: [
      ["Delay time", "40–900 ms"],
      ["Controls", "Time, repeats, mix, wobble"],
      ["Power", "9 V DC, 120 mA"],
    ],
    hue: "purple",
    art: "pedal",
    onHand: 0,
  },

  // Guitars
  {
    sku: "JAM-101",
    slug: "offset-sunburst",
    name: "Offset electric, sunburst",
    brand: "Offset & Co.",
    category: "guitars",
    priceCents: 89900,
    summary: "Alder body, two single coils, floating tremolo.",
    description:
      "The surf-to-shoegaze workhorse. A lightweight alder body, rosewood fingerboard and two wide single coils with a rhythm circuit.",
    specs: [
      ["Body", "Alder"],
      ["Neck", "Maple, C profile"],
      ["Scale", "25.5 in"],
      ["Pickups", "2 × single coil"],
    ],
    hue: "orange",
    art: "guitar",
    onHand: 7,
  },
  {
    sku: "JAM-102",
    slug: "semi-hollow-cherry",
    name: "Semi-hollow, cherry",
    brand: "Kingsway",
    category: "guitars",
    priceCents: 124900,
    summary: "Maple semi-hollow with two humbuckers.",
    description:
      "A centre block tames feedback while the hollow wings keep the air. At home in a jazz trio and a loud rock band.",
    specs: [
      ["Body", "Laminated maple, centre block"],
      ["Scale", "24.75 in"],
      ["Pickups", "2 × humbucker"],
    ],
    hue: "red",
    art: "guitar",
    onHand: 3,
  },
  {
    sku: "JAM-103",
    slug: "parlour-acoustic",
    name: "Parlour acoustic",
    brand: "Harbour",
    category: "guitars",
    priceCents: 42900,
    summary: "Small-bodied solid spruce acoustic.",
    description:
      "A couch and campfire guitar with a surprisingly big voice. Slotted headstock, solid top.",
    specs: [
      ["Top", "Solid spruce"],
      ["Back and sides", "Mahogany"],
      ["Scale", "24 in"],
    ],
    hue: "yellow",
    art: "guitar",
    onHand: 11,
  },
  {
    sku: "JAM-104",
    slug: "baritone-electric",
    name: "Baritone electric",
    brand: "Kingsway",
    category: "guitars",
    priceCents: 97900,
    summary: "27 in scale for tuning down to B.",
    description:
      "Longer scale, heavier strings and a dark humbucker for low tunings that stay tight.",
    specs: [
      ["Scale", "27 in"],
      ["Tuning", "B to B"],
      ["Pickups", "1 × humbucker"],
    ],
    hue: "green",
    art: "guitar",
    onHand: 2,
  },

  // Basses
  {
    sku: "JAM-201",
    slug: "jazz-bass-four",
    name: "Four-string bass",
    brand: "Offset & Co.",
    category: "basses",
    priceCents: 94900,
    summary: "Two single coils and a slim neck.",
    description:
      "The classic growl. Blend the pickups for a hollow, vocal tone or solo the bridge for bite.",
    specs: [
      ["Strings", "4"],
      ["Scale", "34 in"],
      ["Pickups", "2 × single coil"],
    ],
    hue: "teal",
    art: "bass",
    onHand: 6,
  },
  {
    sku: "JAM-202",
    slug: "fretless-five",
    name: "Fretless five-string",
    brand: "Kingsway",
    category: "basses",
    priceCents: 139900,
    summary: "Ebony fingerboard with lined position markers.",
    description: "Singing, woody sustain and a low B string that doesn't flop.",
    specs: [
      ["Strings", "5"],
      ["Fingerboard", "Ebony, lined"],
      ["Electronics", "Active, two-band EQ"],
    ],
    hue: "purple",
    art: "bass",
    onHand: 1,
  },
  {
    sku: "JAM-203",
    slug: "short-scale-bass",
    name: "Short-scale bass",
    brand: "Harbour",
    category: "basses",
    priceCents: 49900,
    summary: "30 in scale, light and easy to play.",
    description: "A thumpy, compact bass for small hands, small rooms and long gigs.",
    specs: [
      ["Strings", "4"],
      ["Scale", "30 in"],
      ["Weight", "3.2 kg"],
    ],
    hue: "cyan",
    art: "bass",
    onHand: 9,
  },

  // Keys and synths
  {
    sku: "JAM-301",
    slug: "stage-piano-88",
    name: "Stage piano, 88 keys",
    brand: "Ivory Lane",
    category: "keys",
    priceCents: 179900,
    summary: "Hammer-action keys and sampled grand, electric and organ.",
    description:
      "A gig-ready piano with split and layer, a clear front panel and balanced outputs.",
    specs: [
      ["Keys", "88, graded hammer action"],
      ["Polyphony", "256"],
      ["Outputs", "2 × balanced jack, headphones"],
    ],
    hue: "gray",
    art: "keys",
    onHand: 4,
  },
  {
    sku: "JAM-302",
    slug: "analog-mono-synth",
    name: "Analog mono synth",
    brand: "Voltage Row",
    category: "keys",
    priceCents: 64900,
    summary: "Two oscillators, ladder filter, 32-step sequencer.",
    description:
      "Fat basslines and squelchy leads, with patch points for when you want to go further.",
    specs: [
      ["Voices", "1"],
      ["Oscillators", "2 + sub"],
      ["Filter", "24 dB ladder"],
      ["Sequencer", "32 steps"],
    ],
    hue: "pink",
    art: "synth",
    onHand: 12,
  },
  {
    sku: "JAM-303",
    slug: "poly-synth-8",
    name: "Eight-voice poly synth",
    brand: "Voltage Row",
    category: "keys",
    priceCents: 149900,
    summary: "Lush pads, chord memory and an arpeggiator.",
    description: "Eight analog voices with stereo chorus. Built for pads that fill a room.",
    specs: [
      ["Voices", "8"],
      ["Keys", "49, velocity sensitive"],
      ["Effects", "Chorus, delay, reverb"],
    ],
    hue: "purple",
    art: "synth",
    onHand: 5,
  },
  {
    sku: "JAM-304",
    slug: "mini-controller-25",
    name: "Mini controller, 25 keys",
    brand: "Ivory Lane",
    category: "keys",
    priceCents: 8900,
    summary: "USB MIDI keyboard with pads and knobs.",
    description: "Fits in a backpack. Eight pads, eight knobs, and it's bus powered.",
    specs: [
      ["Keys", "25 mini keys"],
      ["Pads", "8, velocity sensitive"],
      ["Connection", "USB-C"],
    ],
    hue: "blue",
    art: "keys",
    onHand: 30,
  },

  // Drums
  {
    sku: "JAM-401",
    slug: "jazz-kit-four-piece",
    name: "Four-piece jazz kit",
    brand: "Brushfire",
    category: "drums",
    priceCents: 119900,
    summary: "18 in kick, 12 and 14 in toms, 14 in snare.",
    description:
      "Small sizes that sing. Maple shells, vintage-style lugs and a tone that loves brushes.",
    specs: [
      ["Shells", "Maple, 6 ply"],
      ["Kick", "18 × 14 in"],
      ["Toms", "12 × 8 in, 14 × 14 in"],
    ],
    hue: "red",
    art: "drum",
    onHand: 2,
  },
  {
    sku: "JAM-402",
    slug: "brass-snare",
    name: "Brass snare, 14 × 5.5 in",
    brand: "Brushfire",
    category: "drums",
    priceCents: 39900,
    summary: "Bright, cutting brass shell.",
    description: "A crisp, loud snare with rolled edges and a smooth throw-off.",
    specs: [
      ["Shell", "Brass, 1 mm"],
      ["Size", "14 × 5.5 in"],
      ["Wires", "20 strand"],
    ],
    hue: "yellow",
    art: "drum",
    onHand: 8,
  },
  {
    sku: "JAM-403",
    slug: "ride-cymbal-21",
    name: "Dark ride, 21 in",
    brand: "Brushfire",
    category: "drums",
    priceCents: 32900,
    summary: "Hand-hammered, dry and complex.",
    description:
      "Clear stick definition with a trashy wash underneath. Works as a crash when you lean on it.",
    specs: [
      ["Size", "21 in"],
      ["Alloy", "B20 bronze"],
      ["Finish", "Traditional"],
    ],
    hue: "orange",
    art: "cymbal",
    onHand: 5,
  },

  // Studio
  {
    sku: "JAM-501",
    slug: "large-diaphragm-condenser",
    name: "Large-diaphragm condenser",
    brand: "Roomtone",
    category: "studio",
    priceCents: 29900,
    summary: "Cardioid studio microphone for vocals and acoustic instruments.",
    description: "A smooth top end and low self-noise. Comes with a shock mount and pop filter.",
    specs: [
      ["Pattern", "Cardioid"],
      ["Capsule", "1 in"],
      ["Self-noise", "7 dB(A)"],
      ["Power", "48 V phantom"],
    ],
    hue: "cyan",
    art: "mic",
    onHand: 14,
  },
  {
    sku: "JAM-502",
    slug: "dynamic-stage-mic",
    name: "Dynamic stage mic",
    brand: "Roomtone",
    category: "studio",
    priceCents: 9900,
    summary: "The one you'll find on every stage.",
    description: "Tough, feedback-resistant and flattering on voices and snare drums.",
    specs: [
      ["Pattern", "Cardioid"],
      ["Type", "Dynamic"],
      ["Connector", "XLR"],
    ],
    hue: "gray",
    art: "mic",
    onHand: 40,
  },
  {
    sku: "JAM-503",
    slug: "closed-back-headphones",
    name: "Closed-back headphones",
    brand: "Roomtone",
    category: "studio",
    priceCents: 14900,
    summary: "Isolating monitoring for tracking.",
    description: "Comfortable for long sessions with a detachable coiled cable.",
    specs: [
      ["Type", "Closed back"],
      ["Impedance", "38 Ω"],
      ["Cable", "Detachable, coiled"],
    ],
    hue: "green",
    art: "headphones",
    onHand: 22,
  },
  {
    sku: "JAM-504",
    slug: "valve-combo-20",
    name: "Valve combo, 20 W",
    brand: "Lineout",
    category: "studio",
    priceCents: 79900,
    summary: "One 12 in speaker, spring reverb and tremolo.",
    description: "A small, loud valve amp that breaks up sweetly at bedroom-plus volumes.",
    specs: [
      ["Power", "20 W, class A/B"],
      ["Speaker", "1 × 12 in"],
      ["Effects", "Spring reverb, tremolo"],
    ],
    hue: "teal",
    art: "amp",
    onHand: 3,
  },
];

/** Low-stock threshold used by the stock badge. */
export const LOW_STOCK = 5;
