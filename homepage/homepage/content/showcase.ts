import opacityImage from "./showcase/images/opacity.png";
import invoiceRadarImage from "./showcase/images/invoice-radar.png";
import reconfiguredImage from "./showcase/images/reconfigured.png";
import wagesoImage from "./showcase/images/wageso.png";
import tillyScreenImage from "./showcase/images/tilly-screen.png";
import learnAnythingImage from "./showcase/images/learn-anything.png";
import cupplImage from "./showcase/images/cuppl.png";
import mtorImage from "./showcase/images/mtor.png";
import hendImage from "./showcase/images/hend.png";
import motleyImage from "./showcase/images/motley.png";
import spicyGolfImage from "./showcase/images/spicy-golf.png";
import { testimonials } from "./testimonials";

export const products = [
  {
    name: "Opacity",
    imageUrl: opacityImage.src,
    url: "https://opacity.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A product designer's best friend.",
  },
  {
    name: "Invoice Radar",
    imageUrl: invoiceRadarImage.src,
    url: "https://invoiceradar.com",
    description: "",
    jazzUse: "",
    testimonials: [
      testimonials.invoiceRadar,
    ],
    slogan: "Automatically gather invoices.",
  },
  {
    name: "Reconfigured",
    imageUrl: reconfiguredImage.src,
    url: "https://reconfigured.io",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "The AI notepad for people who think for work.",
  },
  {
    name: "Wageso",
    imageUrl: wagesoImage.src,
    url: "https://wageso.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A a privacy-first financial tracking app.",
  },
  {
    name: "Tilly",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan:
      "Be the friend who remembers.",
    url: "https://tilly.social",
    imageUrl: tillyScreenImage.src,
  },
  {
    name: "Learn Anything",
    imageUrl: learnAnythingImage.src,
    url: "https://learn-anything.xyz",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan:
      "A community-driven learning platform.",
  },
  {
    name: "Cuppl",
    imageUrl: cupplImage.src,
    url: "https://www.getcuppl.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "An all-in-one app for couples.",
  },
  {
    name: "MTOR",
    imageUrl: mtorImage.src,
    url: "https://mtor.club",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A collaborative, real-time workout planner and tracker.",
  },
  {
    name: "Hend",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan:
      "Natural language acquisition, dialed to your level.",
    url: "https://hendapp.com",
    imageUrl: hendImage.src,
  },
  {
    name: "Motley",
    imageUrl: motleyImage.src,
    url: "https://trymotley.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Collect and connect anything.",
  },
  {
    name: "Spicy Golf",
    imageUrl: spicyGolfImage.src,
    url: "https://spicy.golf",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Golf Games Kicked up a Notch.",
  }
];
