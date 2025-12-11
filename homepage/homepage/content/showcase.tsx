import opacityImage from "./showcase/images/opacity.png";
import suhylImage from "./showcase/images/suhyl.png";
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
import { StaticImageData } from "next/image";
import { OpacityLogo } from "./showcase/images/OpacityLogo";
import { SuhylLogo } from "./showcase/images/SuhylLogo";
import { InvoiceRaderLogo } from "./showcase/images/InvoiceRadarLogo";
import { ReconfiguredLogo } from "./showcase/images/ReconfiguredLogo";
import { WagesoLogo } from "./showcase/images/WagesoLogo";
import { TillyLogo } from "./showcase/images/TillyLogo";
import { CupplLogo } from "./showcase/images/CupplLogo";
import { HendLogo } from "./showcase/images/HendLogo";

type Product = {
  name: string;
  image: StaticImageData;
  logo?: React.FunctionComponent<{ height?: number }>;
  featured?: boolean;
  url: string;
  description: string;
  jazzUse: string;
  testimonials: (typeof testimonials)[keyof typeof testimonials][];
  slogan: string;
};

export const products: Product[] = [
  {
    name: "Opacity",
    image: opacityImage,
    logo: OpacityLogo,
    featured: true,
    url: "https://opacity.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A design tool as the source of truth for components.",
  },
  {
    name: "Suhyl",
    image: suhylImage,
    logo: SuhylLogo,
    featured: true,
    url: "https://suhyl.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A point-of-service system for restaurants and venues.",
  },
  {
    name: "Invoice Radar",
    image: invoiceRadarImage,
    logo: InvoiceRaderLogo,
    url: "https://invoiceradar.com",
    featured: true,
    description: "",
    jazzUse: "",
    testimonials: [testimonials.invoiceRadar],
    slogan: "Automatically gather invoices.",
  },
  {
    name: "Reconfigured",
    image: reconfiguredImage,
    logo: ReconfiguredLogo,
    featured: true,
    url: "https://reconfigured.io",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "The AI notepad for people who think for work.",
  },
  {
    name: "Wageso",
    image: wagesoImage,
    logo: WagesoLogo,
    featured: true,
    url: "https://wageso.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A a privacy-first financial tracking app.",
  },
  {
    name: "Tilly",
    image: tillyScreenImage,
    logo: TillyLogo,
    featured: true,
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Be the friend who remembers.",
    url: "https://tilly.social",
  },
  {
    name: "Learn Anything",
    image: learnAnythingImage,
    url: "https://learn-anything.xyz",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A community-driven learning platform.",
  },
  {
    name: "Cuppl",
    image: cupplImage,
    logo: CupplLogo,
    featured: true,
    url: "https://www.getcuppl.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "An all-in-one app for couples.",
  },
  {
    name: "MTOR",
    image: mtorImage,
    url: "https://mtor.club",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "A collaborative, real-time workout planner and tracker.",
  },
  {
    name: "Hend",
    logo: HendLogo,
    featured: true,
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Natural language acquisition, dialed to your level.",
    url: "https://hendapp.com",
    image: hendImage,
  },
  {
    name: "Motley",
    image: motleyImage,
    url: "https://trymotley.com",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Collect and connect anything.",
  },
  {
    name: "Spicy Golf",
    image: spicyGolfImage,
    url: "https://spicy.golf",
    description: "",
    jazzUse: "",
    testimonials: [],
    slogan: "Golf Games Kicked up a Notch.",
  },
];
