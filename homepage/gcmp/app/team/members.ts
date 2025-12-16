import type { StaticImageData } from "next/image";
import anselmImage from "../components/images/anselm.jpg";
import guidoImage from "../components/images/guido.jpeg";
import gioImage from "../components/images/gio.jpg";
import nikitaImage from "../components/images/nikita.jpg";
import nicoImage from "../components/images/nico.jpeg";

export interface TeamMember {
  name: string;
  slug: string;
  titles: string[];
  image: StaticImageData | string;
  location: string;
  x?: string;
  github?: string;
  website?: string;
  linkedin?: string;
  bluesky?: string;
}

export const team: Array<TeamMember> = [
  {
    name: "Anselm Eickhoff",
    slug: "anselm",
    titles: ["Founder"],
    image: anselmImage,
    location: "San Francisco, US ",
    x: "anselm_io",
    github: "aeplay",
    website: "http://anselm.io",
    bluesky: "anselm.io",
    linkedin: "anselm-eickhoff",
  },
  {
    name: "Guido D'Orsi",
    slug: "guido",
    titles: ["Lead Engineer", "React Performance"],
    image: guidoImage,
    location: "Piano di Sorrento, Italy ",
    github: "gdorsi",
  },
  {
    name: "Giordano Ricci",
    slug: "gio",
    titles: ["Full-Stack Dev", "Observability Expert"],
    location: "Lisbon, Portugal ",
    github: "Elfo404",
    website: "https://giordanoricci.com",
    linkedin: "giordanoricci",
    image: gioImage,
  },
  {
    name: "Nikita Voloboev",
    slug: "nikita",
    location: "Barcelona, Spain",
    titles: ["Full-Stack Dev"],
    x: "nikitavoloboev",
    github: "nikitavoloboev",
    website: "https://nikiv.dev",
    image: nikitaImage,
  },
  {
    name: "Nico Rainhart",
    slug: "nico",
    location: "Buenos Aires, Argentina",
    titles: ["Full-Stack Dev", "Framework Engineer"],
    image: nicoImage,
    github: "nrainhart",
    linkedin: "nicolás-rainhart",
  },
];
