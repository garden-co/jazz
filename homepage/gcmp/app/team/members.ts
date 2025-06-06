export interface TeamMember {
  name: string;
  titles: string[];
  image: string;
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
    titles: ["Founder"],
    image: "anselm.jpg",
    location: "London, UK ",
    x: "anselm_io",
    github: "aeplay",
    website: "http://anselm.io",
    bluesky: "anselm.io",
    linkedin: "anselm-eickhoff",
  },
  {
    name: "Guido D'Orsi",
    titles: ["Lead Engineer", "React Performance"],
    image: "guido.jpeg",
    location: "Piano di Sorrento, Italy ",
    github: "gdorsi",
  },
  {
    name: "Giordano Ricci",
    titles: ["Full-Stack Dev", "Observability Expert"],
    location: "Lisbon, Portugal ",
    github: "Elfo404",
    website: "https://giordanoricci.com",
    linkedin: "giordanoricci",
    image: "gio.jpg",
  },
  {
    name: "Trisha Lim",
    titles: ["Frontend Dev", "Marketing"],
    image: "trisha.png",
    location: "Lisbon, Portugal ",
    github: "trishalim",
    website: "https://trishalim.com",
  },
  {
    name: "Benjamin Leveritt",
    titles: ["Full-Stack Dev", "Technical Writer"],
    image: "benjamin.jpg",
    location: "Portsmouth, UK ",
    github: "bensleveritt",
  },
  {
    name: "Nikos Papadopoulos",
    titles: ["Full-Stack Dev"],
    location: "Farnham, UK",
    github: "4rknova",
    website: "https://www.4rknova.com",
    linkedin: "nikpapas",
    image: "nikos.png",
  },
  {
    name: "Emil Sayahi",
    titles: ["Full-Stack Dev", "Support Dev"],
    location: "San Francisco, California, US",
    github: "emmyoh",
    bluesky: "sayahi.bsky.social",
    linkedin: "emil-sayahi",
    image: "emil.jpg",
  },
  {
    name: "Meg Culotta",
    titles: ["Support Dev", "Frontend Dev"],
    location: "Minneapolis, Minnesota, US",
    github: "mculotta120",
    image: "meg.jpg",
  },
  {
    name: "Nikita Voloboev",
    location: "Barcelona, Spain",
    titles: ["Full-Stack Dev"],
    x: "nikitavoloboev",
    github: "nikitavoloboev",
    website: "https://nikiv.dev",
    image: "nikita.jpg",
  },
  {
    name: "Sammii Kellow",
    location: "London, UK",
    titles: ["Design Engineer", "Marketing"],
    x: "SammiiHaylock",
    github: "sammii-hk",
    website: "https://sammii.dev",
    linkedin: "sammii",
    image: "sammii.jpg",
  },
  {
    name: "Brad Anderson",
    location: "Atlanta, Georgia, US",
    titles: ["React Native Specialist"],
    x: "boorad",
    github: "boorad",
    bluesky: "boorad.bsky.social",
    linkedin: "boorad",
    image: "brad.png",
  },
];
