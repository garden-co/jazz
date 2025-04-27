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
  },
  {
    name: "Guido D'Orsi",
    titles: ["Lead Engineer", "React Performance"],
    image: "guido.jpeg",
    location: "Piano di Sorrento, Italy ",
    github: "gdorsi",
  },
  {
    name: "Andrei Popa",
    titles: ["Full-Stack Dev", "Infra"],
    image: "andrei.jpeg",
    location: "Bucharest, Romania ",
    x: "elitepax",
    github: "pax-k",
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
    name: "Giordano Ricci",
    titles: ["Full-Stack Dev", "DevOps"],
    location: "Lisbon, Portugal ",
    linkedin: "giordanoricci",
    github: "Elfo404",
    website: "https://giordanoricci.com",
    image: "gio.jpg",
  },
  {
    name: "Nikos Papadopoulos",
    titles: ["Full-Stack Dev"],
    location: "Farnham, UK",
    website: "https://www.4rknova.com",
    linkedin: "nikpapas",
    github: "4rknova",
    image: "nikos.png",
  },
  {
    name: "Emil Sayahi",
    titles: ["Full-Stack Dev", "Support Dev"],
    location: "Oxford, Ohio, US",
    github: "emmyoh",
    linkedin: "emil-sayahi",
    bluesky: "sayahi.bsky.social",
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
    name: "James Vickery",
    location: "Birmingham, UK",
    titles: ["Full-Stack Dev", "Support Dev"],
    github: "jmsv",
    bluesky: "jmsv.bsky.social",
    image: "james.jpg",
  },
  {
    name: "Stephanie Lemmeyer",
    location: "Boston, Massachusetts, US",
    titles: ["Lead DevOps"],
    github: "slemmeyer",
    linkedin: "stephanielemmeyer",
    website: "https://stephanielemmeyer.me",
    image: "stephanie.jpg",
  },
  {
    name: "Nikita Voloboev",
    location: "Barcelona, Spain",
    titles: ["Full-Stack Dev"],
    github: "nikitavoloboev",
    x: "nikitavoloboev",
    website: "https://nikiv.dev",
    image: "nikita.jpg",
  },
];
