import theoImage from "./images/theo.jpg";
import theoDarkImage from "./images/theo-dark.jpg";

export const testimonials = {
  theo: {
    name: "Theo",
    role: "@theo",
    image: theoImage,
    darkImage: theoDarkImage,
    url: "https://x.com/theo",
    content: (
      <>
        <p>
          I talked with the team. They work really hard. The Jazz team clearly
          cares, almost maybe too much, about making Jazz a great solution.
        </p>
        <p>
          One of the best experiences I've had working with open source devs on
          a short notice.
        </p>
      </>
    ),
  },
  spreadsheetApp: {
    name: "Spreadsheet app (stealth)",
    role: "CTO",
    content: (
      <p>
        You don&apos;t have to think about deploying a database, SQL schemas,
        relations, and writing queries… Basically, if you know TypeScript, you
        know Jazz , and you can ship an app. It&apos;s just so nice!
      </p>
    ),
  },
  invoiceRadar: {
    name: "Invoice Radar",
    role: "Technical Founder",
    content: (
      <>
        We just wanted to build a single-player experience first, planning to
        add team features much later. But because of Jazz, we had orgs
        from day one. All we needed to add was an invite button.
      </>
    ),
  },
};
