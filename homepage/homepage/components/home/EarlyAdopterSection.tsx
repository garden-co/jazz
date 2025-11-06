"use client";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";
import { Testimonial } from "@garden-co/design-system/src/components/molecules/Testimonial";
import useEmblaCarousel from "embla-carousel-react";
import Autoplay from "embla-carousel-autoplay";
import clsx from "clsx";

const testimonials = [
  {
    name: "Theo",
    role: "@theo",
    imageUrl: "/theo.jpg",
    darkImageUrl: "/theo-dark.jpg",
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
  {
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
  {
    name: "Invoice Radar",
    role: "Technical Founder",
    content: (
      <>
        We just wanted to build a single-player experience first, planning to
        add team and org features much later. But because of Jazz, we had that
        from day one. All we needed to add was an invite button.
      </>
    ),
  },
];

function TestimonialSlider({ className }: { className?: string }) {
  const [emblaRef] = useEmblaCarousel(
    {
      loop: true,
      align: "start",
    },
    [Autoplay({ delay: 5000, stopOnInteraction: false })],
  );

  return (
    <div className={clsx("overflow-hidden", className)} ref={emblaRef}>
      <div className="flex items-start">
        {testimonials.map((testimonial, index) => (
          <div key={index} className="min-w-0 flex-[0_0_100%]">
            <Testimonial
              name={testimonial.name}
              role={testimonial.role}
              imageUrl={testimonial.imageUrl}
              darkImageUrl={testimonial.darkImageUrl}
              url={testimonial.url}
            >
              {testimonial.content}
            </Testimonial>
          </div>
        ))}
      </div>
    </div>
  );
}

export function EarlyAdopterSection() {
  return (
    <section className="bg-stone-100 dark:bg-black/30">
      <div className="container grid grid-cols-3 items-center gap-y-12">
        <div className="col-span-3 py-12 lg:col-span-2">
          <div className="max-w-3xl">
            <SectionHeader
              kicker="Get started"
              title="Let's build your next app together"
            />

            <Prose className="mb-6 md:text-pretty">
              <p>
                Whether you're building something big with Jazz or just trying
                things out, we've got a team of developers who have seen and
                built everything.
              </p>
              <p>
                We're happy to help you hands-on with your app, and ready to
                tailor Jazz features to your needs.
              </p>
            </Prose>

            <div className="flex gap-3">
              <Button href="/docs" intent="primary">
                Read docs
              </Button>
              <Button
                href="https://discord.gg/utDMjHYg42"
                intent="primary"
                variant="outline"
              >
                Join Discord
              </Button>
            </div>
          </div>
        </div>
        <TestimonialSlider className="hidden overflow-hidden lg:block lg:border-l lg:py-8 lg:pl-8" />
      </div>
    </section>
  );
}
