"use client";

import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";
import { Testimonial } from "@garden-co/design-system/src/components/molecules/Testimonial";
import { testimonials } from "@/content/testimonials";

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
        <Testimonial
          {...testimonials.theo}
          className="lg:border-l p-8 lg:pr-0 max-w-full"
        />
      </div>
    </section>
  );
}
