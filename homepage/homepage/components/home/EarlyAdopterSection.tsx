import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";
import { SectionHeader } from "@garden-co/design-system/src/components/molecules/SectionHeader";
import { Testimonial } from "@garden-co/design-system/src/components/molecules/Testimonial";

function TheoTestimonial({
  size,
  className,
}: { size?: "sm" | "md"; className?: string }) {
  return (
    <Testimonial
      size={size}
      name="Theo"
      role="@theo"
      imageUrl="/theo.jpg"
      darkImageUrl="/theo-dark.jpg"
      url="https://x.com/theo"
      className={className}
    >
      <p>
        I talked with the team. They work really hard. The Jazz team clearly
        cares, almost maybe too much, about making Jazz a great solution.
      </p>
      <p>
        One of the best experiences I've had working with open source devs on a
        short notice.
      </p>
    </Testimonial>
  );
}

export function EarlyAdopterSection() {
  return (
    <div className="grid grid grid-cols-3 items-center gap-y-12">
      <TheoTestimonial size="md" className="col-span-3 lg:hidden" />
      <div className="col-span-3 lg:col-span-2">
        <div className="max-w-3xl space-y-6">
          <SectionHeader
            kicker="Get started"
            title="Let's build your next app together"
          />

          <Prose className="md:text-pretty mb-6">
            <p>
              Whether you're building something big with Jazz or just trying
              things out, we've got a team of developers who have seen and built
              everything.
            </p>
            <p>
              We're happy to help you hands-on with your app, and ready to
              tailor Jazz features to your needs.
            </p>
          </Prose>

          <div className="flex gap-3">
            <Button href="/docs" variant="primary">
              Read docs
            </Button>
            <Button href="https://discord.gg/utDMjHYg42" variant="secondary">
              Join Discord
            </Button>
          </div>
        </div>
      </div>
      <TheoTestimonial
        size="sm"
        className="hidden lg:block lg:pl-8 lg:py-8 lg:border-l"
      />
    </div>
  );
}
