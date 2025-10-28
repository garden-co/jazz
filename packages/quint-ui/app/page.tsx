import clsx from "clsx";
import Link from "next/link";

export default function Home() {
  return (
    <main className="flex flex-col gap-2">
      <h1 className="text-3xl font-bold tracking-tight">Quint UI</h1>

      <p className="p-1">
        Quint uses a color palette which extends tailwind classes, with some
        modifications, see{" "}
        <Link
          href="https://tailwindcss.com/docs/colors#using-color-utilities"
          className="text-primary"
        >
          Tailwind Color Utilities
        </Link>{" "}
        for more infomation on basic usage.
      </p>
      <p className="mt-1 p-1">
        Nearly all use cases are encapsulated by harnessing variables which have
        a baked in light & dark mode; meaning there are only a limited number of
        variables which are required for most development.
        <span className="italic">
          To see light/dark mode toggle your system settings.
        </span>
      </p>

      <h3 id="color-variables" className="text-md mt-3 font-bold">
        Color Variables
      </h3>

      <p className="p-1">
        The following variables are available and should be used as a preference
        to tailwind classes:
      </p>

      <div className="grid grid-cols-2 gap-2 p-3">
        <div className="bg-primary text-white p-3 rounded-md">Primary</div>
        <div className="bg-highlight text-white p-3 rounded-md">Highlight</div>
        <div className="bg-tip text-white p-3 rounded-md">Tip</div>
        <div className="bg-info text-white p-3 rounded-md">Info</div>
        <div className="bg-success text-white p-3 rounded-md">Success</div>
        <div className="bg-warning text-white p-3 rounded-md">Warning</div>
        <div className="bg-alert text-white p-3 rounded-md">Alert</div>
        <div className="bg-danger text-white p-3 rounded-md">Danger</div>
        <div className="bg-muted text-white p-3 rounded-md">Muted</div>
        <div className="bg-strong text-white dark:text-stone-900 p-3 rounded-md">
          Strong
        </div>
      </div>
      <p className="text-xs mt-1 mb-4">
        NB: These classes should be used across all apps as the primary an
        secondary colour are updated per app.
        <br />
        <br />
        For full custimisation, the default colours on tailwind are programmed
        to be the tailwind semantic colours, so you can achieve a transparent
        primary with `blue/20`.
      </p>

      <h3 id="text-color-variables" className="text-md mt-3 font-bold">
        Text Color Variables
      </h3>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-2 my-3 px-3">
        <ColorTypography />
      </div>
    </main>
  );
}

const ColorTypography = () => {
  return (
    <div className={clsx("text-default rounded-md")}>
      <div className="text-default mb-1">text-default</div>
      <div className="text-muted mb-1">text-muted</div>
      <div className="text-strong mb-1">text-strong</div>
      <div className="text-strong my-1">
        <span className="bg-highlight">text-strong bg-highlight*</span>
      </div>
    </div>
  );
};
