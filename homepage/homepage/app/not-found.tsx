import { Icon404 } from "@garden-co/design-system/src/components/atoms/icons/404";

export default function NotFound() {
  const text = "Don't Worry 'Bout Me";

  return (
    <div className="flex flex-col items-flex-start justify-center min-h-[95vh] w-auto pb-32">
      <Icon404 className="w-75" />
      <h1 className="text-3xl font-semibold my-7">{text}</h1>
      <p>Page not found.</p>
    </div>
  );
}
