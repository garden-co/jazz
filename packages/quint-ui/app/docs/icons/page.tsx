import { HeartIcon, PlusIcon, TrashIcon } from "lucide-react";

export default function IconsPage() {
  return (
    <div>
      <h2 className="text-2xl mt-5 mb-2 font-bold">Icons</h2>

      <h3 className="text-lg mt-5 mb-2 font-bold">Import Pattern</h3>
      <p className="mb-3">
        Import icons individually from Lucide React for optimal bundle size and
        tree-shaking.
      </p>

      <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
        {`// Import individual icons
import { HeartIcon, PlusIcon, TrashIcon } from "lucide-react";

<HeartIcon />
<PlusIcon />
<TrashIcon />
`}
      </pre>

      <h3 className="text-lg mt-5 mb-2 font-bold">Styling</h3>
      <p className="mb-3">
        The Icon component is styled with Tailwind CSS classes.
      </p>

      <div className="flex gap-2 my-2">
        <HeartIcon size={24} />
        <PlusIcon size={32} />
        <TrashIcon size={16} className="text-red" />
      </div>

      <pre className="bg-stone-100 dark:bg-stone-800 p-4 rounded-lg text-sm overflow-x-auto">
        {`// Import individual icons
import { HeartIcon, PlusIcon, TrashIcon } from "lucide-react";

<HeartIcon size={24} />
<PlusIcon size={32} />
<TrashIcon size={16} className="text-red" />
`}
      </pre>
    </div>
  );
}
