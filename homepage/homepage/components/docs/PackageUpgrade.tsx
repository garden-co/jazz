import { ContentByFramework } from "@/components/docs/ContentByFramework";
import { isValidFramework } from "@/lib/framework";

export interface PackageUpgradeProps {
  packageName: string;
  version: string;
  children: React.ReactNode;
}

function PackageUpgradeBody({
  packageName,
  version,
  children,
}: PackageUpgradeProps) {
  return (
    <>
      <h3 className="not-prose text-stone-900 font-semibold font-display text-lg dark:text-white flex items-center gap-2 mb-3">
        {packageName} @ {version}
      </h3>

      <div className="prose prose-p:mb-3 prose-ul:my-2 prose-headings:text-base prose-headings:font-normal prose-headings:font-sans mb-5">
        {children}
      </div>
    </>
  );
}

export function PackageUpgrade(props: PackageUpgradeProps) {
  const frameworkPackage = props.packageName.replace("jazz-", "");

  if (isValidFramework(frameworkPackage)) {
    return (
      <ContentByFramework framework={frameworkPackage}>
        <PackageUpgradeBody {...props} />
      </ContentByFramework>
    );
  }

  return <PackageUpgradeBody {...props} />;
}
