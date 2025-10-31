import { CoValueCoreDiagram } from "./diagrams";
import { header, scenario1 } from "./page";

export function HashAndSignatureSlide({ progressIdx }: { progressIdx: number }) {
  return <div className="mt-[10vh]">
    <CoValueCoreDiagram
      header={header}
      sessions={scenario1}
      showView={true}
      showCore={true}
      showHashAndSignature={true}
      encryptedItems={false}
      hashProgressIdx={progressIdx}
    />
  </div>
}