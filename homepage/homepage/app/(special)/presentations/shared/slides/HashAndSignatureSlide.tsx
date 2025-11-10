import { CoValueCoreDiagram } from "../coValueDiagrams/diagrams";
import { scenario1 } from "../scenarios";

export function HashAndSignatureSlide({ progressIdx }: { progressIdx: number }) {
  return <div className="mt-[10vh]">
    <CoValueCoreDiagram
      header={scenario1.header}
      sessions={scenario1.sessions}
      showView={true}
      showCore={true}
      showHashAndSignature={true}
      encryptedItems={false}
      hashProgressIdx={progressIdx}
    />
  </div>
}