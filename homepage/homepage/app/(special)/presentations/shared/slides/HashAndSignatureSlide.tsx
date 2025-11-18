import { CoValueCoreDiagram } from "../coValueDiagrams/diagrams";
import { scenario1 } from "../scenarios";

export function HashAndSignatureSlide({ progressIdx, highlightGroup }: { progressIdx: number, highlightGroup?: boolean }) {
  return <div className="mx-auto">
    <CoValueCoreDiagram
      header={scenario1.header}
      sessions={scenario1.sessions}
      showView={true}
      showCore={true}
      showHashAndSignature={true}
      encryptedItems={false}
      hashProgressIdx={progressIdx}
      highlightOwner={highlightGroup}
    />
  </div>
}