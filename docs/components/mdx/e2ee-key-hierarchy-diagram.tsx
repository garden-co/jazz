/**
 * The key hierarchy in the "Introducing E2EE in Jazz 2.0" post: each key
 * unlocks the next, from device to encrypted column, with the server beside
 * them. Drawn in currentColor so it follows the site theme.
 */
export function E2eeKeyHierarchyDiagram() {
  return (
    <svg
      viewBox="0 0 760 516"
      role="img"
      aria-label="Each key unlocks the next, from device to encrypted column"
      fontSize="11.5"
      fill="currentColor"
      className="docs-block"
      style={{ width: "100%", height: "auto" }}
    >
      <defs>
        <marker
          id="e2ee-keys-arrow"
          viewBox="0 0 10 10"
          refX="9"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M0 0L10 5L0 10z" fill="currentColor" fillOpacity="0.5" />
        </marker>
      </defs>
      <text x="24" y="34" fontSize="15" fontWeight="600">
        Each key unlocks the next, from device to encrypted column
      </text>
      <g fill="none" stroke="currentColor" strokeOpacity="0.5" strokeWidth="1.25">
        <path d="M244 132V180" markerEnd="url(#e2ee-keys-arrow)" />
        <path d="M244 252V300" markerEnd="url(#e2ee-keys-arrow)" />
        <path d="M244 372V420" markerEnd="url(#e2ee-keys-arrow)" />
      </g>
      <g fillOpacity="0.65">
        <text x="256" y="160">
          unseals the account key
        </text>
        <text x="256" y="280">
          unseals each space key
        </text>
        <text x="256" y="400">
          decrypts on read, encrypts on write
        </text>
      </g>
      <g
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.5"
        strokeWidth="1.25"
        strokeDasharray="4 4"
      >
        <path d="M504 336H464" />
        <path d="M504 456H464" />
      </g>
      <rect
        x="24"
        y="60"
        width="440"
        height="72"
        rx="8"
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.5"
        strokeWidth="1.25"
      />
      <text x="40" y="84" fontSize="13" fontWeight="600">
        Device
      </text>
      <text x="40" y="104">
        Approved from a device you already use, or restored
      </text>
      <text x="40" y="120">
        with a recovery phrase or passkey
      </text>
      <rect
        x="24"
        y="180"
        width="440"
        height="72"
        rx="8"
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.5"
        strokeWidth="1.25"
      />
      <text x="40" y="204" fontSize="13" fontWeight="600">
        Account
      </text>
      <text x="40" y="224">
        Gets its own copy of each space key, sealed to it
      </text>
      <text x="40" y="240">
        Nobody else, the server included, can unseal it
      </text>
      <rect
        x="24"
        y="300"
        width="440"
        height="72"
        rx="8"
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.5"
        strokeWidth="1.25"
      />
      <text x="40" y="324" fontSize="13" fontWeight="600">
        Space
      </text>
      <text x="40" y="344">
        One key shared by all members, e.g. a project and its tasks
      </text>
      <text x="40" y="360">
        Rotated when someone leaves; the newest key opens older ones
      </text>
      <rect
        x="24"
        y="420"
        width="440"
        height="72"
        rx="8"
        fill="currentColor"
        fillOpacity="0.08"
        stroke="currentColor"
        strokeWidth="2"
      />
      <text x="40" y="444" fontSize="13" fontWeight="600">
        Encrypted columns
      </text>
      <text x="40" y="464">
        Encrypted with XChaCha20-Poly1305 on the device
      </text>
      <text x="40" y="480">
        Optional equality token for exact-match filters
      </text>
      <rect
        x="504"
        y="300"
        width="232"
        height="192"
        rx="8"
        fill="none"
        stroke="currentColor"
        strokeOpacity="0.5"
        strokeWidth="1.25"
      />
      <text x="520" y="324" fontSize="13" fontWeight="600">
        Server
      </text>
      <text x="520" y="348">
        Stores sealed keys and
      </text>
      <text x="520" y="364">
        ciphertext, never a usable key
      </text>
      <text x="520" y="396">
        Sends only the rows your
      </text>
      <text x="520" y="412">
        read rules allow
      </text>
      <g fillOpacity="0.65">
        <text x="520" y="444">
          Still sees plain columns, who is
        </text>
        <text x="520" y="460">
          in which space, and value sizes
        </text>
      </g>
    </svg>
  );
}
