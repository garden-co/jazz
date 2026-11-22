// An npm registry that can't serve Jazz: every other name redirects to npmjs, a Jazz one is 404.
// So a dependency that isn't overridden with a candidate tarball fails the install.
import { createServer } from "node:http";
import { isJazzName } from "./names.mjs";

const server = createServer((request, response) => {
  const name = decodeURIComponent((request.url ?? "/").split("?")[0].slice(1));
  if (isJazzName(name)) {
    response.writeHead(404, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: `${name}: only the candidate's tarballs` }));
  } else {
    response.writeHead(302, { location: `https://registry.npmjs.org${request.url}` });
    response.end();
  }
});
server.listen(0, "127.0.0.1", () => console.log(`REGISTRY http://127.0.0.1:${server.address().port}/`));
