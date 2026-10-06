import http from "node:http";

import { serve } from "inngest/node";

import { inngest, provisionWorkspaceWorkflow } from "./workspace/inngest.js";

const port = Number(process.env.PORT ?? 3000);

const server = http.createServer(
  serve({
    client: inngest,
    functions: [provisionWorkspaceWorkflow],
  }),
);

server.listen(port, "0.0.0.0", () => {
  console.log(`Inngest app listening on http://0.0.0.0:${port}`);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    server.close(() => {
      console.log("Inngest app stopped");
    });
  });
}
