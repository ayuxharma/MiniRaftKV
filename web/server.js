const path = require("path");

const express = require("express");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const app = express();
const port = Number(process.env.PORT || 8080);
const protoPath = path.join(__dirname, "..", "proto", "raft.proto");

// These defaults match the three local Raft nodes used in the project.
// Environment variables let a container runtime provide different addresses.
const nodeAddresses = [
  process.env.RAFT_NODE_1 || "127.0.0.1:50051",
  process.env.RAFT_NODE_2 || "127.0.0.1:50052",
  process.env.RAFT_NODE_3 || "127.0.0.1:50053"
];

// Load the same service definition used by the C++ nodes.
const packageDefinition = protoLoader.loadSync(protoPath, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const raftPackage = grpc.loadPackageDefinition(packageDefinition).miniraft.rpc;
const clients = nodeAddresses.map((address) => ({
  address,
  client: new raftPackage.RaftService(
    address,
    grpc.credentials.createInsecure()
  )
}));

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Convert one callback-based gRPC request into a Promise.
function callNode(client, method, request) {
  return new Promise((resolve, reject) => {
    const options = { deadline: Date.now() + 800 };

    client[method](request, options, (error, response) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(response);
    });
  });
}

function canTryAnotherNode(error) {
  return error.code === grpc.status.FAILED_PRECONDITION ||
    error.code === grpc.status.UNAVAILABLE ||
    error.code === grpc.status.DEADLINE_EXCEEDED;
}

// The UI does not need to know which node is leader.
// Try each node until the current leader accepts the request.
async function callLeader(method, request) {
  let lastError;

  for (const node of clients) {
    try {
      return await callNode(node.client, method, request);
    } catch (error) {
      lastError = error;

      if (!canTryAnotherNode(error)) {
        throw error;
      }
    }
  }

  const error = new Error(
    lastError ? lastError.details : "No Raft node is available"
  );
  error.httpStatus = 503;
  throw error;
}

function sendError(response, error) {
  const status = error.httpStatus ||
    (error.code === grpc.status.INVALID_ARGUMENT ? 400 : 502);

  response.status(status).json({
    error: error.details || error.message || "Request failed"
  });
}

// Return the status of every node so the UI can show the cluster.
app.get("/api/status", async (request, response) => {
  const nodes = await Promise.all(
    clients.map(async (node, index) => {
      try {
        const status = await callNode(node.client, "getStatus", {});

        return {
          available: true,
          address: node.address,
          ...status
        };
      } catch (error) {
        return {
          available: false,
          nodeId: `node-${index + 1}`,
          address: node.address,
          error: error.details || "Unavailable"
        };
      }
    })
  );

  response.json({ nodes });
});

app.post("/api/kv", async (request, response) => {
  const { key, value } = request.body;

  if (typeof key !== "string" || key.length === 0 ||
      typeof value !== "string") {
    response.status(400).json({ error: "key and value must be strings" });
    return;
  }

  try {
    const result = await callLeader("set", { key, value });
    response.status(202).json({
      message: "Write accepted by the leader",
      logIndex: result.logIndex
    });
  } catch (error) {
    sendError(response, error);
  }
});

app.get("/api/kv/:key", async (request, response) => {
  try {
    const result = await callLeader("get", { key: request.params.key });
    response.json(result);
  } catch (error) {
    sendError(response, error);
  }
});

app.delete("/api/kv/:key", async (request, response) => {
  try {
    const result = await callLeader("delete", { key: request.params.key });
    response.status(202).json({
      message: "Delete accepted by the leader",
      logIndex: result.logIndex
    });
  } catch (error) {
    sendError(response, error);
  }
});

// Container health checks can use this lightweight endpoint.
app.get("/healthz", (request, response) => {
  response.json({ status: "ok" });
});

app.listen(port, "0.0.0.0", () => {
  console.log(`MiniRaftKV dashboard: http://localhost:${port}`);
});
