const nodesBody = document.querySelector("#nodes");
const updated = document.querySelector("#updated");
const keyInput = document.querySelector("#key");
const valueInput = document.querySelector("#value");
const result = document.querySelector("#result");

// Add one safe text cell without inserting HTML from the server.
function addCell(row, value, className = "") {
  const cell = document.createElement("td");
  cell.textContent = value;
  cell.className = className;
  row.append(cell);
}

async function refreshStatus() {
  try {
    const response = await fetch("/api/status");
    const data = await response.json();
    nodesBody.replaceChildren();

    for (const node of data.nodes) {
      const row = document.createElement("tr");
      addCell(row, node.nodeId);

      if (!node.available) {
        addCell(row, "Offline", "offline");
        addCell(row, "-");
        addCell(row, "-");
        addCell(row, "-");
        addCell(row, "-");
      } else {
        const roleClass = node.role.toLowerCase() === "leader" ? "leader" : "";
        addCell(row, node.role, roleClass);
        addCell(row, node.currentTerm);
        addCell(row, node.leaderId || "Electing");
        addCell(row, node.commitIndex);
        addCell(row, node.lastLogIndex);
      }

      nodesBody.append(row);
    }

    updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    updated.textContent = "Gateway unavailable";
  }
}

function show(message, type) {
  result.textContent = message;
  result.className = type;
}

function getKey() {
  const key = keyInput.value.trim();

  if (!key) {
    show("Enter a key first.", "error");
    return null;
  }

  return key;
}

async function readResponse(response) {
  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || "Request failed");
  }

  return data;
}

document.querySelector("#set").addEventListener("click", async () => {
  const key = getKey();

  if (!key) {
    return;
  }

  try {
    const response = await fetch("/api/kv", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key, value: valueInput.value })
    });
    const data = await readResponse(response);
    show(`${data.message}. Log index: ${data.logIndex}`, "success");
  } catch (error) {
    show(error.message, "error");
  }
});

document.querySelector("#get").addEventListener("click", async () => {
  const key = getKey();

  if (!key) {
    return;
  }

  try {
    const response = await fetch(`/api/kv/${encodeURIComponent(key)}`);
    const data = await readResponse(response);

    if (data.found) {
      valueInput.value = data.value;
      show(`Stored value: ${data.value}`, "success");
    } else {
      show("That key does not exist.", "error");
    }
  } catch (error) {
    show(error.message, "error");
  }
});

document.querySelector("#delete").addEventListener("click", async () => {
  const key = getKey();

  if (!key) {
    return;
  }

  try {
    const response = await fetch(`/api/kv/${encodeURIComponent(key)}`, {
      method: "DELETE"
    });
    const data = await readResponse(response);
    show(`${data.message}. Log index: ${data.logIndex}`, "success");
  } catch (error) {
    show(error.message, "error");
  }
});

refreshStatus();
setInterval(refreshStatus, 2000);
