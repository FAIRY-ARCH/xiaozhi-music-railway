// xiaozhi-music-bridge
// 桥接：小智 MCP 中继 <-> 上游音乐 MCP WebSocket 服务器（双向转发 JSON-RPC 消息）
const RELAY_URL = Deno.env.get("RELAY_URL");
const UPSTREAM_URL = Deno.env.get("UPSTREAM_URL") ?? "wss://xiaozhi-music-railway.fairy-arch.deno.net";
if (!RELAY_URL) {
  console.error("RELAY_URL not set");
  Deno.exit(1);
}

// 诊断状态
const state = {
  relay: "not-started",
  upstream: "not-started",
  lastError: "",
  attempts: 0,
  lastConnect: 0,
  msgsRelayToUp: 0,
  msgsUpToRelay: 0,
};

function open(ws, name) {
  return new Promise((resolve, reject) => {
    state[name] = "opening";
    ws.onopen = () => { state[name] = "open"; resolve(); };
    ws.onerror = () => { state[name] = "error"; state.lastError = name + " open failed"; reject(new Error(name + " open failed")); };
  });
}

async function pipe(src, dst, name) {
  for await (const msg of src) {
    if (typeof msg === "string") {
      dst.send(msg);
    } else {
      dst.send(await new Response(msg).text());
    }
    if (name === "relay") state.msgsRelayToUp++; else state.msgsUpToRelay++;
  }
}

async function runBridge() {
  for (;;) {
    let relay;
    let up;
    try {
      state.attempts++;
      state.lastConnect = Date.now();
      relay = new WebSocket(RELAY_URL);
      up = new WebSocket(UPSTREAM_URL);
      await Promise.all([open(relay, "relay"), open(up, "upstream")]);
      console.log("bridge connected: relay + upstream");
      await Promise.race([pipe(relay, up, "relay"), pipe(up, relay, "upstream")]);
    } catch (err) {
      state.lastError = String(err && err.message ? err.message : err);
      console.error("bridge error:", err);
    } finally {
      try { relay?.close(); } catch {}
      try { up?.close(); } catch {}
      state.relay = "closed";
      state.upstream = "closed";
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
}

Deno.serve((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/health") return new Response("ok");
  if (url.pathname === "/state") return Response.json(state);
  return new Response("xiaozhi-music-bridge running", { status: 200 });
});
runBridge();
