// xiaozhi-music-bridge
// 桥接：小智 MCP 中继 <-> 上游音乐 MCP WebSocket 服务器（双向转发 JSON-RPC 消息）
const RELAY_URL = Deno.env.get("RELAY_URL");
const UPSTREAM_URL = Deno.env.get("UPSTREAM_URL") ?? "wss://xiaozhi-music-railway.fairy-arch.deno.net";

if (!RELAY_URL) {
  console.error("RELAY_URL not set");
  Deno.exit(1);
}

function open(ws) {
  return new Promise((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = () => reject(new Error("websocket open failed"));
  });
}

async function pipe(src, dst) {
  for await (const msg of src) {
    if (typeof msg === "string") {
      dst.send(msg);
    } else {
      dst.send(await new Response(msg).text());
    }
  }
}

async function runBridge() {
  for (;;) {
    let relay;
    let up;
    try {
      relay = new WebSocket(RELAY_URL);
      up = new WebSocket(UPSTREAM_URL);
      await Promise.all([open(relay), open(up)]);
      console.log("bridge connected: relay + upstream");
      await Promise.race([pipe(relay, up), pipe(up, relay)]);
    } catch (err) {
      console.error("bridge error:", err);
    } finally {
      try { relay?.close(); } catch {}
      try { up?.close(); } catch {}
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
}

Deno.serve((req) => {
  if (new URL(req.url).pathname === "/health") return new Response("ok");
  return new Response("xiaozhi-music-bridge running", { status: 200 });
});

runBridge();
