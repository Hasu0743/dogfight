// ================================================================
// 航空格闘戦シミュレーター - オンライン対戦 中継サーバー
// ================================================================
// このサーバーは「ゲームのロジックを一切知らない単純な中継役」です。
// - ランダムマッチのキュー管理
// - フレンドマッチの部屋コード発行・入室
// - マッチが成立した2人の間でメッセージ（機体選択・状態同期・撃墜通知）を
//   右から左へ転送するだけ
// 実際の飛行物理・命中判定はすべて各クライアント（index2.html）側で
// 計算されます（このサーバーはその結果を relay するだけ）。
//
// 使い方:
//   1. Node.js をインストール（https://nodejs.org/）
//   2. このフォルダで次を実行:
//        npm install
//        node server.js
//   3. 同じPC・同じLAN内の別プレイヤーは、index2.html の
//      「サーバーアドレス」欄に ws://<このPCのIPアドレス>:8080 を入力して接続
//      （同じPCで2タブ立ち上げて試す場合は ws://localhost:8080 のままでOK）
//
// ゲーム内の「フィードバック」ボタンから送られた内容は、同じポートの
// POST /feedback で受け取り、このフォルダの feedback.log に追記していきます。
// ================================================================

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 8080;
const FEEDBACK_FILE = path.join(__dirname, "feedback.log");
const FEEDBACK_MAX_LEN = 4000; // 1件あたりの本文の最大文字数（荒らし・誤爆対策）

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*", // file:// で開いたクライアントからも送れるようにする
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function handleFeedback(req, res) {
  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
    if (body.length > FEEDBACK_MAX_LEN * 4) req.destroy(); // 異常に大きい本文は途中で切断する
  });
  req.on("end", () => {
    let payload;
    try {
      payload = JSON.parse(body);
    } catch {
      res.writeHead(400, CORS_HEADERS);
      res.end("invalid json");
      return;
    }
    const message = String(payload.message || "").slice(0, FEEDBACK_MAX_LEN).trim();
    if (!message) {
      res.writeHead(400, CORS_HEADERS);
      res.end("empty message");
      return;
    }
    const record = {
      time: new Date().toISOString(),
      message,
      mode: payload.mode || null,
      playerAircraft: payload.playerAircraft || null,
      enemyAircraft: payload.enemyAircraft || null,
    };
    fs.appendFile(FEEDBACK_FILE, JSON.stringify(record) + "\n", (err) => {
      if (err) {
        res.writeHead(500, CORS_HEADERS);
        res.end("write failed");
        return;
      }
      res.writeHead(200, CORS_HEADERS);
      res.end("ok");
    });
  });
}

const httpServer = http.createServer((req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, CORS_HEADERS); // プリフライトリクエストへの応答
    res.end();
    return;
  }
  if (req.method === "POST" && req.url === "/feedback") {
    handleFeedback(req, res);
    return;
  }
  res.writeHead(404, CORS_HEADERS);
  res.end("not found");
});

const wss = new WebSocketServer({ server: httpServer });

// ジェット/レシプロを混戦させないよう、ランダムマッチのキューはカテゴリ別に分けて管理する
// （ゲームロジック自体はサーバーには持ち込まず、あくまで「誰と誰を組ませるか」の
// ルーティング情報としてのみカテゴリを扱う）
const CATEGORIES = ["jet", "prop"];
function normalizeCategory(c) {
  return CATEGORIES.includes(c) ? c : "jet";
}

let randomQueue = { jet: [], prop: [] }; // カテゴリ別・対戦相手を待っているソケットのFIFOキュー
const pendingRooms = new Map();  // 部屋コード -> { ws, category }（フレンドの入室を待っている情報）

const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // 紛らわしい I/O/0/1 を除外
const ROOM_CODE_LEN = 4;

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function makeRoomCode() {
  let code;
  do {
    code = Array.from({ length: ROOM_CODE_LEN }, () =>
      ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)]
    ).join("");
  } while (pendingRooms.has(code));
  return code;
}

function removeFromQueue(ws) {
  for (const cat of CATEGORIES) {
    randomQueue[cat] = randomQueue[cat].filter((s) => s !== ws);
  }
}

function removePendingRoomsFor(ws) {
  for (const [code, waiting] of pendingRooms) {
    if (waiting.ws === ws) pendingRooms.delete(code);
  }
}

// 2人を組ませて対戦成立させる（以後はこの2ソケット間でメッセージを中継する）
function pairUp(a, b, category) {
  a.opponent = b;
  b.opponent = a;
  a.myAircraft = null;
  b.myAircraft = null;
  a.wantsRematch = false;
  b.wantsRematch = false;
  send(a, { type: "matched", side: "A", category });
  send(b, { type: "matched", side: "B", category });
}

// 対戦が終わった/切断された時に、お互いの opponent 参照を解消する
function unpair(ws) {
  const opp = ws.opponent;
  ws.opponent = null;
  ws.myAircraft = null;
  ws.wantsRematch = false;
  if (opp) {
    opp.opponent = null;
    send(opp, { type: "opponent_left" });
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.opponent = null;
  ws.myAircraft = null;
  ws.wantsRematch = false;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return; // 不正なメッセージは無視
    }

    switch (msg.type) {
      case "queue_random": {
        removeFromQueue(ws);
        removePendingRoomsFor(ws);
        const category = normalizeCategory(msg.category);
        randomQueue[category].push(ws);
        send(ws, { type: "queued" });
        if (randomQueue[category].length >= 2) {
          const a = randomQueue[category].shift();
          const b = randomQueue[category].shift();
          pairUp(a, b, category);
        }
        break;
      }

      case "create_room": {
        removeFromQueue(ws);
        removePendingRoomsFor(ws);
        const code = makeRoomCode();
        const category = normalizeCategory(msg.category);
        pendingRooms.set(code, { ws, category });
        send(ws, { type: "room_created", code });
        break;
      }

      case "join_room": {
        removeFromQueue(ws);
        const code = String(msg.code || "").trim().toUpperCase().slice(0, ROOM_CODE_LEN);
        const waiting = pendingRooms.get(code);
        if (!waiting || waiting.ws === ws || waiting.ws.readyState !== WebSocket.OPEN) {
          send(ws, { type: "room_error", message: "そのコードの部屋は見つかりませんでした。" });
          break;
        }
        pendingRooms.delete(code);
        pairUp(waiting.ws, ws, waiting.category);
        break;
      }

      case "cancel": {
        removeFromQueue(ws);
        removePendingRoomsFor(ws);
        break;
      }

      case "select_aircraft": {
        if (!ws.opponent) break;
        ws.myAircraft = msg.aircraft;
        send(ws.opponent, { type: "opponent_selected", aircraft: msg.aircraft });
        if (ws.opponent.myAircraft) {
          send(ws, { type: "start" });
          send(ws.opponent, { type: "start" });
        }
        break;
      }

      case "state": {
        if (ws.opponent) send(ws.opponent, { type: "state", data: msg.data });
        break;
      }

      case "i_died": {
        if (ws.opponent) send(ws.opponent, { type: "opponent_died" });
        break;
      }

      case "rematch": {
        if (!ws.opponent) break;
        ws.wantsRematch = true;
        if (ws.opponent.wantsRematch) {
          ws.myAircraft = null;
          ws.opponent.myAircraft = null;
          ws.wantsRematch = false;
          ws.opponent.wantsRematch = false;
          send(ws, { type: "rematch_ready" });
          send(ws.opponent, { type: "rematch_ready" });
        }
        break;
      }

      case "leave_match": {
        unpair(ws);
        break;
      }
    }
  });

  ws.on("close", () => {
    removeFromQueue(ws);
    removePendingRoomsFor(ws);
    unpair(ws);
  });
});

// 切断されたまま残っているソケットを定期的に掃除する
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(heartbeat));

httpServer.listen(PORT, () => {
  console.log(`Dogfight relay server listening on ws://0.0.0.0:${PORT} (feedback: POST http://0.0.0.0:${PORT}/feedback)`);
});
