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
const STATS_FILE = path.join(__dirname, "stats.json");
const NAME_MAX_LEN = 16;
const CHAT_MAX_LEN = 200;
const LOBBY_HISTORY_MAX = 30;        // 入室した人に見せる直近ログの件数
const LOBBY_CHAT_INTERVAL_MS = 1500; // 連投（荒らし・誤爆）を防ぐ1人あたりの最短送信間隔

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*", // file:// で開いたクライアントからも送れるようにする
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// --- 緩いランク（勝率ベースの称号）用の戦績。名前は自己申告なので、
//     アカウント認証も不正防止もしない「友達内で遊ぶ前提」の軽い仕組み。 ---
let stats = {};
try {
  stats = JSON.parse(fs.readFileSync(STATS_FILE, "utf8"));
} catch {
  stats = {};
}
function saveStats() {
  fs.writeFile(STATS_FILE, JSON.stringify(stats), () => {}); // 失敗しても対戦自体は継続させたいので結果は見ない
}
function normalizeName(raw) {
  return String(raw || "").trim().slice(0, NAME_MAX_LEN) || "名無しさん";
}

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

function handleResult(req, res) {
  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
    if (body.length > 2000) req.destroy();
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
    const name = normalizeName(payload.name);
    if (payload.result !== "win" && payload.result !== "loss") {
      res.writeHead(400, CORS_HEADERS);
      res.end("result must be 'win' or 'loss'");
      return;
    }
    const entry = stats[name] || { wins: 0, total: 0 };
    entry.total += 1;
    if (payload.result === "win") entry.wins += 1;
    stats[name] = entry;
    saveStats();
    res.writeHead(200, { ...CORS_HEADERS, "Content-Type": "application/json" });
    res.end(JSON.stringify(entry));
  });
}

// お知らせ：announcement.json を手で書き換えるだけで、次にアクセスしたプレイヤー全員に
// ポップアップで通知できる（サーバー再起動不要、ファイルが無ければ「お知らせ無し」扱い）。
const ANNOUNCEMENT_FILE = path.join(__dirname, "announcement.json");
function handleAnnouncement(req, res) {
  fs.readFile(ANNOUNCEMENT_FILE, "utf8", (err, data) => {
    let payload = { id: "", message: "" };
    if (!err) {
      try { payload = JSON.parse(data); } catch {}
    }
    res.writeHead(200, { ...CORS_HEADERS, "Content-Type": "application/json" });
    res.end(JSON.stringify(payload));
  });
}

function handleStatsLookup(req, res, url) {
  const name = normalizeName(url.searchParams.get("name"));
  const entry = stats[name] || { wins: 0, total: 0 };
  res.writeHead(200, { ...CORS_HEADERS, "Content-Type": "application/json" });
  res.end(JSON.stringify(entry));
}

// ブラウザで直接このサーバーを開いた時にゲーム画面が見られるように、
// index2_6dof.html だけをピンポイントで返す（フォルダ丸ごと配信はしない。
// feedback.log や server.js 自体が外から見えてしまう事故を防ぐため）。
const GAME_FILE = path.join(__dirname, "index2_6dof.html");
function handleGamePage(req, res) {
  fs.readFile(GAME_FILE, (err, data) => {
    if (err) {
      res.writeHead(500, CORS_HEADERS);
      res.end("game file not found");
      return;
    }
    res.writeHead(200, { ...CORS_HEADERS, "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
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
  if (req.method === "POST" && req.url === "/result") {
    handleResult(req, res);
    return;
  }
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (req.method === "GET" && url.pathname === "/stats") {
    handleStatsLookup(req, res, url);
    return;
  }
  if (req.method === "GET" && url.pathname === "/announcement") {
    handleAnnouncement(req, res);
    return;
  }
  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index2_6dof.html")) {
    handleGamePage(req, res);
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

// チームあたりの人数。1 なら従来の1対1、4 なら4対4。
const TEAM_SIZES = [1, 4];
function normalizeTeamSize(n) {
  return TEAM_SIZES.includes(n) ? n : 1;
}
const MAX_PER_TEAM = 4;

// ランダムマッチのキュー："<category>:<teamSize>" -> 待っているソケットのFIFO
const randomQueues = new Map();
function queueKey(category, size) {
  return `${category}:${size}`;
}

const rooms = new Map();         // roomId -> room
const pendingRooms = new Map();  // 部屋コード -> room（コードで参加できる部屋）

const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // 紛らわしい I/O/0/1 を除外
const ROOM_CODE_LEN = 4;
let nextRoomId = 1;
let nextPlayerId = 1;

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

// ================================================================
// 部屋：1対1も4対4も「2チームからなる部屋」という同じ形で扱う。
// サーバーは誰がどのチームかと、機体選択・撃墜・決着の進行だけを見る
// （飛行物理と命中判定は今まで通り各クライアントが計算する）。
// ================================================================
function createRoom(category, teamSize, code) {
  const room = {
    id: nextRoomId++,
    code: code || null,
    category: normalizeCategory(category),
    teamSize: normalizeTeamSize(teamSize),
    members: [],       // ws の配列
    hostId: null,      // ws.playerId
    phase: "lobby",    // "lobby" | "select" | "playing"
  };
  rooms.set(room.id, room);
  return room;
}

function roomOf(ws) {
  return ws.roomId ? rooms.get(ws.roomId) || null : null;
}

function teamCount(room, team) {
  return room.members.filter((m) => m.team === team).length;
}

function addToRoom(room, ws, team) {
  ws.roomId = room.id;
  ws.playerId = ws.playerId || `p${nextPlayerId++}`;
  ws.team = team || (teamCount(room, "A") <= teamCount(room, "B") ? "A" : "B");
  ws.aircraft = null;
  ws.alive = true;
  ws.wantsRematch = false;
  room.members.push(ws);
  if (!room.hostId) room.hostId = ws.playerId;
}

function removeFromRoom(ws) {
  const room = roomOf(ws);
  ws.roomId = null;
  if (!room) return null;
  room.members = room.members.filter((m) => m !== ws);
  if (room.code && room.members.length === 0) pendingRooms.delete(room.code);
  if (room.members.length === 0) {
    rooms.delete(room.id);
    return null;
  }
  if (room.hostId === ws.playerId) room.hostId = room.members[0].playerId; // ホストが抜けたら次の人へ
  return room;
}

function rosterOf(room) {
  return room.members.map((m) => ({
    id: m.playerId,
    name: m.name || "名無しさん",
    team: m.team,
    aircraft: m.aircraft || null,
    alive: m.alive,
  }));
}

function broadcastRoom(room, msg, except) {
  for (const m of room.members) {
    if (m !== except) send(m, msg);
  }
}

function sendRoomUpdate(room) {
  const players = rosterOf(room);
  for (const m of room.members) {
    send(m, {
      type: "room_update",
      you: m.playerId,
      host: room.hostId,
      code: room.code,
      category: room.category,
      teamSize: room.teamSize,
      players,
    });
  }
}

// 部屋のメンバーを確定させ、機体選択フェーズに入る
function beginSelect(room) {
  room.phase = "select";
  room.members.forEach((m) => { m.aircraft = null; m.alive = true; m.wantsRematch = false; });
  const players = rosterOf(room);
  for (const m of room.members) {
    send(m, { type: "match_ready", you: m.playerId, category: room.category, players });
  }
}

// 全員が機体を選んだら開戦
function startIfAllSelected(room) {
  if (room.phase !== "select") return;
  if (!room.members.every((m) => m.aircraft)) return;
  room.phase = "playing";
  broadcastRoom(room, { type: "start", players: rosterOf(room) });
}

// 生き残っているチームが1つだけになったら決着
function checkMatchOver(room, reason) {
  if (room.phase !== "playing") return;
  const aliveTeams = ["A", "B"].filter((t) => room.members.some((m) => m.team === t && m.alive));
  if (aliveTeams.length > 1) return;
  room.phase = "lobby";
  broadcastRoom(room, {
    type: "match_over",
    winTeam: aliveTeams[0] || null,
    reason: reason || (aliveTeams.length === 0 ? "相打ちになりました。" : undefined),
  });
}

function removeFromQueues(ws) {
  for (const [key, q] of randomQueues) {
    const filtered = q.filter((s) => s !== ws);
    if (filtered.length === 0) randomQueues.delete(key);
    else randomQueues.set(key, filtered);
  }
}

// キューに必要人数（teamSize×2）が溜まったら部屋を作って開始する
function tryFormRandomMatch(category, size) {
  const key = queueKey(category, size);
  const q = randomQueues.get(key) || [];
  const needed = size * 2;
  while (q.length >= needed) {
    const picked = q.splice(0, needed);
    const room = createRoom(category, size, null);
    picked.forEach((ws, i) => addToRoom(room, ws, i % 2 === 0 ? "A" : "B"));
    beginSelect(room);
  }
  if (q.length === 0) randomQueues.delete(key);
  else randomQueues.set(key, q);
}

function notifyQueueProgress(category, size) {
  const q = randomQueues.get(queueKey(category, size)) || [];
  for (const ws of q) send(ws, { type: "queued", size, count: q.length });
}

// 対戦から抜ける／切断されたときの後始末。残ったメンバーには誰が抜けたかを伝え、
// それで決着がついたなら勝敗も配信する。
function closeRoom(room, message) {
  broadcastRoom(room, { type: "room_closed", message });
  room.members.forEach((m) => { m.roomId = null; });
  if (room.code) pendingRooms.delete(room.code);
  rooms.delete(room.id);
}

function leaveRoom(ws) {
  const room = roomOf(ws);
  if (!room) return;
  const phase = room.phase;
  const leftId = ws.playerId;
  const remaining = removeFromRoom(ws);
  if (!remaining) return; // 最後の1人が抜けた＝部屋ごと消滅済み

  if (phase === "playing") {
    // 対戦中の退出は撃墜と同じ扱い。それで片方のチームが全滅すれば決着する。
    broadcastRoom(remaining, { type: "player_left", id: leftId });
    checkMatchOver(remaining, "参加者が退出したため対戦終了です。");
  } else if (phase === "select") {
    // 機体選択中に抜けられると全員が待ち続けてしまうので、その回は中止する
    closeRoom(remaining, "参加者が退出したため、対戦の準備を中止しました。");
  } else {
    // 待合室では、残った人はそのまま待てる
    sendRoomUpdate(remaining);
  }
}

// --- グローバルチャット（ロビー）---
// 対戦の中継とは別系統の仕組み。メニュー画面を開いているクライアントが lobby_join で
// 参加し、発言は参加者全員へ配信する。対戦マッチングとは無関係なので、
// ここに入っていること／出ることが対戦の成立に影響することはない。
const lobby = new Set();
let lobbyHistory = []; // 直近 LOBBY_HISTORY_MAX 件の { name, text }

function broadcastLobby(msg, except) {
  for (const sock of lobby) {
    if (sock !== except) send(sock, msg);
  }
}

function broadcastLobbyCount() {
  broadcastLobby({ type: "lobby_count", count: lobby.size });
}

// --- フレンド（オンライン状況と招待）---
// アカウント認証は無く、名前は完全な自己申告。したがってこれは「同じ名前を名乗っている人が
// 今ロビーに居るか」を見せているに過ぎず、なりすましは防げない。招待で渡るのも
// ルームコードだけなので、悪用されても部屋が荒れる以上のことは起きない、という前提の設計。
const INVITE_INTERVAL_MS = 3000; // 招待の連打防止

function lobbyNames() {
  const names = new Set();
  for (const sock of lobby) {
    if (sock.lobbyName) names.add(sock.lobbyName);
  }
  return [...names];
}

function broadcastLobbyNames() {
  broadcastLobby({ type: "lobby_names", names: lobbyNames() });
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.roomId = null;
  ws.playerId = null;
  ws.name = null;

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
        removeFromQueues(ws);
        leaveRoom(ws);
        const category = normalizeCategory(msg.category);
        const size = normalizeTeamSize(msg.size);
        ws.name = normalizeName(msg.name);
        ws.queueCategory = category;
        ws.queueSize = size;
        const key = queueKey(category, size);
        const q = randomQueues.get(key) || [];
        q.push(ws);
        randomQueues.set(key, q);
        notifyQueueProgress(category, size);
        tryFormRandomMatch(category, size);
        break;
      }

      case "create_room": {
        removeFromQueues(ws);
        leaveRoom(ws);
        const code = makeRoomCode();
        const room = createRoom(msg.category, msg.size, code);
        pendingRooms.set(code, room);
        addToRoom(room, ws, "A");
        ws.name = normalizeName(msg.name);
        send(ws, { type: "room_created", code });
        sendRoomUpdate(room);
        break;
      }

      case "join_room": {
        removeFromQueues(ws);
        const code = String(msg.code || "").trim().toUpperCase().slice(0, ROOM_CODE_LEN);
        const room = pendingRooms.get(code);
        if (!room || room.phase !== "lobby" || roomOf(ws) === room) {
          send(ws, { type: "room_error", message: "そのコードの部屋は見つかりませんでした。" });
          break;
        }
        if (room.members.length >= MAX_PER_TEAM * 2) {
          send(ws, { type: "room_error", message: "その部屋は満員です。" });
          break;
        }
        leaveRoom(ws);
        addToRoom(room, ws, null); // 少ない方のチームへ自動で割り振る
        ws.name = normalizeName(msg.name);
        sendRoomUpdate(room);
        break;
      }

      case "switch_team": {
        const room = roomOf(ws);
        if (!room || room.phase !== "lobby") break;
        const to = ws.team === "A" ? "B" : "A";
        if (teamCount(room, to) >= MAX_PER_TEAM) break; // 定員オーバーのチームには移れない
        ws.team = to;
        sendRoomUpdate(room);
        break;
      }

      case "room_start": {
        const room = roomOf(ws);
        if (!room || room.phase !== "lobby") break;
        if (room.hostId !== ws.playerId) break; // 開始できるのはホストだけ
        if (!["A", "B"].every((t) => teamCount(room, t) > 0)) break;
        if (room.code) pendingRooms.delete(room.code); // 開始後は途中参加させない
        beginSelect(room);
        break;
      }

      case "cancel": {
        removeFromQueues(ws);
        leaveRoom(ws);
        break;
      }

      case "select_aircraft": {
        const room = roomOf(ws);
        if (!room) break;
        ws.aircraft = msg.aircraft;
        ws.name = normalizeName(msg.name);
        broadcastRoom(room, { type: "roster_update", players: rosterOf(room) });
        startIfAllSelected(room);
        break;
      }

      case "state": {
        const room = roomOf(ws);
        if (!room) break;
        // 自分の状態を、同じ部屋の他の全員へそのまま中継する
        broadcastRoom(room, { type: "state", id: ws.playerId, data: msg.data }, ws);
        break;
      }

      case "i_died": {
        const room = roomOf(ws);
        if (!room || !ws.alive) break;
        ws.alive = false;
        broadcastRoom(room, { type: "player_died", id: ws.playerId }, ws);
        checkMatchOver(room);
        break;
      }

      case "chat": {
        const room = roomOf(ws);
        if (!room) break;
        const text = String(msg.text || "").slice(0, CHAT_MAX_LEN).trim();
        if (!text) break;
        broadcastRoom(room, { type: "chat", name: normalizeName(msg.name), text }, ws);
        break;
      }

      case "lobby_join": {
        lobby.add(ws);
        ws.lobbyName = normalizeName(msg.name);
        send(ws, {
          type: "lobby_history",
          messages: lobbyHistory,
          count: lobby.size,
          names: lobbyNames(),
        });
        broadcastLobbyCount();
        broadcastLobbyNames();
        break;
      }

      case "lobby_rename": {
        if (!lobby.has(ws)) break;
        ws.lobbyName = normalizeName(msg.name);
        broadcastLobbyNames();
        break;
      }

      case "friend_invite": {
        if (!lobby.has(ws)) break;
        const now = Date.now();
        if (ws.lastInviteAt && now - ws.lastInviteAt < INVITE_INTERVAL_MS) break;
        const to = normalizeName(msg.to);
        const code = String(msg.code || "").trim().toUpperCase().slice(0, ROOM_CODE_LEN);
        if (!code) break;
        ws.lastInviteAt = now;
        // 同じ名前を名乗っている人全員に届く（誰が本物かはサーバーには分からない）
        for (const sock of lobby) {
          if (sock !== ws && sock.lobbyName === to) {
            send(sock, { type: "friend_invited", from: ws.lobbyName, code });
          }
        }
        break;
      }

      case "lobby_leave": {
        if (lobby.delete(ws)) { broadcastLobbyCount(); broadcastLobbyNames(); }
        break;
      }

      case "lobby_chat": {
        if (!lobby.has(ws)) break; // 参加していないソケットからの発言は無視する
        const now = Date.now();
        if (ws.lastLobbyChatAt && now - ws.lastLobbyChatAt < LOBBY_CHAT_INTERVAL_MS) break;
        const text = String(msg.text || "").slice(0, CHAT_MAX_LEN).trim();
        if (!text) break;
        ws.lastLobbyChatAt = now;
        const entry = { name: normalizeName(msg.name), text };
        lobbyHistory.push(entry);
        if (lobbyHistory.length > LOBBY_HISTORY_MAX) lobbyHistory = lobbyHistory.slice(-LOBBY_HISTORY_MAX);
        // 送信者の画面には自分の発言を即座に出しているので、ここでは本人以外へ配る
        broadcastLobby({ type: "lobby_chat", name: entry.name, text: entry.text }, ws);
        break;
      }

      case "rematch": {
        const room = roomOf(ws);
        if (!room) break;
        ws.wantsRematch = true;
        if (room.members.every((m) => m.wantsRematch)) {
          room.phase = "select";
          room.members.forEach((m) => { m.aircraft = null; m.alive = true; m.wantsRematch = false; });
          broadcastRoom(room, { type: "rematch_ready", players: rosterOf(room) });
        }
        break;
      }

      case "leave_match": {
        leaveRoom(ws);
        break;
      }
    }
  });

  ws.on("close", () => {
    removeFromQueues(ws);
    leaveRoom(ws);
    if (lobby.delete(ws)) { broadcastLobbyCount(); broadcastLobbyNames(); }
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
