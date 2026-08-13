// ==========================================================================
// 答えてネット（モダン版） - server.js
// Node.js + Express + Socket.io によるリアルタイム投票/アンケートシステム
// データは全てメモリ上に保持（ルーム単位。DB不要・Render無料プラン想定）
// ==========================================================================

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const QRCode = require('qrcode');
const { customAlphabet } = require('nanoid');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;

// ---------------------------------------------------------------------
// スリープ防止（self-ping）設定
// ルームの作成・利用があってから KEEP_AWAKE_HOURS の間だけ、
// アプリ自身が PING_INTERVAL_MINUTES ごとに自分の公開URLへリクエストを送ることで
// Renderの無料プランが15分無アクセスでスリープするのを防ぐ。
// 一定時間アクティビティがなければ自動的に停止し、夜間・休日は通常通りスリープする。
// ---------------------------------------------------------------------
const KEEP_AWAKE_HOURS = Number(process.env.KEEP_AWAKE_HOURS) || 3;
const PING_INTERVAL_MINUTES = Number(process.env.PING_INTERVAL_MINUTES) || 10;
// RenderのWebサービスには RENDER_EXTERNAL_URL が自動的に設定される。
// 他のホスティングを使う場合は SELF_URL を手動で設定すれば同じ仕組みが使える。
const SELF_URL = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || null;

let lastActivityAt = 0; // 最後にルームが使われた時刻（0=まだ一度も使われていない）
function touchActivity() {
  lastActivityAt = Date.now();
}

// 4桁の数字コードを生成（キーワード未指定時に使用）
const genCode = customAlphabet('0123456789', 4);

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// 自己pingが叩く軽量なヘルスチェック用エンドポイント
// （外部の監視サービスを使う場合もここを指定すればよい）
app.get('/api/health', (req, res) => {
  res.status(200).json({ ok: true, time: Date.now() });
});


// ---------------------------------------------------------------------
// ルーム管理（メモリ上）
// rooms: { [roomId]: RoomState }
// RoomState = {
//   password,
//   question: { id, code, type: 'choice'|'text'|'wordcloud', title,
//               choices, anonymous, color, status: 'open'|'closed', createdAt } | null,
//   answers: [{ choiceIndex? , text?, name? }],
//   lastActivity: timestamp   // 未使用ルームの掃除用
// }
// ---------------------------------------------------------------------
const rooms = new Map();

const ROOM_ID_RE = /^[0-9A-Za-z_-]{2,40}$/;

function getOrTouchRoom(roomId) {
  const r = rooms.get(roomId);
  if (r) r.lastActivity = Date.now();
  return r;
}

function publicQuestionView(room) {
  if (!room || !room.question) return null;
  const q = room.question;
  return {
    id: q.id,
    code: q.code,
    type: q.type,
    title: q.title,
    choices: q.choices || null,
    anonymous: q.anonymous,
    color: q.color,
    status: q.status,
  };
}

function normalizeWord(s) {
  return String(s).trim().replace(/\s+/g, ' ').toLowerCase();
}

function computeResults(room) {
  if (!room || !room.question) return null;
  const q = room.question;
  const answers = room.answers;

  if (q.type === 'choice') {
    const counts = new Array(q.choices.length).fill(0);
    for (const a of answers) {
      if (a.choiceIndex >= 0 && a.choiceIndex < counts.length) counts[a.choiceIndex]++;
    }
    return { type: 'choice', counts, total: answers.length, choices: q.choices, anonymous: q.anonymous };
  }

  if (q.type === 'wordcloud') {
    const freq = new Map(); // normalized -> {text(display, first-seen), count}
    for (const a of answers) {
      const norm = normalizeWord(a.text || '');
      if (!norm) continue;
      if (!freq.has(norm)) freq.set(norm, { text: a.text.trim(), count: 0 });
      freq.get(norm).count++;
    }
    const words = Array.from(freq.values()).sort((a, b) => b.count - a.count);
    return { type: 'wordcloud', words, total: answers.length, anonymous: q.anonymous };
  }

  // free text
  return {
    type: 'text',
    items: answers.map((a) => ({ text: a.text, name: q.anonymous ? null : (a.name || null) })),
    total: answers.length,
    anonymous: q.anonymous,
  };
}

function broadcastResults(roomId, room) {
  io.to(`room:${roomId}`).emit('results:update', computeResults(room));
}

function broadcastQuestion(roomId, room) {
  io.to(`room:${roomId}`).emit('question:update', publicQuestionView(room));
}

// 未使用ルームを定期的に掃除（24時間操作がなければ破棄。メモリ節約）
setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [id, r] of rooms) {
    if (r.lastActivity < cutoff) rooms.delete(id);
  }
}, 60 * 60 * 1000);

// ---------------------------------------------------------------------
// QRコード生成API（ルームの現在の設問への直接URLをQR化）
// ---------------------------------------------------------------------
app.get('/api/qr', async (req, res) => {
  const roomId = String(req.query.room || '');
  const room = rooms.get(roomId);
  if (!room || !room.question) return res.status(404).json({ error: 'no active question' });
  const proto = req.headers['x-forwarded-proto'] || req.protocol;
  const base = `${proto}://${req.headers.host}`;
  const url = `${base}/?room=${encodeURIComponent(roomId)}&code=${encodeURIComponent(room.question.code)}`;
  try {
    const dataUrl = await QRCode.toDataURL(url, { width: 280, margin: 1 });
    res.json({ url, dataUrl });
  } catch (e) {
    res.status(500).json({ error: 'qr generation failed' });
  }
});

// ---------------------------------------------------------------------
// Socket.io イベント
// ---------------------------------------------------------------------
io.on('connection', (socket) => {
  socket.data.teacherRoom = null; // 認証済みなら roomId が入る

  // ルームへのログイン（未作成のルームIDなら、そのパスワードで新規作成される）
  socket.on('room:auth', ({ roomId, password }, cb) => {
    roomId = String(roomId || '').trim();
    password = String(password || '');

    if (!ROOM_ID_RE.test(roomId)) {
      if (cb) cb({ ok: false, error: 'ルームIDは半角英数字・ハイフン・アンダースコアで2〜40文字にしてください。' });
      return;
    }
    if (!password) {
      if (cb) cb({ ok: false, error: 'パスワードを入力してください。' });
      return;
    }

    let room = rooms.get(roomId);
    if (!room) {
      // 新規ルーム作成
      room = { password, question: null, answers: [], lastActivity: Date.now() };
      rooms.set(roomId, room);
    } else if (room.password !== password) {
      if (cb) cb({ ok: false, error: 'ルームIDまたはパスワードが違います。' });
      return;
    }
    room.lastActivity = Date.now();
    touchActivity();

    socket.data.teacherRoom = roomId;
    socket.join(`room:${roomId}`);

    if (cb) {
      cb({
        ok: true,
        question: publicQuestionView(room),
        results: computeResults(room),
      });
    }
  });

  // 学生がルーム+コードで設問を照会（学生ソケットもそのルームに join し、以後の更新を受け取る）
  socket.on('code:check', ({ roomId, code }, cb) => {
    roomId = String(roomId || '').trim();
    const room = getOrTouchRoom(roomId);
    if (!room || !room.question || room.question.code !== String(code || '').trim()) {
      if (cb) cb({ ok: false, error: 'コードが見つかりません。確認してもう一度お試しください。' });
      return;
    }
    if (room.question.status !== 'open') {
      if (cb) cb({ ok: false, error: 'この設問は現在回答を受け付けていません。' });
      return;
    }
    socket.data.studentRoom = roomId;
    socket.join(`room:${roomId}`);
    if (cb) cb({ ok: true, question: publicQuestionView(room) });
  });

  // 新しい設問を作成（認証済みの先生のみ）
  socket.on('question:create', (payload, cb) => {
    const roomId = socket.data.teacherRoom;
    const room = roomId && rooms.get(roomId);
    if (!room) {
      if (cb) cb({ ok: false, error: '認証が必要です。' });
      return;
    }
    const { type, title, choices, keyword, anonymous } = payload || {};

    if (!['choice', 'text', 'wordcloud'].includes(type)) {
      if (cb) cb({ ok: false, error: '設問タイプが不正です。' });
      return;
    }
    if (type === 'choice') {
      if (!Array.isArray(choices) || choices.length < 2 || choices.length > 6) {
        if (cb) cb({ ok: false, error: '選択肢は2〜6個で指定してください。' });
        return;
      }
    }

    let code = (keyword || '').trim();
    if (code) {
      if (!/^[0-9A-Za-z]{1,30}$/.test(code)) {
        if (cb) cb({ ok: false, error: 'キーワードは半角英数字30文字以内で入力してください。' });
        return;
      }
    } else {
      code = genCode();
    }

    const colors = ['#ccffff', '#ffff99', '#ff99ff', '#ffcc00', '#ccff00'];
    const color = colors[Math.floor(Math.random() * colors.length)];

    const titleDefault = { choice: '設問', text: '自由記述', wordcloud: '一言でどうぞ' }[type];

    room.question = {
      id: Date.now().toString(36),
      code,
      type,
      title: (title || '').trim() || titleDefault,
      choices: type === 'choice' ? choices.map((c) => String(c).trim()).filter(Boolean) : null,
      anonymous: anonymous !== false, // デフォルト匿名
      color,
      status: 'open',
      createdAt: new Date().toISOString(),
    };
    room.answers = [];
    room.lastActivity = Date.now();
    touchActivity();

    broadcastQuestion(roomId, room);
    broadcastResults(roomId, room);
    if (cb) cb({ ok: true, question: publicQuestionView(room) });
  });

  // 設問の受付終了（先生のみ）
  socket.on('question:close', (cb) => {
    const roomId = socket.data.teacherRoom;
    const room = roomId && rooms.get(roomId);
    if (!room) {
      if (cb) cb({ ok: false, error: '認証が必要です。' });
      return;
    }
    if (room.question) {
      room.question.status = 'closed';
      broadcastQuestion(roomId, room);
    }
    if (cb) cb({ ok: true });
  });

  // 回答送信
  socket.on('answer:submit', (payload, cb) => {
    const { roomId, code, choiceIndex, text, name } = payload || {};
    const room = rooms.get(String(roomId || '').trim());
    if (!room || !room.question || room.question.code !== String(code || '').trim()) {
      if (cb) cb({ ok: false, error: 'この設問は終了したか、コードが正しくありません。' });
      return;
    }
    const q = room.question;
    if (q.status !== 'open') {
      if (cb) cb({ ok: false, error: '受付は終了しました。' });
      return;
    }

    const entry = {};
    if (!q.anonymous) {
      const n = String(name || '').trim();
      if (!n) {
        if (cb) cb({ ok: false, error: '記名式の設問です。お名前を入力してください。' });
        return;
      }
      entry.name = n.slice(0, 40);
    }

    if (q.type === 'choice') {
      const idx = Number(choiceIndex);
      if (!Number.isInteger(idx) || idx < 0 || idx >= q.choices.length) {
        if (cb) cb({ ok: false, error: '選択肢が不正です。' });
        return;
      }
      entry.choiceIndex = idx;
    } else {
      const t = String(text || '').trim();
      const maxLen = q.type === 'wordcloud' ? 30 : 200;
      if (!t) {
        if (cb) cb({ ok: false, error: '回答を入力してください。' });
        return;
      }
      if (t.length > maxLen) {
        if (cb) cb({ ok: false, error: `回答は${maxLen}文字以内で入力してください。` });
        return;
      }
      entry.text = t;
    }

    room.answers.push(entry);
    room.lastActivity = Date.now();
    touchActivity();
    broadcastResults(roomId, room);
    if (cb) cb({ ok: true });
  });

  socket.on('disconnect', () => {
    // 特に処理不要（ルーム状態はソケットに紐付かない）
  });
});

// ---------------------------------------------------------------------
// self-ping ループ：直近のアクティビティから KEEP_AWAKE_HOURS 以内なら、
// PING_INTERVAL_MINUTES ごとに自分自身の /api/health を叩いてスリープを防ぐ
// ---------------------------------------------------------------------
if (SELF_URL) {
  setInterval(async () => {
    if (lastActivityAt === 0) return; // まだ一度もルームが使われていない
    const elapsedMs = Date.now() - lastActivityAt;
    if (elapsedMs > KEEP_AWAKE_HOURS * 60 * 60 * 1000) return; // 目覚めさせておく期間を過ぎた

    try {
      const url = `${SELF_URL.replace(/\/$/, '')}/api/health`;
      const res = await fetch(url);
      console.log(`[keep-awake] self-ping ${res.status} at ${new Date().toISOString()}`);
    } catch (e) {
      console.log(`[keep-awake] self-ping failed: ${e.message}`);
    }
  }, PING_INTERVAL_MINUTES * 60 * 1000);
} else {
  console.log('[keep-awake] SELF_URL / RENDER_EXTERNAL_URL が未設定のため self-ping は無効です（ローカル開発では問題ありません）。');
}

server.listen(PORT, () => {
  console.log(`答えてネット（モダン版） running on http://localhost:${PORT}`);
  if (SELF_URL) {
    console.log(`[keep-awake] 有効: ルーム利用から${KEEP_AWAKE_HOURS}時間、${PING_INTERVAL_MINUTES}分おきに self-ping (${SELF_URL})`);
  }
});
