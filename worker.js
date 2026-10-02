// ورکر واحد: پروکسی z.ai (مدل‌های GLM) + مدل رایگان Qwen روی Cloudflare Workers AI
// - اگه model با "@cf/" شروع بشه  → Cloudflare Workers AI (بدون کلید)
// - در غیر این صورت               → همون پروکسی قبلی z.ai (با کلید خود کاربر)

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

// مدل‌های مجاز Cloudflare (قبل از اضافه کردن مدل جدید: npx wrangler ai models list)
const CF_MODELS = ["@cf/qwen/qwen2.5-coder-32b-instruct"];
const MAX_OUT = 8000; // سقف توکن خروجی (کانتکست این مدل حدود ۳۲ هزار توکنه)

const json = (o, status = 200) =>
  new Response(JSON.stringify(o), {
    status,
    headers: { ...CORS, "Content-Type": "application/json" },
  });

// content ممکنه آرایه باشه (متن + تصویر)؛ مدل متنی فقط رشته می‌گیره
function flatten(m) {
  let c = m.content;
  if (Array.isArray(c)) c = c.filter((p) => p && p.type === "text").map((p) => p.text).join("\n");
  return { role: m.role, content: String(c || "") };
}

// تبدیل جریان Cloudflare به فرمت OpenAI
function toOpenAIStream(src) {
  const dec = new TextDecoder();
  const enc = new TextEncoder();
  let buf = "";
  function handle(line, controller) {
    line = line.trim();
    if (line.indexOf("data:") !== 0) return;
    const d = line.slice(5).trim();
    if (!d || d === "[DONE]") return;
    let j;
    try { j = JSON.parse(d); } catch (e) { return; }
    const t = j.response ?? (j.choices && j.choices[0] && j.choices[0].delta && j.choices[0].delta.content) ?? "";
    if (t) controller.enqueue(enc.encode("data: " + JSON.stringify({ choices: [{ delta: { content: t } }] }) + "\n\n"));
  }
  return src.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        buf += dec.decode(chunk, { stream: true });
        const lines = buf.split("\n");
        buf = lines.pop();
        for (const l of lines) handle(l, controller);
      },
      flush(controller) {
        if (buf) handle(buf, controller);
        controller.enqueue(enc.encode("data: [DONE]\n\n"));
      },
    })
  );
}

async function runCF(body, env) {
  if (!env.AI) return json({ error: { code: "noai", message: "اتصال Workers AI (binding با نام AI) روی ورکر تنظیم نشده." } }, 500);
  const model = body.model;
  if (CF_MODELS.indexOf(model) === -1) return json({ error: { code: "model", message: "این مدل پشتیبانی نمی‌شود: " + model } }, 400);
  if (!Array.isArray(body.messages) || !body.messages.length) return json({ error: { code: "bad", message: "messages خالیه" } }, 400);

  const messages = body.messages.map(flatten);
  const max_tokens = Math.min(Number(body.max_tokens) || 4096, MAX_OUT);
  const stream = body.stream !== false;

  try {
    const out = await env.AI.run(model, { messages, max_tokens, stream });
    if (stream) {
      return new Response(toOpenAIStream(out), {
        headers: { ...CORS, "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
      });
    }
    const text = out.response ?? (out.choices && out.choices[0] && out.choices[0].message && out.choices[0].message.content) ?? "";
    return json({ choices: [{ message: { role: "assistant", content: text }, finish_reason: "stop" }] });
  } catch (e) {
    const raw = String((e && e.message) || e);
    const err = (status, code, fa) =>
      json({ error: { code, message: fa + "\n(جزئیات فنی: " + raw.slice(0, 150) + ")" } }, status);
    if (/neurons|4006|daily free allocation/i.test(raw))
      return err(429, "quota", "سهمیه‌ی رایگان امروز مدل Qwen تموم شده. فردا دوباره امتحان کن یا یکی از مدل‌های GLM رو انتخاب کن.");
    if (/context|too long|too many tokens|exceed|5021|maximum/i.test(raw))
      return err(413, "context", "متن گفتگو برای این مدل بیش از حد طولانیه (مدل نمی‌تونه این‌قدر متن رو یه‌جا بخونه).");
    if (/rate limit|too many requests|429/i.test(raw))
      return err(429, "rate", "درخواست‌ها پشت سر هم زیاد بود. چند ثانیه صبر کن و دوباره امتحان کن.");
    if (/capacity|overloaded|busy|3040|temporar|unavailable/i.test(raw))
      return err(503, "busy", "سرور مدل Qwen الان شلوغه و ظرفیت نداره. یکی دو دقیقه صبر کن و دوباره امتحان کن.");
    if (/timeout|timed out/i.test(raw))
      return err(504, "timeout", "مدل Qwen زیادی طول کشید و قطع شد. توضیح بازی رو ساده‌تر کن و دوباره امتحان کن.");
    return err(500, "unknown", "مشکلی تو سرور مدل Qwen پیش اومد. دوباره امتحان کن.");
  }
}


// ===================== صف انتظار (Durable Object) =====================
// هر «صف» مخصوص یک مدل است؛ برای مدل‌های z.ai به ازای هر کلید API جدا (چون سقف همزمانی z.ai برای هر کلید جداست).
const MAX_RUNNING_CF = 2;   // چند نفر همزمان با Qwen بسازن
const MAX_RUNNING_ZAI = 1;  // چند نفر همزمان با یک کلید z.ai بسازن
const WAIT_IDLE_MS = 15000; // کسی که توی صفه و ۱۵ ثانیه خبر نده (تب بسته شد) حذف می‌شه
const RUN_IDLE_MS = 60000;  // کسی که داره می‌سازه و ۱ دقیقه خبر نده حذف می‌شه
const RUN_HARD_MS = 6 * 60000; // حداکثر زمان یک ساخت

async function sha16(text) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(d)).slice(0, 8).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function handleQueue(request, env, url) {
  if (!env.QUEUE) return json({ error: { message: "queue not enabled" } }, 404);
  let model = url.searchParams.get("model") || "";
  if (request.method === "POST") {
    const b = await request.clone().json().catch(() => ({}));
    model = b.model || model;
  }
  model = String(model || "default");
  const isCf = model.indexOf("@cf/") === 0;
  const name = isCf ? "cf:" + model : model + ":" + (await sha16(request.headers.get("Authorization") || ""));
  const fwd = new Request(request);
  fwd.headers.set("x-max", String(isCf ? MAX_RUNNING_CF : MAX_RUNNING_ZAI));
  const res = await env.QUEUE.get(env.QUEUE.idFromName(name)).fetch(fwd);
  return new Response(res.body, { status: res.status, headers: { ...CORS, "Content-Type": "application/json" } });
}

export class QueueDO {
  constructor() {
    this.waiting = []; // {id, seen}
    this.running = []; // {id, start, seen}
  }
  clean(now, max) {
    this.waiting = this.waiting.filter((t) => now - t.seen < WAIT_IDLE_MS);
    this.running = this.running.filter((t) => now - t.seen < RUN_IDLE_MS && now - t.start < RUN_HARD_MS);
    while (this.running.length < max && this.waiting.length) {
      const t = this.waiting.shift();
      this.running.push({ id: t.id, start: now, seen: now });
    }
  }
  info(id) {
    const ready = this.running.some((t) => t.id === id);
    const idx = this.waiting.findIndex((t) => t.id === id);
    return {
      queue: true,
      id,
      ready,
      ahead: ready ? 0 : this.running.length + Math.max(idx, 0),
      total: this.waiting.length + this.running.length,
    };
  }
  async fetch(request) {
    const url = new URL(request.url);
    const now = Date.now();
    const max = Number(request.headers.get("x-max")) || 1;
    this.clean(now, max);
    const out = (o) => new Response(JSON.stringify(o), { headers: { "Content-Type": "application/json" } });

    if (url.pathname === "/queue/join") {
      const id = crypto.randomUUID();
      this.waiting.push({ id, seen: now });
      this.clean(now, max);
      return out(this.info(id));
    }
    if (url.pathname === "/queue/status") {
      const id = url.searchParams.get("id") || "";
      const w = this.waiting.find((t) => t.id === id);
      const r = this.running.find((t) => t.id === id);
      if (w) w.seen = now;
      else if (r) r.seen = now;
      else { // بلیت منقضی یا DO ریست شده → دوباره آخر صف
        this.waiting.push({ id, seen: now });
        this.clean(now, max);
      }
      return out(this.info(id));
    }
    if (url.pathname === "/queue/done") {
      const b = await request.json().catch(() => ({}));
      this.waiting = this.waiting.filter((t) => t.id !== b.id);
      this.running = this.running.filter((t) => t.id !== b.id);
      this.clean(now, max);
      return out({ queue: true, ok: true });
    }
    return new Response("not found", { status: 404 });
  }
}
// ======================================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });
    if (url.pathname.indexOf("/queue/") === 0) return handleQueue(request, env, url);
    if (request.method !== "POST") return json({ ok: true, proxy: "z.ai", cf_models: CF_MODELS, queue: !!env.QUEUE });

    const text = await request.text();
    let body = null;
    try { body = JSON.parse(text); } catch (e) {}

    if (body && typeof body.model === "string" && body.model.indexOf("@cf/") === 0) {
      return runCF(body, env);
    }

    // پروکسی z.ai (مثل قبل)
    const r = await fetch("https://api.z.ai/api/paas/v4/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": request.headers.get("Authorization") || "",
      },
      body: text,
    });
    return new Response(r.body, {
      status: r.status,
      headers: { ...CORS, "Content-Type": r.headers.get("Content-Type") || "application/json" },
    });
  },
};
