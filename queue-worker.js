// صف انتظار برای Cloudflare Worker
// ۱) این فایل رو کنار worker فعلی‌ات بذار (یا کدش رو داخلش کپی کن).
// ۲) داخل fetch ی worker فعلی‌ات، اولِ همه این رو اضافه کن:
//      const q = await handleQueue(request, env);
//      if (q) return q;
// ۳) کلاس QueueDO رو export کن و توی wrangler.toml این رو اضافه کن:
//      [[durable_objects.bindings]]
//      name = "QUEUE"
//      class_name = "QueueDO"
//      [[migrations]]
//      tag = "v-queue"
//      new_sqlite_classes = ["QueueDO"]

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

export async function handleQueue(request, env) {
  const url = new URL(request.url);
  if (!url.pathname.startsWith("/queue/")) return null;
  if (request.method === "OPTIONS") return new Response(null, { headers: CORS });
  let model = url.searchParams.get("model") || "default";
  if (request.method === "POST") {
    const b = await request.clone().json().catch(() => ({}));
    model = b.model || model;
  }
  const stub = env.QUEUE.get(env.QUEUE.idFromName(String(model)));
  const res = await stub.fetch(request);
  const out = new Response(res.body, res);
  Object.entries(CORS).forEach(([k, v]) => out.headers.set(k, v));
  return out;
}

const MAX_RUNNING = 1;          // چند نفر همزمان اجازه‌ی ساخت دارن (برای هر مدل)
const IDLE_MS = 15000;          // اگه این‌قدر poll نکرد (تب بسته شد) از صف حذف می‌شه
const RUN_LIMIT_MS = 4 * 60000; // حداکثر زمان یک ساخت

export class QueueDO {
  constructor() {
    this.waiting = []; // {id, seen}
    this.running = []; // {id, start}
  }
  clean(now) {
    this.waiting = this.waiting.filter((t) => now - t.seen < IDLE_MS);
    this.running = this.running.filter((t) => now - t.start < RUN_LIMIT_MS);
    while (this.running.length < MAX_RUNNING && this.waiting.length) {
      const t = this.waiting.shift();
      this.running.push({ id: t.id, start: now });
    }
  }
  info(id) {
    const ready = this.running.some((t) => t.id === id);
    const idx = this.waiting.findIndex((t) => t.id === id);
    return {
      queue: true,
      id,
      ready,
      ahead: ready ? 0 : (idx < 0 ? 0 : idx) + this.running.length,
      total: this.waiting.length + this.running.length,
    };
  }
  async fetch(request) {
    const url = new URL(request.url);
    const now = Date.now();
    this.clean(now);
    const json = (o) => new Response(JSON.stringify(o), { headers: { "Content-Type": "application/json" } });

    if (url.pathname === "/queue/join") {
      const id = crypto.randomUUID();
      this.waiting.push({ id, seen: now });
      this.clean(now);
      return json(this.info(id));
    }
    if (url.pathname === "/queue/status") {
      const id = url.searchParams.get("id");
      const w = this.waiting.find((t) => t.id === id);
      if (w) w.seen = now;
      else if (!this.running.some((t) => t.id === id)) {
        // بلیت منقضی شده → دوباره آخر صف
        this.waiting.push({ id, seen: now });
        this.clean(now);
      }
      return json(this.info(id));
    }
    if (url.pathname === "/queue/done") {
      const b = await request.json().catch(() => ({}));
      this.waiting = this.waiting.filter((t) => t.id !== b.id);
      this.running = this.running.filter((t) => t.id !== b.id);
      this.clean(now);
      return json({ queue: true, ok: true });
    }
    return new Response("not found", { status: 404 });
  }
}
