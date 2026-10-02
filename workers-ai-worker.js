// ورکر white-voice-72bd — مدل‌های رایگان Cloudflare Workers AI
// بدون نیاز به کلید API. نیاز دارد: binding با نام AI (همونی که الان داری).
// ورودی و خروجی با فرمت OpenAI (مثل ورکر z.ai) تا برنامه بدون تغییر زیاد کار کنه.

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};

// مدل‌های مجاز (برای اینکه کسی مدل سنگین صدا نزنه و سهمیه‌ی رایگان تموم نشه).
// قبل از اضافه کردن مدل جدید: npx wrangler ai models list
const ALLOWED = [
  "@cf/qwen/qwen2.5-coder-32b-instruct",
];
const MAX_OUT = 8000; // سقف توکن خروجی (کانتکست این مدل‌ها حدود ۳۲ هزار توکنه)

const json = (o, status = 200) =>
  new Response(JSON.stringify(o), {
    status,
    headers: { ...CORS, "Content-Type": "application/json" },
  });

// content ممکنه آرایه باشه (متن + تصویر)؛ مدل‌های متنی فقط رشته می‌گیرن
function flatten(m) {
  let c = m.content;
  if (Array.isArray(c)) c = c.filter((p) => p && p.type === "text").map((p) => p.text).join("\n");
  return { role: m.role, content: String(c || "") };
}

// تبدیل جریان Cloudflare به فرمت OpenAI (data: {"choices":[{"delta":{"content":"..."}}]})
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

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });
    if (request.method === "GET") return json({ ok: true, models: ALLOWED });
    if (request.method !== "POST") return json({ error: { message: "متد نامعتبر" } }, 405);

    let body;
    try { body = await request.json(); } catch (e) { return json({ error: { message: "JSON نامعتبر" } }, 400); }

    const model = body.model || ALLOWED[0];
    if (ALLOWED.indexOf(model) === -1) return json({ error: { message: "این مدل پشتیبانی نمی‌شود: " + model } }, 400);
    if (!Array.isArray(body.messages) || !body.messages.length) return json({ error: { message: "messages خالیه" } }, 400);

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
  },
};
