/**
 * Cloudflare Worker: multi-domain proxy for NotifBot short links (https://<domain>/<slug>).
 *
 * Deploy once in your Cloudflare account (Workers & Pages -> Create Worker),
 * set environment variable ORIGIN_BASE_URL to your Railway bot URL
 * (e.g. "https://your-bot.up.railway.app"), and attach routes:
 *   <short-domain.com>/*
 *   www.<short-domain.com>/*
 *
 * Or let the bot attach DNS + Worker routes automatically via CLOUDFLARE_API_TOKEN
 * and CLOUDFLARE_WORKER_NAME when adding a domain in /domains.
 */

export default {
  async fetch(request, env) {
    const originBase = (env.ORIGIN_BASE_URL || "").trim().replace(/\/+$/, "");
    if (!originBase) {
      return new Response("ORIGIN_BASE_URL is not configured", { status: 500 });
    }

    const incomingUrl = new URL(request.url);
    if (incomingUrl.pathname === "/" || incomingUrl.pathname === "") {
      return new Response("OK", {
        status: 200,
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    const upstreamUrl = new URL(incomingUrl.pathname + incomingUrl.search, originBase);
    const headers = new Headers();
    const originalHost = incomingUrl.hostname.toLowerCase();
    headers.set("X-Short-Host", originalHost);
    headers.set("X-Forwarded-Host", originalHost);

    const ua = request.headers.get("user-agent");
    if (ua) headers.set("User-Agent", ua);
    const acceptLang = request.headers.get("accept-language");
    if (acceptLang) headers.set("Accept-Language", acceptLang);

    const upstreamResp = await fetch(upstreamUrl.toString(), {
      method: request.method,
      headers,
      redirect: "manual",
    });

    return upstreamResp;
  },
};
