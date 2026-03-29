import http from "node:http";
import { URL } from "node:url";
import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const HOST = process.env.JISILU_WEBHOOK_HOST || "127.0.0.1";
const PORT = Number(process.env.JISILU_WEBHOOK_PORT || 8787);
const TOKEN = process.env.JISILU_WEBHOOK_TOKEN || "";

const USERNAME = process.env.JISILU_USERNAME || "";
const PASSWORD = process.env.JISILU_PASSWORD || "";

const LOGIN_URL = process.env.JISILU_LOGIN_URL || "https://www.jisilu.cn/account/login/";
const ETF_PAGE_URL = process.env.JISILU_ETF_PAGE_URL || "https://www.jisilu.cn/data/etf/";
const ETF_LIST_URL = process.env.JISILU_ETF_LIST_URL || "https://www.jisilu.cn/data/etf/etf_list/";

const NAV_TIMEOUT_MS = Number(process.env.JISILU_NAV_TIMEOUT_MS || 30000);
const POST_LOGIN_WAIT_MS = Number(process.env.JISILU_POST_LOGIN_WAIT_MS || 2500);
const HEADLESS = String(process.env.JISILU_HEADLESS || "1") !== "0";
const ARTIFACT_DIR = process.env.JISILU_ARTIFACT_DIR || "runtime/tmp/jisilu_webhook";
const MANUAL_LOGIN_TIMEOUT_MS = Number(process.env.JISILU_MANUAL_LOGIN_TIMEOUT_MS || 180000);

function json(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        reject(new Error("request body too large"));
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function safeEqualBearer(actualHeader, expectedToken) {
  if (!expectedToken) return true;
  const prefix = "Bearer ";
  if (!actualHeader || !actualHeader.startsWith(prefix)) return false;
  const actualToken = actualHeader.slice(prefix.length);
  const a = Buffer.from(actualToken);
  const b = Buffer.from(expectedToken);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

async function fillFirst(page, selectors, value) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if (await locator.count()) {
      await locator.fill(value);
      return selector;
    }
  }
  throw new Error(`unable to find selector: ${selectors.join(", ")}`);
}

async function clickFirst(page, selectors) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if (await locator.count()) {
      await locator.click();
      return selector;
    }
  }
  throw new Error(`unable to find clickable selector: ${selectors.join(", ")}`);
}

async function checkIfPresent(page, selectors) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if (!(await locator.count())) continue;
    const checked = await locator.isChecked().catch(() => false);
    if (!checked) {
      await locator.check({ force: true }).catch(async () => {
        await locator.click({ force: true });
      });
    }
    return true;
  }
  return false;
}

async function clickIfPresent(page, selectors) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if (!(await locator.count())) continue;
    await locator.click({ force: true }).catch(() => {});
    return true;
  }
  return false;
}

async function ensureLoginCheckboxes(page) {
  const rememberChecked = await checkIfPresent(page, [
    '.remember_me input[type="checkbox"]',
    '.user_agree_box .remember_me input[type="checkbox"]',
    'input[type="checkbox"][name="auto_login"]',
  ]);

  const agreementChecked =
    await checkIfPresent(page, [
      '.user_agree input[type="checkbox"]',
      '.user_agree_box .user_agree input[type="checkbox"]',
      'input[type="checkbox"][name*="agree"]',
      'input[type="checkbox"][id*="agree"]',
      'input[type="checkbox"][name*="policy"]',
      'input[type="checkbox"][id*="policy"]',
      'input[type="checkbox"][name*="privacy"]',
      'input[type="checkbox"][id*="privacy"]',
    ]) ||
    await clickIfPresent(page, [
      '.user_agree',
      '.user_agree span',
      'label:has-text("同意")',
      'label:has-text("隐私")',
      'label:has-text("协议")',
      'span:has-text("同意")',
      'span:has-text("隐私")',
      'span:has-text("协议")',
    ]);

  return {
    rememberChecked,
    agreementChecked,
  };
}

function joinCookieHeader(cookies) {
  return cookies
    .filter((item) => item.name && item.value)
    .map((item) => `${item.name}=${item.value}`)
    .join("; ");
}

async function saveArtifacts(page, tag) {
  await fs.mkdir(ARTIFACT_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const prefix = path.join(ARTIFACT_DIR, `${stamp}_${tag}`);
  const screenshotPath = `${prefix}.png`;
  const htmlPath = `${prefix}.html`;
  await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
  const html = await page.content().catch(() => "");
  if (html) {
    await fs.writeFile(htmlPath, html, "utf8").catch(() => {});
  }
  return { screenshotPath, htmlPath };
}

async function isLoggedIn(page) {
  const loginButtons = page.locator('a[href="/account/login/"], a[href="/login/"]');
  if (await loginButtons.count()) {
    return false;
  }
  const marker = await page.evaluate(() => {
    const raw = typeof window !== "undefined" ? window.G_USER_ID : undefined;
    return typeof raw === "number" ? raw : null;
  }).catch(() => null);
  if (typeof marker === "number") {
    return marker > 0;
  }
  return true;
}

async function waitForManualLogin(page) {
  const deadline = Date.now() + MANUAL_LOGIN_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await page.goto(ETF_PAGE_URL, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS }).catch(() => {});
    await page.waitForTimeout(1500);
    if (await isLoggedIn(page)) {
      return true;
    }
  }
  return false;
}

async function refreshJisiluCookie() {
  if (!USERNAME || !PASSWORD) {
    throw new Error("missing JISILU_USERNAME or JISILU_PASSWORD");
  }

  const browser = await chromium.launch({ headless: HEADLESS });
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 960 },
      userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    });
    const page = await context.newPage();

    await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });

    await fillFirst(page, [
      'input[name="user_name"]',
      'input[name="username"]',
      'input[type="text"]',
      "#username",
    ], USERNAME);
    await fillFirst(page, [
      'input[name="password"]',
      'input[type="password"]',
      "#password",
    ], PASSWORD);
    await ensureLoginCheckboxes(page);

    await Promise.allSettled([
      page.waitForNavigation({ waitUntil: "networkidle", timeout: NAV_TIMEOUT_MS }),
      clickFirst(page, [
        'button[type="submit"]',
        'input[type="submit"]',
        ".btn-login",
        ".login-btn",
      ]),
    ]);

    await page.waitForTimeout(POST_LOGIN_WAIT_MS);
    await page.goto(ETF_PAGE_URL, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS });
    await page.waitForTimeout(1000);

    const apiResp = await page.request.get(ETF_LIST_URL, {
      params: {
        ___jsl: `LST___t=${Date.now()}`,
        rp: "500",
        page: "1",
        unit_total: "",
        volume: "",
      },
      headers: {
        Referer: ETF_PAGE_URL,
        "X-Requested-With": "XMLHttpRequest",
      },
      timeout: NAV_TIMEOUT_MS,
    });

    const apiText = await apiResp.text();
    if (!apiResp.ok()) {
      throw new Error(`jisilu etf api failed | status=${apiResp.status()} | body=${apiText.slice(0, 300)}`);
    }
    if (apiText.includes("游客仅显示前 20 条") || apiText.includes("请登录查看完整列表数据")) {
      if (!HEADLESS) {
        console.log("jisilu webhook: waiting for manual login in the opened browser...");
        const ok = await waitForManualLogin(page);
        if (ok) {
          const retryResp = await page.request.get(ETF_LIST_URL, {
            params: {
              ___jsl: `LST___t=${Date.now()}`,
              rp: "500",
              page: "1",
              unit_total: "",
              volume: "",
            },
            headers: {
              Referer: ETF_PAGE_URL,
              "X-Requested-With": "XMLHttpRequest",
            },
            timeout: NAV_TIMEOUT_MS,
          });
          const retryText = await retryResp.text();
          if (retryResp.ok() && !retryText.includes("游客仅显示前 20 条") && !retryText.includes("请登录查看完整列表数据")) {
            const cookies = await context.cookies("https://www.jisilu.cn");
            const cookieHeader = joinCookieHeader(cookies);
            if (!cookieHeader) {
              throw new Error("empty cookie after manual login");
            }
            return {
              cookie: cookieHeader,
              cookie_names: cookies.map((item) => item.name),
              login_url: LOGIN_URL,
              etf_page_url: ETF_PAGE_URL,
            };
          }
        }
      }

      const artifacts = await saveArtifacts(page, "tourist_mode");
      const currentUrl = page.url();
      const pageTitle = await page.title().catch(() => "");
      const headlessHint = HEADLESS
        ? " | hint=set JISILU_HEADLESS=0 and retry"
        : "";
      throw new Error(
        `jisilu login succeeded but api still in tourist mode | url=${currentUrl} | title=${pageTitle} | screenshot=${artifacts.screenshotPath} | html=${artifacts.htmlPath}${headlessHint}`
      );
    }

    const cookies = await context.cookies("https://www.jisilu.cn");
    const cookieHeader = joinCookieHeader(cookies);
    if (!cookieHeader) {
      throw new Error("empty cookie after login");
    }

    return {
      cookie: cookieHeader,
      cookie_names: cookies.map((item) => item.name),
      login_url: LOGIN_URL,
      etf_page_url: ETF_PAGE_URL,
    };
  } finally {
    await browser.close();
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "GET" && url.pathname === "/healthz") {
      return json(res, 200, { ok: true, service: "jisilu_cookie_webhook" });
    }

    if (req.method !== "POST" || url.pathname !== "/refresh/jisilu-cookie") {
      return json(res, 404, { ok: false, error: "not_found" });
    }

    if (!safeEqualBearer(req.headers.authorization, TOKEN)) {
      return json(res, 401, { ok: false, error: "unauthorized" });
    }

    const rawBody = await readRequestBody(req);
    const payload = rawBody ? JSON.parse(rawBody) : {};
    if ((payload.action || "refresh_cookie") !== "refresh_cookie" || (payload.site || "jisilu") !== "jisilu") {
      return json(res, 400, { ok: false, error: "invalid_payload" });
    }

    const refreshed = await refreshJisiluCookie();
    return json(res, 200, {
      ok: true,
      cookie: refreshed.cookie,
      cookie_names: refreshed.cookie_names,
      site: "jisilu",
    });
  } catch (error) {
    return json(res, 500, {
      ok: false,
      error: String(error && error.message ? error.message : error),
    });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`jisilu cookie webhook listening on http://${HOST}:${PORT}`);
});
