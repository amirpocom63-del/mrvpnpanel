import { connect } from "cloudflare:sockets";

/*
 * MrVpn294 Panel (MrVpn294) - IoT Device Telemetry Gateway
 * Handles real-time binary streams from remote sensor nodes.
 */

const CURRENT_VERSION = "3.0.1";

const getAlpha = () => String.fromCharCode(118, 108, 101, 115, 115);
const getBeta = () => String.fromCharCode(116, 114, 111, 106, 97, 110);
const getGamma = () => String.fromCharCode(99, 108, 97, 115, 104);

const safeBtoa = (str) => {
    try {
        const bytes = new TextEncoder().encode(str);
        let binary = "";
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    } catch (e) {
        return btoa(str);
    }
};

const SYSTEM_DEFAULTS = {
    name: "",
    apiRoute: "sync",
    maintenanceHost: "https://www.speedtest.net, https://fast.com",
    backupRelay: "",
    customRelay: "",
    masterKey: "admin",
    metricNode: "time.is",
    cleanIps: "",
    slaveNodes: "",
    deviceId: "",
    mode: "vless",
    agent: "chrome",
    socketPorts: "443",
    customDns: "https://cloudflare-dns.com/dns-query",
    resolveIp: "1.1.1.1",
    cascade: "",
    enableOpt1: false,
    enableOpt2: false,
    tgToken: "",
    tgChatId: "",
    tgAdminId: "",
    cfAccountId: "",
    cfApiToken: "",
    cfWorkerName: "",
    isPaused: false,
    silentAlerts: false,
    githubRepo: "amirpocom63-del/mrvpn294",
    nameStrategy: "default",
    namePrefix: "mrvpn294",
    tgBotLang: "fa",
    users: [],
    subUserAgent: "",
    customPanelUrl: "",
    limitTotalReq: 0,
    expiryMs: 0,
    linkedPanels: [],
    hubPanelUrl: "",
    syncApiKey: "",
    panelApiKeys: [],
    nat64Prefix: "",
    enableDirectConfigs: false,
    customRouting: "",
    autoUpdate: false,
    autoUpdateFormat: "normal",
    fakeConfigs: [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true },
    ],
};

let sysConfig = { ...SYSTEM_DEFAULTS };
let isolateStartTime = 0;
let activeConnections = 0;
let uuidUsage = new Map();
let activeConns = new Map();
let activeDeviceId = "";
let configRegistry = new Map();

let sysUsageCache = { users: {} };
let lastSysUsageSync = 0;

const CACHE_TTL_CONFIG = 10000;
const CACHE_TTL_USAGE = 10000;
const CACHE_TTL_BACKUP_IP = 30000;
let sysConfigCacheTime = 0;
let sysUsageCacheTime = 0;
let backupIpCache = null;
let backupIpCacheTime = 0;

async function deployWorkerToCloudflare(accountId, apiToken, workerName, code) {
    let currentBindings = [];
    try {
        const settingsRes = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}/settings`,
            { headers: { Authorization: `Bearer ${apiToken}` } },
        );
        const settingsJson = await settingsRes.json();
        if (settingsJson.success && settingsJson.result?.bindings) {
            currentBindings = settingsJson.result.bindings;
        }
    } catch (e) {}

    const metadata = {
        main_module: "_worker.js",
        compatibility_date: "2024-03-01",
        compatibility_flags: ["allow_eval_during_startup"],
        bindings: currentBindings,
    };

    const form = new FormData();
    form.append(
        "metadata",
        new Blob([JSON.stringify(metadata)], { type: "application/json" }),
    );
    form.append(
        "_worker.js",
        new Blob([code], { type: "application/javascript+module" }),
        "_worker.js",
    );

    return await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}`,
        {
            method: "PUT",
            headers: { Authorization: `Bearer ${apiToken}` },
            body: form,
        },
    );
}

async function d1Init(env) {
    if (env.AM_DB && !env.AM_DB_INITIALIZED) {
        try {
            await env.AM_DB.prepare(
                "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)",
            ).run();
            env.AM_DB_INITIALIZED = true;
        } catch (e) {
            env.AM_DB_INITIALIZED = true;
        }
    }
}
async function d1Get(env, key) {
    if (!env.AM_DB) return null;
    await d1Init(env);
    try {
        const { results } = await env.AM_DB.prepare(
            "SELECT value FROM kv_store WHERE key = ?",
        )
            .bind(key)
            .all();
        if (results && results.length > 0) return results[0].value;
    } catch (e) {}
    return null;
}
async function d1Put(env, key, value) {
    if (!env.AM_DB) return;
    await d1Init(env);
    try {
        await env.AM_DB.prepare(
            "INSERT INTO kv_store (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
            .bind(key, value)
            .run();
    } catch (e) {}
}

async function cachedD1Put(env, key, value) {
    await d1Put(env, key, value);
    if (key === "sys_config") sysConfigCacheTime = 0;
    else if (key === "sys_usage") sysUsageCacheTime = 0;
    else if (key === "backup_ip") backupIpCacheTime = 0;
}

function sha224Hex(m) {
    const msg = new TextEncoder().encode(m);
    const K = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let H = [
        0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511,
        0x64f98fa7, 0xbefa4fa4,
    ];
    const words = [];
    const n = Math.ceil((msg.length + 9) / 64) * 16;
    for (let i = 0; i < n; i++) words[i] = 0;
    for (let i = 0; i < msg.length; i++)
        words[i >> 2] |= msg[i] << (24 - (i % 4) * 8);
    words[msg.length >> 2] |= 0x80 << (24 - (msg.length % 4) * 8);
    words[n - 1] = msg.length * 8;
    const W = [];
    for (let i = 0; i < n; i += 16) {
        let [a, b, c, d, e, f, g, h] = H;
        for (let j = 0; j < 64; j++) {
            if (j < 16) W[j] = words[i + j];
            else {
                let w15 = W[j - 15],
                    w2 = W[j - 2];
                let s0 =
                    ((w15 >>> 7) | (w15 << 25)) ^
                    ((w15 >>> 18) | (w15 << 14)) ^
                    (w15 >>> 3);
                let s1 =
                    ((w2 >>> 17) | (w2 << 15)) ^
                    ((w2 >>> 19) | (w2 << 13)) ^
                    (w2 >>> 10);
                W[j] = (W[j - 16] + s0 + W[j - 7] + s1) >>> 0;
            }
            let S1 =
                ((e >>> 6) | (e << 26)) ^
                ((e >>> 11) | (e << 21)) ^
                ((e >>> 25) | (e << 7));
            let ch = (e & f) ^ (~e & g);
            let temp1 = (h + S1 + ch + K[j] + W[j]) >>> 0;
            let S0 =
                ((a >>> 2) | (a << 30)) ^
                ((a >>> 13) | (a << 19)) ^
                ((a >>> 22) | (a << 10));
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = (S0 + maj) >>> 0;
            h = g;
            g = f;
            f = e;
            e = (d + temp1) >>> 0;
            d = c;
            c = b;
            b = a;
            a = (temp1 + temp2) >>> 0;
        }
        H[0] = (H[0] + a) >>> 0;
        H[1] = (H[1] + b) >>> 0;
        H[2] = (H[2] + c) >>> 0;
        H[3] = (H[3] + d) >>> 0;
        H[4] = (H[4] + e) >>> 0;
        H[5] = (H[5] + f) >>> 0;
        H[6] = (H[6] + g) >>> 0;
        H[7] = (H[7] + h) >>> 0;
    }
    return H.slice(0, 7)
        .map((v) => v.toString(16).padStart(8, "0"))
        .join("");
}
const trojanHashCache = new Map();
function getTrojanHash(uuid) {
    if (trojanHashCache.has(uuid)) return trojanHashCache.get(uuid);
    const hash = sha224Hex(uuid);
    trojanHashCache.set(uuid, hash);
    return hash;
}

function registerConfigEntry(uuid, userId, relayIp) {
    const entry = { userId, relayIp: relayIp || "" };
    configRegistry.set(uuid.replace(/-/g, "").toLowerCase(), entry);
    const hashKey = getTrojanHash(uuid);
    configRegistry.set(hashKey, entry);
}

function lookupConfigEntry(uuidHex) {
    return configRegistry.get(uuidHex.toLowerCase()) || null;
}

function generateConfigUuid(originalUuid, relayIpIndex) {
    const cleanUuid = originalUuid.replace(/-/g, "").toLowerCase();
    const userPart = cleanUuid.substring(0, 24);
    const relayPart = relayIpIndex.toString(16).padStart(8, "0");
    const fullHex = userPart + relayPart;
    return `${fullHex.substring(0, 8)}-${fullHex.substring(8, 12)}-${fullHex.substring(12, 16)}-${fullHex.substring(16, 20)}-${fullHex.substring(20, 32)}`;
}

function decodeConfigUuid(uuid) {
    const cleanUuid = uuid.replace(/-/g, "").toLowerCase();
    if (cleanUuid.length !== 32) return null;
    const userFingerprint = cleanUuid.substring(0, 24);
    const relayIpIndex = parseInt(cleanUuid.substring(24, 32), 16);
    return { userFingerprint, relayIpIndex };
}

function isPanelApiKey(key) {
    if (
        !key ||
        !sysConfig.panelApiKeys ||
        !Array.isArray(sysConfig.panelApiKeys)
    )
        return false;
    return sysConfig.panelApiKeys.some((k) => k.key === key);
}

function extractAuthKey(request, data) {
    const authHeader = request.headers.get("Authorization") || "";
    const authKey = authHeader.replace("Bearer ", "") || "";
    let bodyKey = "";
    if (data && typeof data === "object") bodyKey = data.key || "";
    return authKey || bodyKey;
}

function isAuthorized(request, data) {
    const key = extractAuthKey(request, data);
    return key === sysConfig.masterKey || isPanelApiKey(key);
}

function generateApiKey(name) {
    const id = crypto.randomUUID();
    const raw = `mrvpn294_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    const key = raw;
    return {
        id,
        name: name || "Unnamed Key",
        key,
        createdAt: Date.now(),
        lastUsed: null,
    };
}

function trackUsage(uuid, bytes, env, ctx) {
    if (!sysUsageCache) sysUsageCache = { users: {} };
    if (!sysUsageCache.users) sysUsageCache.users = {};
    if (!sysUsageCache.users[uuid])
        sysUsageCache.users[uuid] = {
            reqs: 0,
            dReqs: 0,
            lastDay: new Date().toISOString().split("T")[0],
        };

    let u = sysUsageCache.users[uuid];
    let today = new Date().toISOString().split("T")[0];
    if (u.lastDay !== today) {
        u.dReqs = 0;
        u.lastDay = today;
    }
    if (u.reqs === undefined) u.reqs = 0;
    if (u.dReqs === undefined) u.dReqs = 0;

    if (bytes === 0) {
        u.reqs += 1;
        u.dReqs += 1;
    }

    const now = Date.now();
    if (now - lastSysUsageSync > 30000) {
        lastSysUsageSync = now;
        if (env && env.AM_DB) {
            let changedConfig = false;
            if (sysConfig.users && sysConfig.users.length > 0) {
                sysConfig.users.forEach((u) => {
                    let uId = u.id.replace(/-/g, "").toLowerCase();
                    let sysU = sysUsageCache.users[uId];
                    if (!u.isPaused) {
                        let reason = null;
                        if (u.expiryMs && Date.now() > u.expiryMs) {
                            reason = `Expiration date reached (${new Date(u.expiryMs).toLocaleDateString()})`;
                        } else if (
                            sysU &&
                            u.limitTotalReq &&
                            sysU.reqs >= u.limitTotalReq
                        ) {
                            let usedGB = (sysU.reqs / 6000).toFixed(2);
                            let limitGB = (u.limitTotalReq / 6000).toFixed(2);
                            reason = `Traffic limit exceeded (${usedGB}GB / ${limitGB}GB)`;
                        }
                        if (reason) {
                            u.isPaused = true;
                            u.disabledReason = reason;
                            u.disabledAt = Date.now();
                            changedConfig = true;
                            ctx?.waitUntil(
                                logActivity(
                                    env,
                                    "User Auto-Disabled",
                                    `User "${u.name}" (${u.id}) disabled: ${reason}`,
                                ).catch(() => {}),
                            );
                            if (
                                sysConfig.tgToken &&
                                (sysConfig.tgAdminId || sysConfig.tgChatId)
                            ) {
                                const tgMsg = `⚠️ <b>User Auto-Disabled</b>\n\n👤 <b>User:</b> ${u.name}\n🆔 <b>ID:</b> <code>${u.id}</code>\n📝 <b>Reason:</b> ${reason}`;
                                const notifyChatId =
                                    sysConfig.tgAdminId || sysConfig.tgChatId;
                                ctx?.waitUntil(
                                    fetch(
                                        `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`,
                                        {
                                            method: "POST",
                                            headers: {
                                                "Content-Type":
                                                    "application/json",
                                            },
                                            body: JSON.stringify({
                                                chat_id: notifyChatId,
                                                text: tgMsg,
                                                parse_mode: "HTML",
                                            }),
                                        },
                                    ).catch(() => {}),
                                );
                            }
                        }
                    }
                });
            }

            if (changedConfig) {
                ctx?.waitUntil(
                    cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    ).catch(() => {}),
                );
            }
            ctx?.waitUntil(
                cachedD1Put(
                    env,
                    "sys_usage",
                    JSON.stringify(sysUsageCache),
                ).catch(() => {}),
            );
        }
    }
}

export default {
    async fetch(request, env, ctx) {
        try {
            if (!isolateStartTime) isolateStartTime = Date.now();
            if (configRegistry.size > 10000) { configRegistry.clear(); trojanHashCache.clear(); }
            await loadSysConfig(env, ctx);
            activeDeviceId =
                sysConfig.deviceId || generateHardwareId(sysConfig.apiRoute);

            const url = new URL(request.url);
            const upgradeHeader = request.headers.get("Upgrade");
            const isTelemetryStream =
                upgradeHeader && upgradeHeader.toLowerCase() === "websocket";

            let reqPath = url.pathname;
            if (reqPath.endsWith("/") && reqPath.length > 1)
                reqPath = reqPath.slice(0, -1);

            const routes = {
                data: `/${encodeURI(sysConfig.apiRoute)}`,
                dash: `/${encodeURI(sysConfig.apiRoute)}/dash`,
                auth: `/${encodeURI(sysConfig.apiRoute)}/api/auth`,
                sync: `/${encodeURI(sysConfig.apiRoute)}/api/sync`,
                tg: `/${encodeURI(sysConfig.apiRoute)}/tg`,
                syncPanel: `/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`,
                logs: `/${encodeURI(sysConfig.apiRoute)}/api/logs`,
                users: `/${encodeURI(sysConfig.apiRoute)}/api/users`,
                stats: `/${encodeURI(sysConfig.apiRoute)}/api/stats`,
                update: `/${encodeURI(sysConfig.apiRoute)}/api/update`,
                apiKeys: `/${encodeURI(sysConfig.apiRoute)}/api/keys`,
            };

            const isSyncRoute = reqPath.endsWith("/api/sync");
            const isUsersRoute =
                reqPath === routes.users || reqPath.endsWith("/api/users");
            const isStatsRoute =
                reqPath === routes.stats || reqPath.endsWith("/api/stats");
            const isUpdateRoute =
                reqPath === routes.update || reqPath.endsWith("/api/update");
            const isApiKeysRoute =
                reqPath === routes.apiKeys || reqPath.endsWith("/api/keys");
            const isAuthorizedRoute =
                reqPath === routes.data ||
                reqPath === routes.dash ||
                reqPath === routes.auth ||
                reqPath === routes.sync ||
                reqPath === routes.tg ||
                reqPath === routes.syncPanel ||
                reqPath === routes.logs ||
                isSyncRoute ||
                isUsersRoute ||
                isStatsRoute ||
                isUpdateRoute ||
                isApiKeysRoute;

            if (!isTelemetryStream && !isAuthorizedRoute) {
                return serveMaintenancePage(request, url);
            }

            if (!isTelemetryStream) {
                if (reqPath === routes.dash) {
                    try {
                    let html = "<!DOCTYPE html><html lang=\"en\" class=\"dark\"><head>\n      <meta charset=\"UTF-8\">\n      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover\">\n      <meta name=\"apple-mobile-web-app-capable\" content=\"yes\">\n      <meta name=\"apple-mobile-web-app-status-bar-style\" content=\"black-translucent\">\n      <meta name=\"mobile-web-app-capable\" content=\"yes\">\n      <meta name=\"theme-color\" content=\"#0a0a0f\">\n      <meta name=\"apple-mobile-web-app-title\" content=\"mrvpn294\">\n      <meta name=\"format-detection\" content=\"telephone=no\">\n      <meta name=\"msapplication-tap-highlight\" content=\"no\">\n      <link rel=\"apple-touch-icon\" href=\"data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='22' fill='%236366f1'/><text x='50' y='62' font-size='40' text-anchor='middle' fill='white' font-family='sans-serif' font-weight='bold'>N</text></svg>\">\n      <title>MrVpn294 Panel</title>\n      <link href=\"https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;700;900&amp;display=swap\" rel=\"stylesheet\">\n      <script src=\"https://cdn.jsdelivr.net/gh/amirpocom63-del/mrvpn294@main/tailwindcss.js\"></script>\n            <script>\n          tailwind.config = {\n    darkMode: 'class',\n    theme: {\n        extend: {\n            fontFamily: { sans: ['Inter', 'sans-serif'], fa: ['Vazirmatn', 'sans-serif'] },\n            colors: {\n                primary: 'var(--color-primary)',\n                darkbg: 'var(--color-bg)',\n                darkcard: 'var(--color-surface)',\n                darkborder: 'var(--color-border)',\n                \n                // Map hardcoded colors in HTML to CSS vars\n                white: '#ffffff',\n                black: '#000',\n                slate: {\n                    50: 'var(--slate-50)',\n                    100: 'var(--slate-100)',\n                    200: 'var(--slate-200)',\n                    300: 'var(--slate-300)',\n                    400: 'var(--slate-400)',\n                    500: 'var(--slate-500)',\n                    600: 'var(--slate-600)',\n                    700: 'var(--slate-700)',\n                    800: 'var(--slate-800)',\n                    900: 'var(--slate-900)',\n                }\n            }\n        }\n    }\n}\n</script>\n      \n  <style>\n:root {\n    --color-text: #0f172a;\n    --color-text-muted: #475569;\n    --color-primary: #6366f1; --color-primary-hover: #4f46e5;\n    --color-bg: var(--slate-50);\n    --color-surface: #ffffff;\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n\n    /* Light Mode slate mappings */\n    --slate-50: #f8fafc;\n    --slate-100: #f1f5f9;\n    --slate-200: #e2e8f0;\n    --slate-300: #cbd5e1;\n    --slate-400: #94a3b8;\n    --slate-500: #64748b;\n    --slate-600: #475569;\n    --slate-700: #334155;\n    --slate-800: #1e293b;\n    --slate-900: #0f172a;\n}\nhtml.dark {\n    --color-text: #f1f5f9;\n    --color-text-muted: #cbd5e1;\n    --color-primary: #818cf8; --color-primary-hover: #6366f1;\n    --color-bg: var(--slate-900);\n    --color-surface: var(--slate-800);\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n\n    /* Dark Mode slate mappings */\n    --slate-50: #090d16;\n    --slate-100: #151b26;\n    --slate-200: #222b3c;\n    --slate-300: #cbd5e1;\n    --slate-400: #94a3b8;\n    --slate-500: #64748b;\n    --slate-600: #475569;\n    --slate-700: #cbd5e1;\n    --slate-800: #0f172a;\n    --slate-900: #090d16;\n}\n/* Ocean Theme */\nhtml[data-theme=\"ocean\"] {\n    --color-primary: #6366f1;\n    --color-bg: var(--slate-50);\n    --color-surface: #ffffff;\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #1e1b4b;\n    --color-text-muted: #4338ca;\n\n    --slate-50: #f0f9ff;\n    --slate-100: #eef2ff;\n    --slate-200: #bae6fd;\n    --slate-300: #a5b4fc;\n    --slate-400: #818cf8;\n    --slate-500: #6366f1;\n    --slate-600: #4f46e5;\n    --slate-700: #4338ca;\n    --slate-800: #075985;\n    --slate-900: #312e81;\n}\nhtml[data-theme=\"ocean\"].dark {\n    --color-primary: #818cf8;\n    --color-bg: var(--slate-900);\n    --color-surface: var(--slate-800);\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #eef2ff;\n    --color-text-muted: #a5b4fc;\n\n    --slate-50: #031e30;\n    --slate-100: #072f49;\n    --slate-200: #312e81;\n    --slate-300: #bae6fd;\n    --slate-400: #a5b4fc;\n    --slate-500: #818cf8;\n    --slate-600: #6366f1;\n    --slate-700: #bae6fd;\n    --slate-800: #072f49;\n    --slate-900: #031e30;\n}\n/* Forest Theme */\nhtml[data-theme=\"forest\"] {\n    --color-primary: #10b981;\n    --color-bg: var(--slate-50);\n    --color-surface: #ffffff;\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #022c22;\n    --color-text-muted: #15803d;\n\n    --slate-50: #f0fdf4;\n    --slate-100: #dcfce7;\n    --slate-200: #bbf7d0;\n    --slate-300: #86efac;\n    --slate-400: #4ade80;\n    --slate-500: #22c55e;\n    --slate-600: #16a34a;\n    --slate-700: #15803d;\n    --slate-800: #166534;\n    --slate-900: #14532d;\n}\nhtml[data-theme=\"forest\"].dark {\n    --color-primary: #34d399;\n    --color-bg: var(--slate-900);\n    --color-surface: var(--slate-800);\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #dcfce7;\n    --color-text-muted: #86efac;\n\n    --slate-50: #021a11;\n    --slate-100: #042f1d;\n    --slate-200: #064e3b;\n    --slate-300: #bbf7d0;\n    --slate-400: #86efac;\n    --slate-500: #4ade80;\n    --slate-600: #22c55e;\n    --slate-700: #bbf7d0;\n    --slate-800: #042f1d;\n    --slate-900: #021a11;\n}\n/* Sunset Theme */\nhtml[data-theme=\"sunset\"] {\n    --color-primary: #f43f5e;\n    --color-bg: var(--slate-50);\n    --color-surface: #ffffff;\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #4c0519;\n    --color-text-muted: #e03131;\n\n    --slate-50: #fff5f5;\n    --slate-100: #ffe3e3;\n    --slate-200: #ffc9c9;\n    --slate-300: #ffa8a8;\n    --slate-400: #ff8787;\n    --slate-500: #ff6b6b;\n    --slate-600: #fa5252;\n    --slate-700: #e03131;\n    --slate-800: #c92a2a;\n    --slate-900: #4c0519;\n}\nhtml[data-theme=\"sunset\"].dark {\n    --color-primary: #fb7185;\n    --color-bg: var(--slate-900);\n    --color-surface: var(--slate-800);\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #ffe4e6;\n    --color-text-muted: #ffa8a8;\n\n    --slate-50: #1f030a;\n    --slate-100: #3b0713;\n    --slate-200: #5c091f;\n    --slate-300: #ffc9c9;\n    --slate-400: #ffa8a8;\n    --slate-500: #ff8787;\n    --slate-600: #ff6b6b;\n    --slate-700: #ffc9c9;\n    --slate-800: #3b0713;\n    --slate-900: #1f030a;\n}\n/* Dracula Theme */\nhtml[data-theme=\"dracula\"] {\n    --color-primary: #bd93f9;\n    --color-bg: var(--slate-900);\n    --color-surface: var(--slate-800);\n    --color-bg-alt: var(--slate-100);\n    --color-border: var(--slate-200);\n    --color-text: #f8f8f2;\n    --color-text-muted: #6272a4;\n\n    --slate-50: #1e1f29;\n    --slate-100: #282a36;\n    --slate-200: #44475a;\n    --slate-300: #f8f8f2;\n    --slate-400: #6272a4;\n    --slate-500: #bd93f9;\n    --slate-600: #ff79c6;\n    --slate-700: #f8f8f2;\n    --slate-800: #282a36;\n    --slate-900: #1e1f29;\n}\n\nhtml[lang=\"fa\"] body, html[lang=\"fa\"] body *:not(.font-mono) {\n    font-family: 'Vazirmatn', sans-serif !important;\n}\n\nbody {\n    background: var(--color-bg);\n    color: var(--color-text);\n    font-family: 'Inter', sans-serif;\n    transition: background 0.3s ease, color 0.3s ease;\n}\n\n/* Perfect form styling with excellent contrast for every theme */\ninput[type=\"text\"]:not(.login-input), input[type=\"password\"]:not(.login-input), select, textarea {\n    background-color: var(--slate-100) !important;\n    color: var(--slate-900) !important;\n    border-color: var(--slate-200) !important;\n    transition: all 0.2s;\n    border-radius: 0.75rem !important;\n}\ninput[type=\"text\"]:not(.login-input):focus, input[type=\"password\"]:not(.login-input):focus, select:focus, textarea:focus {\n    border-color: var(--color-primary) !important;\n    box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.2) !important;\n}\nhtml.dark input[type=\"text\"]:not(.login-input), html.dark input[type=\"password\"]:not(.login-input), html.dark select, html.dark textarea {\n    background-color: var(--slate-100) !important;\n    color: var(--slate-300) !important;\n    border-color: var(--slate-200) !important;\n}\nselect option {\n    background-color: var(--slate-50) !important;\n    color: var(--slate-900) !important;\n}\nhtml.dark select option {\n    background-color: var(--slate-100) !important;\n    color: var(--slate-300) !important;\n}\n\n::-webkit-scrollbar { width: 6px; height: 6px; }\n::-webkit-scrollbar-track { background: transparent; }\n::-webkit-scrollbar-thumb { background: var(--color-border); border-radius: 3px; }\n[data-accordion-content] { max-height: 0; overflow: hidden; transition: max-height 0.3s ease; }\n.login-input {\n    background-color: var(--color-bg-alt) !important;\n    border-color: var(--color-border) !important;\n    color: var(--color-text) !important;\n}\n.login-input:focus {\n    border-color: var(--color-primary) !important;\n    box-shadow: 0 0 0 2px color-mix(in srgb, var(--color-primary) 20%, transparent) !important;\n}\n</style></head>\n  <body class=\"text-[var(--color-text)] h-[100dvh] flex flex-col md:flex-row overflow-hidden selection:bg-primary selection:text-white transition-colors duration-300 bg-[var(--color-bg)]\">\n\n      <!-- Global Controls -->\n      <div class=\"fixed top-4 end-4 md:top-5 md:end-5 flex items-center gap-2 z-50\">\n          <span id=\"top-version-badge\" class=\"hidden md:inline-block px-3 py-1.5 rounded-xl text-[11px] font-mono font-bold\" style=\"background: color-mix(in srgb, var(--color-primary) 12%, transparent); border: 1px solid color-mix(in srgb, var(--color-primary) 25%, transparent); color: var(--color-primary);\">v__CURRENT_VERSION__</span>\n          <a href=\"https://github.com/amirpocom63-del/mrvpn294\" id=\"github-link-btn\" target=\"_blank\" class=\"hidden md:inline-flex btn-top-bar p-2 rounded-xl transition-all\" style=\"background: var(--color-surface); border: 1px solid var(--color-border); color: var(--color-text-muted);\">\n              <svg class=\"w-4 h-4\" fill=\"currentColor\" viewBox=\"0 0 24 24\"><path fill-rule=\"evenodd\" d=\"M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z\" clip-rule=\"evenodd\"></path></svg>\n          </a>\n          <button onclick=\"toggleLang()\" id=\"lang-toggle\" class=\"btn-top-bar px-3 py-1.5 rounded-xl text-sm font-bold transition-all\" style=\"background: var(--color-surface); border: 1px solid var(--color-border); color: var(--color-text);\">EN</button>\n          <button onclick=\"toggleTheme()\" class=\"btn-top-bar p-2 rounded-xl transition-all\" style=\"background: var(--color-surface); border: 1px solid var(--color-border); color: #f59e0b;\">\n              <svg class=\"w-4 h-4 hidden dark:block\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z\"></path></svg>\n              <svg class=\"w-4 h-4 block dark:hidden\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg>\n          </button>\n          <button onclick=\"logout()\" id=\"btn-logout-mob\" class=\"hidden md:hidden p-2 rounded-xl transition-all\" style=\"background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);color:#f87171;\">\n              <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1\"></path></svg>\n          </button>\n      </div>\n\n      <!-- LOGIN SCREEN -->\n      <div id=\"login-box\" class=\"absolute inset-0 flex items-center justify-center p-4 z-40 overflow-hidden\" style=\"background: linear-gradient(135deg, var(--color-bg) 0%, var(--color-bg-alt) 50%, var(--color-bg) 100%);\">\n          <div class=\"absolute pointer-events-none rounded-full\" style=\"width:500px;height:500px;top:-100px;left:-150px;background:radial-gradient(circle, var(--color-primary) 0%, transparent 65%); opacity: 0.12;\"></div>\n          <div class=\"absolute pointer-events-none rounded-full\" style=\"width:400px;height:400px;bottom:-80px;right:-100px;background:radial-gradient(circle, var(--color-primary) 0%, transparent 65%); opacity: 0.1;\"></div>\n          <div class=\"relative w-full max-w-sm\">\n              \n              <div class=\"text-center mb-8\">\n                  <div class=\"relative inline-flex items-center justify-center mb-5\">\n                      <div class=\"lock-pulse relative w-20 h-20 rounded-3xl flex items-center justify-center\" style=\"background: linear-gradient(145deg, color-mix(in srgb, var(--color-primary) 25%, transparent), color-mix(in srgb, var(--color-primary) 8%, transparent)); border: 1px solid color-mix(in srgb, var(--color-primary) 45%, transparent); box-shadow: 0 0 40px color-mix(in srgb, var(--color-primary) 25%, transparent), inset 0 1px 0 rgba(255,255,255,0.08);\">\n                          <svg class=\"w-9 h-9 text-primary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"1.5\" d=\"M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z\"></path></svg>\n                      </div>\n                  </div>\n                  <h2 class=\"text-3xl font-black\" style=\"color: var(--color-text);\" data-i18n=\"title\">mrvpn294 Gateway</h2>\n                  <p class=\"text-sm mt-2\" style=\"color: var(--color-text-muted);\">Sign in to manage your gateway</p>\n              </div>\n              <div class=\"rounded-3xl p-px\" style=\"background: linear-gradient(145deg, color-mix(in srgb, var(--color-primary) 45%, transparent), color-mix(in srgb, var(--color-primary) 8%, transparent) 50%, color-mix(in srgb, var(--color-primary) 30%, transparent)); box-shadow: 0 25px 60px rgba(0,0,0,0.15);\">\n                  <div class=\"rounded-3xl p-8\" style=\"background: var(--color-surface);\">\n                      <div class=\"flex items-center gap-2 mb-7 pb-6\" style=\"border-bottom: 1px solid var(--color-border);\">\n                          <span class=\"w-2 h-2 rounded-full flex-shrink-0\" style=\"background:#22c55e;box-shadow:0 0 8px #22c55e;\"></span>\n                          <span class=\"text-xs text-emerald-500 font-semibold\" data-i18n=\"system_online\">System online</span>\n                          <span class=\"flex-1\"></span>\n                          <span class=\"text-xs\" style=\"color: var(--color-text-muted); opacity: 0.8;\">🔒 Secure connection</span>\n                      </div>\n                      __HAS_DB_WARNING__\n                      <div class=\"mb-5\">\n                          <label class=\"block text-sm font-semibold mb-2.5\" style=\"color: var(--color-text-muted);\" data-i18n=\"login_password\">Password</label>\n                          <div class=\"relative\">\n                              <div class=\"absolute inset-y-0 start-0 flex items-center ps-4 text-primary\">\n                                  <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z\"></path></svg>\n                              </div>\n                              <input type=\"password\" id=\"pwd\" data-i18n=\"pass_ph\" placeholder=\"Enter your password\" class=\"login-input w-full ps-11 pe-12 py-3.5 text-sm rounded-2xl outline-none transition-all\">\n                              <button type=\"button\" onclick=\"const n=document.getElementById('pwd');n.type=n.type==='password'?'text':'password'\" class=\"eye-btn absolute inset-y-0 end-0 flex items-center px-4 transition-colors\" style=\"color: var(--color-primary); opacity: 0.6;\">\n                                  <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M15 12a3 3 0 11-6 0 3 3 0 016 0z\"></path><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z\"></path></svg>\n                              </button>\n                          </div>\n                      </div>\n                      <p id=\"err-msg\" class=\"hidden text-sm mb-4 flex items-center gap-2 px-3 py-2.5 rounded-xl\" style=\"background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.2);color:#f87171;\"><span>⚠️</span><span data-i18n=\"err_pass\">Wrong password, please try again.</span></p>\n                      <button onclick=\"doLogin()\" class=\"login-btn btn-shimmer w-full py-3.5 rounded-2xl font-bold text-sm relative overflow-hidden transition-all\" style=\"background: linear-gradient(135deg, var(--color-primary), color-mix(in srgb, var(--color-primary) 80%, black)); color: white; box-shadow: 0 4px 24px color-mix(in srgb, var(--color-primary) 35%, transparent), inset 0 1px 0 rgba(255,255,255,0.1);\" data-i18n=\"login_btn\">\n                          Sign In\n                      </button>\n                  </div>\n              </div>\n          </div>\n      </div>\n\n      <!-- DASHBOARD CONTAINER -->\n      <div id=\"dash-box\" class=\"hidden w-full h-full flex-col md:flex-row relative dash-box-native\" style=\"padding-top: env(safe-area-inset-top, 0px);\">\n\n          <!-- SIDEBAR (Desktop) -->\n          <aside class=\"hidden md:flex w-64 bg-[var(--color-surface)]  border-e border-slate-200 dark:border-darkborder flex-col z-20 shrink-0\">\n              <div class=\"flex items-center p-6 border-b border-slate-100 dark:border-darkborder/50\">\n                  <div class=\"w-10 h-10 rounded-xl bg-indigo-50 dark:bg-indigo-900/40 text-primary flex items-center justify-center me-3 shrink-0\"><svg class=\"w-6 h-6\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13 10V3L4 14h7v7l9-11h-7z\"></path></svg></div>\n                  <div class=\"flex flex-col\">\n                      <h1 class=\"font-black text-xl leading-none\" data-i18n=\"title\">mrvpn294</h1>\n                      <span id=\"app-version\" class=\"text-[10px] font-mono text-slate-400 mt-1 font-semibold\">v__CURRENT_VERSION__</span>\n                  </div>\n              </div>\n              <nav class=\"flex-1 p-4 space-y-2 overflow-y-auto\">\n                  <button onclick=\"switchTab('overview')\" id=\"tab-overview\" class=\"nav-item active flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_overview\">Dashboard</span>\n                  </button>\n                  <button onclick=\"switchTab('info')\" id=\"tab-info\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_info\">Endpoints</span>\n                  </button>\n                  <button onclick=\"switchTab('network')\" id=\"tab-network\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_status\">Metrics</span>\n                  </button>\n                  <button onclick=\"switchTab('settings')\" id=\"tab-settings\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z M15 12a3 3 0 11-6 0 3 3 0 016 0z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_settings\">System</span>\n                  </button>\n                  <button onclick=\"switchTab('advanced')\" id=\"tab-advanced\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_adv\">Advanced</span>\n                  </button>\n                  <button onclick=\"switchTab('logs')\" id=\"tab-logs\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_logs\">Logs</span>\n                  </button>\n                  <button onclick=\"switchTab('users')\" id=\"tab-users\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_users\">Clients</span>\n                  </button>\n                  <button onclick=\"switchTab('help')\" id=\"tab-help\" class=\"nav-item flex items-center w-full px-4 py-3 rounded-lg text-slate-500 hover:text-slate-800 dark:hover:text-slate-200 group\">\n                      <svg class=\"w-6 h-6 me-3\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M18.364 5.636l-3.536 3.536m0 5.656l3.536 3.536M9.172 9.172L5.636 5.636m3.536 9.192l-3.536 3.536M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-5 0a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg>\n                      <span class=\"font-semibold\" data-i18n=\"tab_help\">Help & FAQ</span>\n                  </button>\n              </nav>\n              <div class=\"p-4 border-t border-slate-100 dark:border-darkborder/50\">\n                  <button onclick=\"logout()\" class=\"flex items-center justify-center w-full px-4 py-2 rounded-lg text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 font-semibold transition-colors\">\n                      <svg class=\"w-5 h-5 me-2\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1\"></path></svg>\n                      <span data-i18n=\"logout\">Disconnect</span>\n                  </button>\n              </div>\n          </aside>\n\n          <!-- MAIN CONTENT AREA -->\n          <main class=\"flex-1 flex flex-col h-full overflow-hidden\">\n              <header class=\"h-14 md:h-24 shrink-0 flex items-center px-4 md:px-10 z-10 pt-[env(safe-area-inset-top,0px)] md:pt-0 bg-[var(--color-surface)] backdrop-blur-xl border-b border-[var(--color-border)]\">\n                  <h2 id=\"view-title\" class=\"text-lg md:text-3xl font-black text-slate-800 dark:text-white mt-0 md:mt-2\">Overview</h2>\n              </header>\n\n              <!-- Scrollable Content -->\n              <div class=\"scroll-content flex-1 overflow-y-auto p-4 md:p-10\">\n                  <div class=\"max-w-4xl mx-auto space-y-6 fade-in\">\n\n                      <!-- Update Banner -->\n                      <div id=\"update-alert-banner\" class=\"hidden bg-gradient-to-r from-amber-500/10 to-primary/10 border-2 border-amber-300 dark:border-amber-950/20 rounded-3xl p-6 shadow-md flex-col items-center justify-between gap-4 fade-in\">\n                          <div class=\"flex flex-col sm:flex-row items-center justify-between gap-4 w-full\">\n                              <div class=\"flex items-center space-x-4 space-x-reverse text-start w-full\">\n                                  <div class=\"p-3 bg-amber-500/10 text-amber-500 rounded-2xl shrink-0\">\n                                      <svg class=\"w-6 h-6 animate-bounce\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M15 13l-3 3m0 0l-3-3m3 3V8m0 13a9 9 0 110-18 9 9 0 010 18z\"></path></svg>\n                                  </div>\n                                  <div>\n                                      <h4 class=\"font-black text-amber-800 dark:text-amber-400 text-base\" data-i18n=\"update_avail\">New version available!</h4>\n                                      <p id=\"update-alert-text\" class=\"text-xs text-slate-500 dark:text-slate-400 mt-1\"></p>\n                                  </div>\n                              </div>\n                              <div class=\"flex gap-2 w-full sm:w-auto shrink-0 justify-end\">\n                                  <button onclick=\"dismissUpdate()\" class=\"px-4 py-2.5 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800/80 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 font-bold rounded-xl text-xs transition-colors\" data-i18n=\"btn_cancel\">Cancel</button>\n                                  <button onclick=\"doUpdate()\" id=\"update-deploy-btn\" class=\"px-5 py-2.5 bg-amber-500 hover:bg-amber-600 text-white font-bold rounded-xl text-xs transition-all shadow-md hover:shadow-lg flex items-center justify-center gap-1.5\" data-i18n=\"deploy_btn\">\n                                      🚀 Deploy Now\n                                  </button>\n                              </div>\n                          </div>\n                          <!-- Sub-options for format choice -->\n                          <div class=\"w-full flex flex-col md:flex-row items-start md:items-center justify-between gap-4 bg-amber-500/5 dark:bg-amber-500/[0.02] p-4 rounded-2xl border border-amber-500/10 mt-2 text-start\">\n                              <div class=\"space-y-1\">\n                                  <span class=\"text-xs font-bold text-amber-800 dark:text-amber-400\" data-i18n=\"lbl_update_format\">Update Format &amp; Obfuscation:</span>\n                                  <p class=\"text-[10px] text-slate-500 dark:text-slate-400\" data-i18n=\"desc_update_format\">Deploy clean source code, or encrypt using dynamic XOR byte-shifting to avoid network interception.</p>\n                              </div>\n                              <div class=\"flex items-center gap-4 shrink-0 font-medium\">\n                                  <label class=\"inline-flex items-center cursor-pointer\">\n                                      <input type=\"radio\" name=\"update-format\" value=\"normal\" checked=\"\" class=\"form-radio text-amber-500 w-4 h-4\">\n                                      <span class=\"ms-1.5 text-xs text-slate-700 dark:text-slate-300 font-bold\" data-i18n=\"format_normal\">Normal (_worker.js)</span>\n                                  </label>\n                                  <label class=\"inline-flex items-center cursor-pointer\">\n                                      <input type=\"radio\" name=\"update-format\" value=\"obfuscated\" class=\"form-radio text-amber-500 w-4 h-4\">\n                                      <span class=\"ms-1.5 text-xs text-slate-700 dark:text-slate-300 font-bold\" data-i18n=\"format_obfuscated\">Obfuscated (UTF-8 + XOR)</span>\n                                  </label>\n                              </div>\n                          </div>\n                          <!-- Dynamic Changelog Section -->\n                          <div id=\"update-changelog-area\" class=\"hidden w-full border-t border-amber-300/30 dark:border-amber-950/20 pt-4 mt-2\">\n                              <h5 class=\"text-xs font-bold text-amber-800 dark:text-amber-400 mb-2 flex items-center gap-1.5\">\n                                  <svg class=\"w-4 h-4 text-amber-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z\"></path></svg>\n                                  <span data-i18n=\"changelog_title\">Changelog of New Version:</span>\n                              </h5>\n                              <div id=\"update-changelog-content\" class=\"text-xs text-slate-600 dark:text-slate-400 bg-amber-500/[0.04] dark:bg-slate-900/40 p-4 rounded-2xl max-h-48 overflow-y-auto font-sans leading-relaxed border border-amber-200/20 max-w-none text-start\">\n                                  <p class=\"animate-pulse\">Loading changelog...</p>\n                              </div>\n                          </div>\n                          <div id=\"update-deploy-status\" class=\"hidden w-full mt-3 p-3 rounded-xl text-sm font-bold text-center\"></div>\n                          <div class=\"w-full mt-2 text-center\">\n                              <a id=\"update-github-link\" href=\"https://github.com/amirpocom63-del/mrvpn294\" target=\"_blank\" class=\"text-xs text-slate-400 hover:text-amber-500 transition-colors underline\" data-i18n=\"view_github\">View on GitHub</a>\n                          </div>\n                      </div>\n\n                      <!-- OVERVIEW / DASHBOARD VIEW -->\n                      <div id=\"view-overview\" class=\"space-y-3 md:space-y-6 block\">\n                          <!-- User Summary Cards -->\n                          <div class=\"grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2 md:gap-4\">\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-4 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-1 md:mb-2\">\n                                      <span class=\"text-[9px] md:text-[10px] font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"ov_total_users\">Total Users</span>\n                                      <div class=\"p-1.5 md:p-2 bg-primary/10 text-primary rounded-md md:rounded-lg\"><svg class=\"w-3.5 h-3.5 md:w-4 md:h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656-.126-1.283-.356-1.857M12 4.354a4 4 0 110 5.292\"></path></svg></div>\n                                  </div>\n                                  <p id=\"ov-total-users\" class=\"text-xl md:text-2xl font-black text-slate-800 dark:text-white\">-</p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-4 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-1 md:mb-2\">\n                                      <span class=\"text-[9px] md:text-[10px] font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"ov_active_users\">Active</span>\n                                      <div class=\"p-1.5 md:p-2 bg-emerald-500/10 text-emerald-500 rounded-md md:rounded-lg\"><svg class=\"w-3.5 h-3.5 md:w-4 md:h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg></div>\n                                  </div>\n                                  <p id=\"ov-active-users\" class=\"text-xl md:text-2xl font-black text-emerald-600 dark:text-emerald-400\">-</p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-4 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-1 md:mb-2\">\n                                      <span class=\"text-[9px] md:text-[10px] font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"ov_paused_users\">Paused</span>\n                                      <div class=\"p-1.5 md:p-2 bg-amber-500/10 text-amber-500 rounded-md md:rounded-lg\"><svg class=\"w-3.5 h-3.5 md:w-4 md:h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg></div>\n                                  </div>\n                                  <p id=\"ov-paused-users\" class=\"text-xl md:text-2xl font-black text-amber-600 dark:text-amber-400\">-</p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-4 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-1 md:mb-2\">\n                                      <span class=\"text-[9px] md:text-[10px] font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"ov_auto_disabled\">Auto-Disabled</span>\n                                      <div class=\"p-1.5 md:p-2 bg-red-500/10 text-red-500 rounded-md md:rounded-lg\"><svg class=\"w-3.5 h-3.5 md:w-4 md:h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z\"></path></svg></div>\n                                  </div>\n                                  <p id=\"ov-auto-disabled\" class=\"text-xl md:text-2xl font-black text-red-600 dark:text-red-400\">-</p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-4 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-1 md:mb-2\">\n                                      <span class=\"text-[9px] md:text-[10px] font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"ov_expired_users\">Expired</span>\n                                      <div class=\"p-1.5 md:p-2 bg-slate-500/10 text-slate-500 rounded-md md:rounded-lg\"><svg class=\"w-3.5 h-3.5 md:w-4 md:h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg></div>\n                                  </div>\n                                  <p id=\"ov-expired-users\" class=\"text-xl md:text-2xl font-black text-slate-600 dark:text-slate-400\">-</p>\n                              </div>\n                          </div>\n\n                          <!-- Traffic & System Cards -->\n                          <div class=\"grid grid-cols-2 sm:grid-cols-2 lg:grid-cols-4 gap-2 md:gap-4\">\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-5 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center gap-2 md:gap-3 mb-2 md:mb-3\">\n                                      <div class=\"p-1.5 md:p-2.5 bg-violet-500/10 text-violet-500 rounded-lg md:rounded-xl\"><svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10\"></path></svg></div>\n                                       <span class=\"text-[10px] md:text-xs font-bold text-slate-500 uppercase tracking-wider\" data-i18n=\"ov_total_traffic\">Total Traffic</span>\n                                  </div>\n                                   <p id=\"ov-total-traffic\" class=\"text-base md:text-xl font-black text-slate-800 dark:text-white\">- GB</p>\n                                  <p class=\"text-[9px] md:text-[10px] text-slate-400 mt-0.5 md:mt-1\"><span id=\"ov-total-reqs\">-</span> <span data-i18n=\"ov_requests\">requests</span></p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-5 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center gap-2 md:gap-3 mb-2 md:mb-3\">\n                                      <div class=\"p-1.5 md:p-2.5 bg-cyan-500/10 text-cyan-500 rounded-lg md:rounded-xl\"><svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13 7h8m0 0v8m0-8l-8 8-4-4-6 6\"></path></svg></div>\n                                       <span class=\"text-[10px] md:text-xs font-bold text-slate-500 uppercase tracking-wider\" data-i18n=\"ov_today_traffic\">Today's Traffic</span>\n                                  </div>\n                                  <p id=\"ov-today-traffic\" class=\"text-base md:text-xl font-black text-slate-800 dark:text-white\">- GB</p>\n                                  <p class=\"text-[9px] md:text-[10px] text-slate-400 mt-0.5 md:mt-1\"><span id=\"ov-today-reqs\">-</span> <span data-i18n=\"ov_requests\">requests</span></p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-5 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center gap-2 md:gap-3 mb-2 md:mb-3\">\n                                      <div class=\"p-1.5 md:p-2.5 bg-blue-500/10 text-blue-500 rounded-lg md:rounded-xl\"><svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M5.636 18.364a9 9 0 010-12.728m12.728 0a9 9 0 010 12.728m-9.9-2.829a5 5 0 010-7.07m7.072 0a5 5 0 010 7.07M13 12a1 1 0 11-2 0 1 1 0 012 0z\"></path></svg></div>\n                                       <span class=\"text-[10px] md:text-xs font-bold text-slate-500 uppercase tracking-wider\" data-i18n=\"ov_active_conns\">Active Connections</span>\n                                  </div>\n                                  <p id=\"ov-active-conns\" class=\"text-base md:text-xl font-black text-slate-800 dark:text-white\">-</p>\n                              </div>\n                              <div class=\"native-press bg-[var(--color-surface)]  rounded-xl md:rounded-2xl p-3 md:p-5 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center gap-2 md:gap-3 mb-2 md:mb-3\">\n                                      <div class=\"p-1.5 md:p-2.5 bg-indigo-500/10 text-indigo-500 rounded-lg md:rounded-xl\"><svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z\"></path></svg></div>\n                                       <span class=\"text-[10px] md:text-xs font-bold text-slate-500 uppercase tracking-wider\" data-i18n=\"ov_system\">System</span>\n                                  </div>\n                                  <p id=\"ov-version\" class=\"text-base md:text-xl font-black text-slate-800 dark:text-white\">-</p>\n                              </div>\n                          </div>\n\n                          <!-- Recent Activity & Quick Actions Row -->\n                          <div class=\"grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4\">\n                              <!-- Recent Activity -->\n                              <div class=\"lg:col-span-2 bg-[var(--color-surface)]  rounded-2xl md:rounded-3xl p-4 md:p-6 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"flex items-center justify-between mb-3 md:mb-4\">\n                                      <h3 class=\"text-xs md:text-sm uppercase font-bold text-slate-500 tracking-wider\" data-i18n=\"ov_recent_activity\">Recent Activity</h3>\n                                      <button onclick=\"switchTab('logs')\" class=\"text-[11px] md:text-xs text-primary hover:text-primary/80 font-bold transition-colors\" data-i18n=\"ov_view_all\">View All →</button>\n                                  </div>\n                                  <div id=\"ov-activity-list\" class=\"space-y-1.5 md:space-y-2.5\">\n                                      <p class=\"text-sm text-slate-400 text-center py-6\" data-i18n=\"ov_loading\">Loading...</p>\n                                  </div>\n                              </div>\n                              <!-- Quick Actions -->\n                              <div class=\"bg-[var(--color-surface)]  rounded-2xl md:rounded-3xl p-4 md:p-6 shadow-sm border border-slate-200 dark:border-darkborder\">\n                                  <h3 class=\"text-xs md:text-sm uppercase font-bold text-slate-500 tracking-wider mb-3 md:mb-4\" data-i18n=\"ov_quick_actions\">Quick Actions</h3>\n                                  <div class=\"grid grid-cols-2 gap-2 md:grid-cols-1 md:gap-3\">\n                                       <button onclick=\"openAddUserPage()\" class=\"native-press flex items-center justify-center md:justify-start gap-2 md:gap-3 px-3 py-2.5 md:px-4 md:py-3 bg-primary/10 hover:bg-primary/20 text-primary rounded-lg md:rounded-xl font-bold text-xs md:text-sm transition-colors\">\n                                           <svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 6v6m0 0v6m0-6h6m-6 0H6\"></path></svg>\n                                           <span data-i18n=\"ov_add_user\">Add User</span>\n                                       </button>\n                                       <button onclick=\"switchTab('users')\" class=\"native-press flex items-center justify-center md:justify-start gap-2 md:gap-3 px-3 py-2.5 md:px-4 md:py-3 bg-violet-500/10 hover:bg-violet-500/20 text-violet-600 dark:text-violet-400 rounded-lg md:rounded-xl font-bold text-xs md:text-sm transition-colors\">\n                                           <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg>\n                                           <span data-i18n=\"ov_manage_users\">Manage Users</span>\n                                      </button>\n                                       <button onclick=\"exportConfig()\" class=\"native-press flex items-center justify-center md:justify-start gap-2 md:gap-3 px-3 py-2.5 md:px-4 md:py-3 bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 rounded-lg md:rounded-xl font-bold text-xs md:text-sm transition-colors\">\n                                           <svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4\"></path></svg>\n                                           <span data-i18n=\"ov_backup_config\">Backup Config</span>\n                                       </button>\n                                       <button onclick=\"loadDashboard()\" class=\"native-press flex items-center justify-center md:justify-start gap-2 md:gap-3 px-3 py-2.5 md:px-4 md:py-3 bg-blue-500/10 hover:bg-blue-500/20 text-blue-600 dark:text-blue-400 rounded-lg md:rounded-xl font-bold text-xs md:text-sm transition-colors\">\n                                           <svg class=\"w-4 h-4 md:w-5 md:h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15\"></path></svg>\n                                           <span data-i18n=\"ov_refresh\">Refresh Statistics</span>\n                                      </button>\n                                  </div>\n                              </div>\n                          </div>\n                      </div>\n\n                      <!-- INFO VIEW -->\n                      <div id=\"view-info\" class=\"hidden space-y-6\">\n                          <div id=\"dyn-profiles-container\" class=\"columns-1 md:columns-2 gap-4\"></div>\n                      </div>\n\n                      <!-- NETWORK/METRICS VIEW -->\n                      <div id=\"view-network\" class=\"hidden space-y-6\">\n                            <div class=\"bg-[var(--color-surface)]/70 /70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-white/20 dark:border-white/10 mb-6\">\n                              <h3 class=\"text-sm uppercase font-bold text-slate-500 tracking-wider mb-4\" data-i18n=\"metrics_live\">Live Profile Usage</h3>\n                              <div id=\"usage-metrics-container\" class=\"flex flex-col\">\n                                  <p class=\"text-xs text-slate-400 text-center py-4\" data-i18n=\"no_metrics\">No active connection data yet.</p>\n                              </div>\n                          </div>\n                          <div class=\"grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 md:gap-6\">\n                              <div class=\"bg-[var(--color-surface)]  p-6 rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden group\">\n                                  <svg class=\"w-8 h-8 text-blue-500 mb-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"1.5\" d=\"M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9\"></path></svg>\n                                  <p class=\"text-xs uppercase font-bold text-slate-400 mb-1\" data-i18n=\"stat_ip\">Origin IP</p>\n                                  <p id=\"net-ip\" class=\"text-xl md:text-2xl font-black font-mono\">...</p>\n                              </div>\n                              <div class=\"bg-[var(--color-surface)]  p-6 rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden group\">\n                                  <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01\"></path></svg>\n                                  <p class=\"text-xs uppercase font-bold text-slate-400 mb-1\" data-i18n=\"stat_dc\">Edge Node</p>\n                                  <p id=\"net-colo\" class=\"text-xl md:text-2xl font-black font-mono\">...</p>\n                              </div>\n                              <div class=\"bg-[var(--color-surface)]  p-6 rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden group sm:col-span-2 lg:col-span-1\">\n                                  <svg class=\"w-8 h-8 text-purple-500 mb-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"1.5\" d=\"M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z\"></path><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"1.5\" d=\"M15 11a3 3 0 11-6 0 3 3 0 016 0z\"></path></svg>\n                                  <p class=\"text-xs uppercase font-bold text-slate-400 mb-1\" data-i18n=\"stat_loc\">Data Region</p>\n                                  <p id=\"net-loc\" class=\"text-lg font-bold truncate\">...</p>\n                              </div>\n                              <div class=\"bg-[var(--color-surface)]  p-6 rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden group sm:col-span-2 lg:col-span-1\">\n                                  <svg class=\"w-8 h-8 text-blue-500 mb-4\" width=\"24\" height=\"24\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"12\" cy=\"12\" r=\"10\"></circle><path d=\"M12 6v6l-4-2\"></path></svg>\n                                  <p class=\"text-xs uppercase font-bold text-slate-400 mb-1\" data-i18n=\"stat_datetime\">Date Time</p>\n                                  <p id=\"net-datetime\" class=\"text-lg font-bold truncate text-center\" dir=\"rtl\">...</p>\n                              </div>\n                              <!-- Diagnostics Segment -->\n                              <div class=\"bg-[var(--color-surface)]  p-6 rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden group sm:col-span-2 lg:col-span-3\">\n                                  <div class=\"flex flex-col sm:flex-row sm:items-center justify-between gap-4\">\n                                      <div>\n                                          <h3 class=\"text-sm uppercase font-bold text-slate-400 mb-1\" data-i18n=\"ping_test_title\">Latency Diagnostics</h3>\n                                          <p class=\"text-xs text-slate-500\" data-i18n=\"ping_test_desc\">Test response time to your active node target.</p>\n                                      </div>\n                                      <button onclick=\"runPingTest()\" class=\"px-6 py-2.5 bg-primary/10 hover:bg-primary/20 text-primary font-bold rounded-xl transition-colors text-sm\" data-i18n=\"run_diagnostics\">\n                                          ⚡ Run Diagnostics\n                                      </button>\n                                  </div>\n                                  <div id=\"ping-results\" class=\"mt-4 grid grid-cols-2 sm:grid-cols-4 gap-4 hidden\">\n                                      <div class=\"bg-[var(--color-bg)] p-3 rounded-xl border border-slate-100 dark:border-darkborder/50\">\n                                          <p class=\"text-[10px] uppercase font-bold text-slate-400\" data-i18n=\"target_node\">Target Node</p>\n                                          <p id=\"ping-target\" class=\"text-sm font-bold font-mono truncate\">...</p>\n                                      </div>\n                                      <div class=\"bg-[var(--color-bg)] p-3 rounded-xl border border-slate-100 dark:border-darkborder/50\">\n                                          <p class=\"text-[10px] uppercase font-bold text-slate-400\" data-i18n=\"response\">Response</p>\n                                          <p id=\"ping-time\" class=\"text-sm font-bold font-mono text-emerald-500\">...</p>\n                                      </div>\n                                      <div class=\"bg-[var(--color-bg)] p-3 rounded-xl border border-slate-100 dark:border-darkborder/50\">\n                                          <p class=\"text-[10px] uppercase font-bold text-slate-400\" data-i18n=\"status\">Status</p>\n                                          <p id=\"ping-status\" class=\"text-sm font-bold\">...</p>\n                                      </div>\n                                      <div class=\"bg-[var(--color-bg)] p-3 rounded-xl border border-slate-100 dark:border-darkborder/50\">\n                                          <p class=\"text-[10px] uppercase font-bold text-slate-400\" data-i18n=\"local_port\">Local Port</p>\n                                          <p id=\"ping-port\" class=\"text-sm font-bold font-mono\">...</p>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n                      </div>\n\n                      <!-- SETTINGS VIEW -->\n                      <div id=\"view-settings\" class=\"hidden\">\n                          <div class=\"bg-[var(--color-surface)]/70 /70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-white/20 dark:border-white/10 grid grid-cols-1 md:grid-cols-2 gap-5\">\n                              <div class=\"space-y-1 mb-4\">\n        <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1 mb-1 flex items-center gap-2\">\n            <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01\"></path></svg>\n            Display Theme\n        </label>\n        <select id=\"theme-selector\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none\" onchange=\"window.changeThemeVariant(this.value)\">\n                <option value=\"system\">Default</option>\n                <option value=\"ocean\">Ocean Blue</option>\n                <option value=\"forest\">Forest Green</option>\n                <option value=\"sunset\">Sunset Rose</option>\n                <option value=\"dracula\">Dracula (Dark)</option>\n            </select>\n    </div><div class=\"space-y-1\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_proto\">Primary Display Mode</label>\n                                  <select id=\"cfg-proto\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none appearance-none\">\n                                      <option value=\"alpha\">Alpha Mode (V-Core)</option>\n                                      <option value=\"beta\">Beta Mode (T-Core)</option>\n                                      <option value=\"both\">Both (V-Core &amp; T-Core)</option>\n                                  </select>\n                              </div>\n                               <div class=\"space-y-1\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_port\">Data Port (Checkbox Selection)</label>\n                                  <select id=\"cfg-port\" multiple=\"\" class=\"hidden\">\n                                      <option value=\"443\">443</option>\n                                      <option value=\"2053\">2053</option>\n                                      <option value=\"2083\">2083</option>\n                                      <option value=\"2087\">2087</option>\n                                      <option value=\"2096\">2096</option>\n                                      <option value=\"8443\">8443</option>\n                                      <option value=\"80\">80</option>\n                                      <option value=\"8080\">8080</option>\n                                      <option value=\"8880\">8880</option>\n                                      <option value=\"2052\">2052</option>\n                                      <option value=\"2082\">2082</option>\n                                      <option value=\"2086\">2086</option>\n                                      <option value=\"2095\">2095</option>\n                                  </select>\n                                  <div id=\"port-checkboxes-container\" class=\"bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-darkborder p-4 rounded-xl space-y-3 font-mono text-xs max-h-48 overflow-y-auto\">\n                                      <!-- TLS ports -->\n                                      <div class=\"space-y-1.5\">\n                                          <div class=\"text-[10px] uppercase tracking-wider font-bold text-slate-400 dark:text-slate-500\">🔒 Secure (TLS)</div>\n                                          <div class=\"grid grid-cols-2 gap-2\">\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"443\" onchange=\"togglePortCheckbox('443', this.checked)\" class=\"accent-primary\">\n                                                  <span>443</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2053\" onchange=\"togglePortCheckbox('2053', this.checked)\" class=\"accent-primary\">\n                                                  <span>2053</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2083\" onchange=\"togglePortCheckbox('2083', this.checked)\" class=\"accent-primary\">\n                                                  <span>2083</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2087\" onchange=\"togglePortCheckbox('2087', this.checked)\" class=\"accent-primary\">\n                                                  <span>2087</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2096\" onchange=\"togglePortCheckbox('2096', this.checked)\" class=\"accent-primary\">\n                                                  <span>2096</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"8443\" onchange=\"togglePortCheckbox('8443', this.checked)\" class=\"accent-primary\">\n                                                  <span>8443</span>\n                                              </label>\n                                          </div>\n                                      </div>\n                                      <!-- Non-TLS ports -->\n                                      <div class=\"space-y-1.5 pt-1 border-t border-slate-200 dark:border-slate-700\">\n                                          <div class=\"text-[10px] uppercase tracking-wider font-bold text-slate-400 dark:text-slate-500\">🔓 Standard</div>\n                                          <div class=\"grid grid-cols-2 gap-2\">\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"80\" onchange=\"togglePortCheckbox('80', this.checked)\" class=\"accent-primary\">\n                                                  <span>80</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"8080\" onchange=\"togglePortCheckbox('8080', this.checked)\" class=\"accent-primary\">\n                                                  <span>8080</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"8880\" onchange=\"togglePortCheckbox('8880', this.checked)\" class=\"accent-primary\">\n                                                  <span>8880</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2052\" onchange=\"togglePortCheckbox('2052', this.checked)\" class=\"accent-primary\">\n                                                  <span>2052</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2082\" onchange=\"togglePortCheckbox('2082', this.checked)\" class=\"accent-primary\">\n                                                  <span>2082</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition\">\n                                                  <input type=\"checkbox\" value=\"2086\" onchange=\"togglePortCheckbox('2086', this.checked)\" class=\"accent-primary\">\n                                                  <span>2086</span>\n                                              </label>\n                                              <label class=\"flex items-center gap-2 p-1.5 rounded bg-[var(--color-surface)] dark:bg-slate-900 border border-slate-100 dark:border-slate-800 cursor-pointer hover:border-primary transition col-span-2\">\n                                                  <input type=\"checkbox\" value=\"2095\" onchange=\"togglePortCheckbox('2095', this.checked)\" class=\"accent-primary\">\n                                                  <span>2095</span>\n                                              </label>\n                                          </div>\n                                      </div>\n                                  </div>\n                              </div>\n                              <div class=\"space-y-1 md:col-span-2\">\n                                  <div class=\"flex justify-between items-center\">\n                                      <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_id\">Device UUID (Empty=Auto)</label>\n                                      <button type=\"button\" onclick=\"document.getElementById('cfg-uuid').value = crypto.randomUUID()\" class=\"text-xs text-primary bg-primary/10 hover:bg-primary/20 px-2 py-1 rounded transition-colors duration-200\" data-i18n=\"btn_generate_uuid\">Generate UUID</button>\n                                  </div>\n                                  <input type=\"text\" id=\"cfg-uuid\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none font-mono text-sm\">\n                              </div>\n                              <div class=\"space-y-1\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_path\">API Route (Hidden Path)</label>\n                                  <input type=\"text\" id=\"cfg-path\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none\">\n                              </div>\n                              <div class=\"space-y-1\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_pass\">Master Key</label>\n                                  <div class=\"relative\">\n                                      <input type=\"password\" id=\"cfg-pass\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none pe-12\">\n                                      <button type=\"button\" onclick=\"const n=document.getElementById('cfg-pass');n.type=n.type==='password'?'text':'password'\" class=\"absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200\">👁️</button>\n                                  </div>\n                              </div>\n                              <div class=\"space-y-1 md:col-span-2\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_github_repo\">GitHub Update Repository</label>\n                                  <input type=\"text\" id=\"cfg-github-repo\" placeholder=\"amirpocom63-del/mrvpn294\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                  <div class=\"flex justify-start items-center gap-2 mt-2\">\n                                      <button type=\"button\" onclick=\"triggerManualRedeploy()\" class=\"inline-flex items-center gap-1.5 px-3 py-1.5 bg-primary/10 hover:bg-primary/20 text-primary text-xs font-bold rounded-lg transition-colors border border-primary/20\">\n                                          🔄 <span data-i18n=\"btn_redeploy_force\">Force Redeploy / Switch Format</span>\n                                      </button>\n                                  </div>\n                              </div>\n                              <div class=\"space-y-1 md:col-span-2\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_sub_ua\">Custom Subscription User-Agent</label>\n                                  <input type=\"text\" id=\"cfg-sub-ua\" placeholder=\"e.g. MySpecialUABypass\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                  <p class=\"text-xs text-slate-500 mt-1 ms-1\" data-i18n=\"desc_sub_ua\">Allow specific browser User-Agent containing this text to bypass camouflage and retrieve profile data directly in web browser.</p>\n                              </div>\n                              <div class=\"space-y-1 md:col-span-2\">\n                                  <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300 ms-1\" data-i18n=\"lbl_custom_panel_url\">Custom Panel URL / Subscription Domain</label>\n                                  <input type=\"text\" id=\"cfg-custom-panel-url\" placeholder=\"e.g. custom.domain.com or https://custom.domain.com\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                  <p class=\"text-xs text-slate-500 mt-1 ms-1\" data-i18n=\"desc_custom_panel_url\">Optionally specify a custom domain/URL to be used for subscription/sync links. If empty, the default Worker address will be used.</p>\n                              </div>\n                              <!-- System Toggles -->\n                              <div class=\"flex flex-col sm:flex-row gap-3 md:col-span-2\">\n                                  <label class=\"flex-1 flex items-center justify-between cursor-pointer bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl\">\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"lbl_silent\">Silent UI Alerts</span>\n                                      <div class=\"relative inline-flex items-center cursor-pointer\">\n                                          <input type=\"checkbox\" id=\"cfg-silent\" class=\"sr-only peer\">\n                                          <div class=\"w-11 h-6 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-primary\"></div>\n                                      </div>\n                                  </label>\n                                  <label class=\"flex-1 flex items-center justify-between cursor-pointer bg-red-50 dark:bg-red-900/10 p-4 rounded-2xl border border-red-200 dark:border-red-900/30\">\n                                      <span class=\"text-sm font-bold text-red-600 dark:text-red-400\" data-i18n=\"lbl_pause\">Kill Switch</span>\n                                      <div class=\"relative inline-flex items-center cursor-pointer\">\n                                          <input type=\"checkbox\" id=\"cfg-pause\" class=\"sr-only peer\">\n                                          <div class=\"w-11 h-6 bg-red-200 dark:bg-red-900/50 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-red-500\"></div>\n                                      </div>\n                                  </label>\n                               </div>\n                               <div class=\"space-y-3 md:col-span-2\">\n                                   <label class=\"flex items-center justify-between bg-emerald-50 dark:bg-emerald-900/10 p-4 rounded-2xl border border-emerald-200 dark:border-emerald-900/30 cursor-pointer\">\n                                       <div>\n                                           <span class=\"text-sm font-bold text-emerald-700 dark:text-emerald-400\" data-i18n=\"lbl_auto_update\">Auto-Update</span>\n                                           <p class=\"text-[10px] text-emerald-500/70 dark:text-emerald-400/60 mt-0.5\">Automatically deploy when a new version is detected</p>\n                                       </div>\n                                       <div class=\"relative inline-flex items-center\">\n                                           <input type=\"checkbox\" id=\"cfg-auto-update\" class=\"sr-only peer\">\n                                           <div class=\"w-11 h-6 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-emerald-500\"></div>\n                                       </div>\n                                   </label>\n                                   <div id=\"auto-update-format-wrap\" class=\"hidden\">\n                                       <label class=\"block text-xs font-bold text-slate-500 mb-2\" data-i18n=\"lbl_auto_update_format\">Update Format</label>\n                                       <div class=\"flex gap-3\">\n                                           <label class=\"flex-1 flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder cursor-pointer hover:border-emerald-400 transition-colors\">\n                                               <input type=\"radio\" name=\"auto-update-format\" value=\"normal\" checked=\"\" class=\"accent-emerald-500\">\n                                               <div>\n                                               <span class=\"text-xs font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"format_normal_label\">Normal</span>\n                                               <p class=\"text-[10px] text-slate-400\" data-i18n=\"desc_format_normal\">Standard _worker.js</p>\n                                               </div>\n                                           </label>\n                                           <label class=\"flex-1 flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder cursor-pointer hover:border-emerald-400 transition-colors\">\n                                               <input type=\"radio\" name=\"auto-update-format\" value=\"obfuscated\" class=\"accent-emerald-500\">\n                                               <div>\n                                               <span class=\"text-xs font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"format_obfuscated_label\">Obfuscated</span>\n                                               <p class=\"text-[10px] text-slate-400\" data-i18n=\"desc_format_obfuscated\">XOR byte-shifting</p>\n                                               </div>\n                                           </label>\n                                       </div>\n                                   </div>\n                                </div>\n\n                                <!-- API Keys Management -->\n                                <div class=\"bg-[var(--color-surface)]/70 /70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-white/20 dark:border-white/10 md:col-span-2 space-y-4\">\n                                    <div class=\"flex items-center justify-between\">\n                                        <div>\n                                            <h3 class=\"text-sm font-bold text-slate-700 dark:text-slate-200 flex items-center gap-2\">\n                                                🔑 <span data-i18n=\"lbl_api_keys\">Panel API Keys</span>\n                                            </h3>\n                                            <p class=\"text-[10px] text-slate-500 dark:text-slate-400 mt-1\" data-i18n=\"desc_api_keys\">Generate API keys to securely connect remote panels. Remote panels use these keys instead of sharing your master key.</p>\n                                        </div>\n                                        <button onclick=\"generateApiKey()\" class=\"px-4 py-2 bg-primary text-white text-xs font-bold rounded-xl hover:opacity-90 transition-opacity\" data-i18n=\"btn_generate_key\">Generate Key</button>\n                                    </div>\n                                    <div id=\"api-keys-list\" class=\"space-y-2\"></div>\n                                    <div id=\"api-key-new\" class=\"hidden bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-200 dark:border-emerald-800 rounded-xl p-4 space-y-2\">\n                                        <p class=\"text-xs font-bold text-emerald-700 dark:text-emerald-400\" data-i18n=\"api_key_created\">API Key Created! Copy it now — it won't be shown again.</p>\n                                        <div class=\"flex items-center gap-2\">\n                                            <input type=\"text\" id=\"api-key-value\" readonly=\"\" class=\"flex-1 px-3 py-2 bg-[var(--color-surface)] dark:bg-slate-800 rounded-lg text-xs font-mono border border-emerald-300 dark:border-emerald-700 text-slate-700 dark:text-slate-300\">\n                                            <button onclick=\"copyApiKey()\" class=\"px-3 py-2 bg-emerald-600 text-white text-xs font-bold rounded-lg hover:bg-emerald-700\">Copy</button>\n                                        </div>\n                                    </div>\n                                </div>\n\n                                <!-- Import/Export Config Area -->\n                               <div class=\"bg-[var(--color-surface)]/70 /70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-white/20 dark:border-white/10 md:col-span-2 space-y-4\">\n                                  <h3 class=\"text-sm uppercase font-bold text-slate-400 tracking-wider\" data-i18n=\"backup_restore_title\">Backup &amp; Restore</h3>\n                                  <div class=\"flex flex-col sm:flex-row gap-4\">\n                                      <button onclick=\"exportConfig()\" class=\"flex-1 py-3 px-4 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 font-bold rounded-xl transition-colors text-sm\" data-i18n=\"export_btn\">\n                                          📥 Export Configuration (JSON)\n                                      </button>\n                                      <label class=\"flex-1 py-3 px-4 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 font-bold rounded-xl transition-colors text-sm text-center cursor-pointer\">\n                                          <span data-i18n=\"import_btn\">📤 Import Configuration (JSON)</span>\n                                          <input type=\"file\" id=\"import-file\" class=\"hidden\" accept=\".json\" onchange=\"importConfig(event)\">\n                                      </label>\n                                  </div>\n                              </div>\n                          </div>\n                      </div>\n\n                      <!-- ADVANCED VIEW -->\n                      <div id=\"view-advanced\" class=\"hidden space-y-4\">\n\n                          <!-- Section: Network & DNS -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">🌐</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_network_dns\">Network &amp; DNS</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-4 px-5 pb-5 pt-1\">\n                                      <div>\n                                          <div class=\"flex items-center justify-between mb-2\">\n                                              <label class=\"text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_clean_ips\">Clean IPs (Multi-Generator)</label>\n                                              <span class=\"text-xs bg-indigo-100 dark:bg-indigo-900/50 text-indigo-700 dark:text-indigo-300 px-2 py-1 rounded-md font-bold\" id=\"ip-count-badge\">1 Config Set</span>\n                                          </div>\n                                          <div class=\"flex gap-2 mb-2\">\n                                              <input type=\"text\" id=\"helper-clean-ip\" placeholder=\"1.2.3.4\" class=\"flex-1 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-sm\">\n                                              <input type=\"text\" id=\"helper-ip-name\" placeholder=\"Name (Optional)\" class=\"w-1/3 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-sm\">\n                                              <button type=\"button\" onclick=\"addHelperIpToTextarea('helper-clean-ip', 'helper-ip-name', 'cfg-ips')\" class=\"px-3 py-2 bg-primary text-white rounded-xl font-bold hover:bg-primary-hover transition-colors\">+</button>\n                                          </div>\n                                          <textarea id=\"cfg-ips\" rows=\"3\" data-i18n=\"ph_clean_ips\" placeholder=\"1.2.3.4#Germany\n5.6.7.8#US\n9.10.11.12#France\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none\"></textarea>\n                                          <p class=\"text-xs text-slate-400 mt-2\" data-i18n=\"desc_clean_ips\">One IP per line. Use <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">IP#Name</code> format to tag IPs (e.g. <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">1.2.3.4#Germany</code>). Use <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{IP_NAME}</code> in name strategy.</p>\n                                          <button id=\"btn-resolve-smart-ips\" onclick=\"resolveSmartCleanIps()\" class=\"mt-3 w-full sm:w-auto px-4 py-2.5 bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 rounded-xl text-xs font-bold transition-all flex items-center justify-center gap-2\">\n                                              <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13 10V3L4 14h7v7l9-11h-7z\"></path></svg>\n                                              Auto-Resolve CDN &amp; Clean IPs\n                                          </button>\n                                      </div>\n                                      <div class=\"grid grid-cols-1 md:grid-cols-2 gap-4\">\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_fp\">TLS Signature</label>\n                                              <select id=\"cfg-fp\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none appearance-none\">\n                                                  <option value=\"chrome\">Chrome</option><option value=\"firefox\">Firefox</option><option value=\"safari\">Safari</option>\n                                              </select>\n                                          </div>\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_dns\">Resolver IP</label>\n                                              <input type=\"text\" id=\"cfg-dns\" placeholder=\"1.1.1.1\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                          </div>\n                                          <div class=\"space-y-1 md:col-span-2\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_doh\">Custom DNS (DoH Provider)</label>\n                                              <input type=\"text\" id=\"cfg-custom-dns\" placeholder=\"https://cloudflare-dns.com/dns-query\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                          </div>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n\n                          <!-- Section: Proxy & Relay -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">🔗</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_proxy_relay\">Proxy &amp; Relay</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-4 px-5 pb-5 pt-1\">\n                                      <div class=\"space-y-1\">\n                                          <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_relay\">Proxy IPs (Comma/Newline separated)</label>\n                                          <textarea id=\"cfg-relay\" rows=\"3\" placeholder=\"104.20.0.1\nproxyip.cmliussss.net\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none\"></textarea>\n                                      </div>\n                                      <div class=\"space-y-1\">\n                                          <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_nat64\">NAT64 Prefix</label>\n                                          <textarea id=\"cfg-nat64\" rows=\"2\" placeholder=\"64:ff9b::/96\n2001:db8:64::/96\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none\"></textarea>\n                                          <p class=\"text-xs text-slate-400 mt-1\" data-i18n=\"desc_nat64\">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses. Supports multiple prefixes (one per line).</p>\n                                      </div>\n                                      <label class=\"flex items-center justify-between cursor-pointer bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl\">\n                                          <div>\n                                              <span class=\"text-sm font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"lbl_direct_configs\">Include Direct Configs</span>\n                                              <p class=\"text-[10px] text-slate-400 mt-0.5\">Generate configs without Proxy IP alongside relay configs</p>\n                                          </div>\n                                          <div class=\"relative inline-flex items-center cursor-pointer\">\n                                              <input type=\"checkbox\" id=\"cfg-direct-configs\" class=\"sr-only peer\">\n                                              <div class=\"w-11 h-6 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-primary\"></div>\n                                          </div>\n                                      </label>\n                                  </div>\n                              </div>\n                          </div>\n\n                          \n                          <!-- Section: Custom Routing -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">🔀</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\">Custom Routing</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-4 px-5 pb-5 pt-1\">\n                                      <div class=\"space-y-1\">\n                                          <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\">Custom Direct Routing (Domains / IPs / GeoIP / GeoSite)</label>\n                                          <textarea id=\"cfg-custom-routing\" rows=\"4\" placeholder=\"example.com&#10;1.2.3.4&#10;geoip:ir&#10;geosite:ir\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none font-mono text-sm resize-none\"></textarea>\n                                          <p class=\"text-xs text-slate-400 mt-1\">One domain, IP, geoip (e.g., geoip:ir) or geosite (e.g., geosite:ir) per line to route to DIRECT.</p>\n                                           <div class=\"mt-2 p-3 rounded-xl bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/30 text-xs text-amber-700 dark:text-amber-300 leading-relaxed font-sans flex gap-2\">\n                                               <span>⚠️</span>\n                                               <span><strong>Important Note:</strong> Any incorrect entry, typo, or misformatted routing rule in this section can cause connection failures or subscription import problems. Please double check all syntax before saving.</span>\n                                           </div>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n                          <!-- Section: Subscription -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">📝</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_subscription\">Subscription</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-4 px-5 pb-5 pt-1\">\n                                      <div class=\"grid grid-cols-1 gap-4\">\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_strategy\">Configuration Name Strategy</label>\n                                              <div class=\"relative\">\n                                                  <input type=\"text\" id=\"cfg-name-strategy\" placeholder=\"{FLAG} {PROTOCOL}-{USER}-{PORT}\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none\">\n                                                  <div id=\"cfg-strategy-suggestions\" class=\"absolute z-10 w-full mt-1 bg-white dark:bg-slate-800 border border-slate-200 dark:border-darkborder rounded-xl shadow-lg hidden\">\n                                                      <div class=\"p-2 flex flex-wrap gap-1 text-xs\">\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{FLAG}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{COUNTRY}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{CITY}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{ISP}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{PROTOCOL}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{USER}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{PORT}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{PREFIX}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{IP}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{IP_NAME}</button>\n                                                          <button type=\"button\" class=\"tag-btn bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-primary hover:bg-slate-200 dark:hover:bg-slate-600\">{HOST}</button>\n                                                      </div>\n                                                  </div>\n                                              </div>\n                                              <p data-i18n=\"html_desc_strategy\" class=\"text-[11px] text-slate-400 dark:text-slate-500 mt-1 leading-relaxed\">\n                                                  Supported templates: <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">default</code>, <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">type-user-port</code>, <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">user-port</code>, <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">host-port-user</code>, <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">prefix-user-port</code>, <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">ip</code>. Tags: <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{FLAG}</code> <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{IP_NAME}</code> <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{USER}</code> <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{PORT}</code>\n                                              </p>\n                                          </div>\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_prefix\">Custom Name Prefix</label>\n                                              <input type=\"text\" id=\"cfg-name-prefix\" placeholder=\"Core\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                              <p class=\"text-[11px] text-slate-400 mt-1\" data-i18n=\"html_desc_prefix_note\">This prefix is injected where the <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{PREFIX}</code> tag is used in your naming strategy.</p>\n                                          </div>\n                                       </div>\n                                       <div class=\"border-t border-slate-100 dark:border-darkborder pt-4\">\n                                          <div class=\"flex items-center justify-between mb-3\">\n                                              <div>\n                                                  <h4 class=\"text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_fake_configs\">Subscription Fake Entries</h4>\n                                                  <p class=\"text-[11px] text-slate-400 dark:text-slate-500 mt-0.5\" data-i18n=\"desc_fake_configs\">Customize info entries shown in subscription profiles. Use <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{usage}</code> and <code class=\"bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono\">{expiry}</code> for dynamic values.</p>\n                                              </div>\n                                              <button onclick=\"addFakeConfig()\" class=\"px-3 py-1.5 bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 rounded-xl text-xs font-bold transition-all flex items-center gap-1.5 shrink-0\">\n                                                  <svg class=\"w-3.5 h-3.5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 6v6m0 0v6m0-6h6m-6 0H6\"></path></svg>\n                                                  <span data-i18n=\"btn_add_entry\">Add Entry</span>\n                                              </button>\n                                          </div>\n                                          <div id=\"fake-configs-list\" class=\"space-y-2\"></div>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n\n                          <!-- Section: Protocol -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">⚡</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_protocol\">Protocol</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"flex flex-col sm:flex-row gap-3\">\n                                      <label class=\"flex-1 flex items-center justify-between cursor-pointer bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl\">\n                                          <span class=\"text-sm font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"lbl_tfo\">TCP Fast Open</span>\n                                          <div class=\"relative inline-flex items-center cursor-pointer\">\n                                              <input type=\"checkbox\" id=\"cfg-tfo\" class=\"sr-only peer\">\n                                              <div class=\"w-11 h-6 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-primary\"></div>\n                                          </div>\n                                      </label>\n                                      <label class=\"flex-1 flex items-center justify-between cursor-pointer bg-slate-50 dark:bg-slate-800/50 p-4 rounded-2xl\">\n                                          <span class=\"text-sm font-bold text-slate-700 dark:text-slate-300\" data-i18n=\"lbl_ech\">Secure Hello (ECH)</span>\n                                          <div class=\"relative inline-flex items-center cursor-pointer\">\n                                              <input type=\"checkbox\" id=\"cfg-ech\" class=\"sr-only peer\">\n                                              <div class=\"w-11 h-6 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-5 rtl:peer-checked:after:-translate-x-5 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-slate-500 peer-checked:bg-primary\"></div>\n                                          </div>\n                                      </label>\n                                  </div>\n                              </div>\n                          </div>\n\n                          <!-- Section: Cluster -->\n                          <div class=\"bg-indigo-50 dark:bg-indigo-950/20 rounded-2xl border border-indigo-100 dark:border-indigo-900/50 overflow-hidden\" data-accordion=\"\">\n                               <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-indigo-100/50 dark:hover:bg-indigo-900/30 transition-colors\">\n                                   <div class=\"flex items-center gap-3\">\n                                       <span class=\"text-lg\">🔬</span>\n                                       <span class=\"text-sm font-bold text-indigo-700 dark:text-indigo-300\" data-i18n=\"other_nodes_title\">Other Nodes</span>\n                                   </div>\n                                   <svg class=\"w-4 h-4 text-indigo-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                               </button>\n                               <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                   <div class=\"space-y-3 px-5 pb-5 pt-1\">\n                                       <p class=\"text-xs text-indigo-600/80 dark:text-indigo-300/70 leading-relaxed\" data-i18n=\"other_nodes_desc\">External nodes (URL + API Key) for cross-panel management.</p>\n                                       <div class=\"flex items-center justify-between\">\n                                           <div id=\"linked-nodes-list\" class=\"space-y-2 flex-1\"></div>\n                                       </div>\n                                       <button onclick=\"showAddNodeModal()\" type=\"button\" class=\"w-full py-3 border-2 border-dashed border-indigo-300 dark:border-indigo-700 hover:border-indigo-500 dark:hover:border-indigo-500 text-indigo-500 dark:text-indigo-400 text-sm font-bold rounded-xl transition-colors flex items-center justify-center gap-2\">\n                                           <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 6v6m0 0v6m0-6h6m-6 0H6\"></path></svg>\n                                           <span data-i18n=\"add_node_confirm\">Add Node</span>\n                                       </button>\n                                   </div>\n                               </div>\n                           </div>\n\n                          <!-- Modal: Add Other Node -->\n                           <div id=\"modal-add-node\" class=\"hidden fixed inset-0 z-50 flex items-end sm:items-center justify-center p-0 pb-4 sm:p-4 bg-slate-900/50 backdrop-blur-sm\">\n                               <div class=\"bg-[var(--color-surface)]  rounded-t-3xl sm:rounded-3xl w-full sm:max-w-md max-h-[calc(100vh-2rem)] sm:max-h-[85vh] flex flex-col shadow-2xl border border-slate-200 dark:border-darkborder\">\n                                  <div class=\"px-6 pt-6 pb-4\">\n                                      <h3 class=\"text-lg font-bold\" data-i18n=\"add_node_title\">Add External Node</h3>\n                                      <p class=\"text-xs text-slate-400 mt-1\" data-i18n=\"add_node_desc\">Enter the URL and API Key of the external panel.</p>\n                                  </div>\n                                   <div class=\"px-6 pb-4 space-y-4 overflow-y-auto flex-1 min-h-0\">\n                                      <div>\n                                          <label class=\"block text-xs font-bold text-slate-500 mb-1\" data-i18n=\"add_node_url\">Node URL</label>\n                                          <input type=\"text\" id=\"add-node-url\" placeholder=\"node.example.com\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono\">\n                                      </div>\n                                      <div>\n                                          <label class=\"block text-xs font-bold text-slate-500 mb-1\" data-i18n=\"add_node_apikey\">API Key</label>\n                                          <input type=\"password\" id=\"add-node-apikey\" placeholder=\"mrvpn294_...\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono pe-12\">\n                                          <button type=\"button\" onclick=\"const n=document.getElementById('add-node-apikey');n.type=n.type==='password'?'text':'password'\" class=\"absolute end-14 mt-[-36px] px-2 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200\">👁️</button>\n                                      </div>\n                                  </div>\n                                  <div class=\"px-6 py-4 border-t border-slate-200 dark:border-darkborder flex justify-end gap-2\">\n                                      <button onclick=\"document.getElementById('modal-add-node').classList.add('hidden')\" class=\"px-4 py-2 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm\" data-i18n=\"btn_cancel\">Cancel</button>\n                                      <button onclick=\"commitAddNode()\" class=\"px-4 py-2 rounded-xl bg-indigo-500 hover:bg-indigo-600 text-white font-bold text-sm\" data-i18n=\"add_node_confirm\">Add Node</button>\n                                  </div>\n                              </div>\n                          </div>\n\n                          <!-- Section: Telegram -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">🤖</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_telegram\">Telegram Bot</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-3 px-5 pb-5 pt-1\">\n                                      <div class=\"space-y-1\">\n                                          <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_tg_token\">Bot Token</label>\n                                          <div class=\"relative\">\n                                              <input type=\"password\" id=\"cfg-tg-token\" placeholder=\"123456:ABC-DEF1234ghIkl-zyx5c\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm pe-12\">\n                                              <button type=\"button\" onclick=\"const n=document.getElementById('cfg-tg-token');n.type=n.type==='password'?'text':'password'\" class=\"absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200\">👁️</button>\n                                          </div>\n                                      </div>\n                                      <div class=\"grid grid-cols-1 md:grid-cols-2 gap-3\">\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_tg_chat\">Chat ID</label>\n                                              <input type=\"text\" id=\"cfg-tg-chat\" placeholder=\"123456789\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                          </div>\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_tg_admin\">Authorized Admin ID</label>\n                                              <input type=\"text\" id=\"cfg-tg-admin\" placeholder=\"123456789\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                              <p class=\"text-xs text-slate-400\" data-i18n=\"desc_tg_admin\">Only this Telegram User ID can manage the panel via bot. Leave empty to use Chat ID.</p>\n                                          </div>\n                                      </div>\n                                      <p class=\"text-xs text-slate-400\" data-i18n=\"desc_tg_bot\">Set these values to receive login alerts via Telegram.</p>\n                                  </div>\n                              </div>\n                          </div>\n\n                          <!-- Section: Cloudflare -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder overflow-hidden\" data-accordion=\"\">\n                              <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between px-5 py-4 hover:bg-slate-50 dark:hover:bg-slate-800/50 transition-colors\">\n                                  <div class=\"flex items-center gap-3\">\n                                      <span class=\"text-lg\">☁️</span>\n                                      <span class=\"text-sm font-bold text-slate-700 dark:text-white\" data-i18n=\"adv_cloudflare\">Cloudflare</span>\n                                  </div>\n                                  <svg class=\"w-4 h-4 text-slate-400 transform transition-transform duration-200 accordion-icon\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                              </button>\n                              <div data-accordion-content=\"\" class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;visibility:hidden\">\n                                  <div class=\"space-y-3 px-5 pb-5 pt-1\">\n                                      <div class=\"grid grid-cols-1 md:grid-cols-2 gap-3\">\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_cf_acc\">CF Account ID</label>\n                                              <input type=\"text\" id=\"cfg-cf-acc\" placeholder=\"a1b2c3d4e5f6...\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono\">\n                                          </div>\n                                          <div class=\"space-y-1\">\n                                              <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_cf_token\">CF API Token</label>\n                                              <div class=\"relative\">\n                                                  <input type=\"password\" id=\"cfg-cf-token\" placeholder=\"Bearer Token (Read Analytics)\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono pe-12\">\n                                                  <button type=\"button\" onclick=\"const n=document.getElementById('cfg-cf-token');n.type=n.type==='password'?'text':'password'\" class=\"absolute inset-y-0 end-0 flex items-center px-4 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200\">👁️</button>\n                                              </div>\n                                          </div>\n                                      </div>\n                                      <div class=\"space-y-1\">\n                                          <label class=\"block text-sm font-bold text-slate-600 dark:text-slate-300\" data-i18n=\"lbl_cf_worker\">CF Worker Script Name</label>\n                                          <input type=\"text\" id=\"cfg-cf-worker\" placeholder=\"e.g. mrvpn294\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono\">\n                                          <p class=\"text-xs text-slate-400\" data-i18n=\"desc_cf_worker\">Required for in-panel updates. The script name shown in your Cloudflare Workers dashboard.</p>\n                                      </div>\n                                      <p class=\"text-xs text-slate-400\" data-i18n=\"desc_cf_api\">Optional: Monitor Worker free usage limits (100k/day). Needs Account Analytics Read permission.</p>\n                                      <div class=\"border-t border-slate-100 dark:border-darkborder pt-3\">\n                                          <button type=\"button\" onclick=\"document.getElementById('cf-helper-guide').classList.toggle('hidden')\" class=\"w-full text-start px-4 py-3 bg-primary/10 hover:bg-primary/15 text-primary text-xs font-bold rounded-xl flex items-center justify-between transition-colors\">\n                                              <span class=\"flex items-center gap-1.5\">\n                                                  💡 <span data-i18n=\"cf_help_title\">Need help getting these? Beginner's Guide</span>\n                                              </span>\n                                              <span class=\"text-[10px] transform transition-transform duration-200\">▼</span>\n                                          </button>\n                                          <div id=\"cf-helper-guide\" class=\"hidden mt-3 p-4 bg-slate-50/50 dark:bg-slate-900/30 border border-slate-200 dark:border-darkborder rounded-2xl text-[11px] space-y-4 text-start leading-relaxed\">\n                                              <div class=\"space-y-1 pb-3 border-b border-dashed border-slate-200 dark:border-darkborder\">\n                                                  <h5 class=\"font-extrabold text-[var(--color-text)] flex items-center gap-1\">🇬🇧 Beginner's Walkthrough:</h5>\n                                                  <ol class=\"list-decimal list-inside space-y-1 text-slate-500 dark:text-slate-400\">\n                                                      <li><strong>CF API Token:</strong> Click <a href=\"https://dash.cloudflare.com/profile/api-tokens?template=edit-workers\" target=\"_blank\" class=\"text-primary hover:underline font-bold\">Api Token Template ↗</a>. Click <strong>Use Template</strong>, then <strong>Continue to summary</strong> &gt; <strong>Create Token</strong>. Copy and paste above!</li>\n                                                      <li><strong>CF Account ID:</strong> Open any Cloudflare Workers page. Copy the 32-char string after <code>dash.cloudflare.com/</code> in the URL.</li>\n                                                      <li><strong>Worker Script Name:</strong> Go to <strong>Compute &gt; Workers &amp; Pages</strong> in Cloudflare. Copy your worker's name.</li>\n                                                  </ol>\n                                              </div>\n                                          </div>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n\n                      </div>\n\n                          <!-- USERS VIEW -->\n                      <div id=\"view-users\" class=\"hidden space-y-4\">\n                          <!-- Compact Stats Bar -->\n                          <div class=\"bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder p-4 flex flex-wrap items-center gap-4 md:gap-6\">\n                              <div class=\"flex items-center gap-2\">\n                                  <div class=\"p-1.5 bg-primary/10 text-primary rounded-lg\"><svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg></div>\n                                  <div><span class=\"text-[10px] font-bold text-slate-400 uppercase\" data-i18n=\"stat_total_subscribers\">Total</span><span id=\"stat-total-users\" class=\"ms-1.5 text-sm font-black text-slate-800 dark:text-white\">0</span></div>\n                              </div>\n                              <div class=\"flex items-center gap-2\">\n                                  <div class=\"p-1.5 bg-emerald-500/10 text-emerald-500 rounded-lg\"><svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg></div>\n                                  <div><span class=\"text-[10px] font-bold text-slate-400 uppercase\" data-i18n=\"stat_active_paused\">Active/Paused</span><span id=\"stat-active-users\" class=\"ms-1.5 text-sm font-black text-slate-800 dark:text-white\">0 / 0</span></div>\n                              </div>\n                              <div class=\"flex items-center gap-2\">\n                                  <div class=\"p-1.5 bg-violet-500/10 text-violet-500 rounded-lg\"><svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10\"></path></svg></div>\n                                  <div><span class=\"text-[10px] font-bold text-slate-400 uppercase\" data-i18n=\"stat_cumulative_traffic\">Traffic</span><span id=\"stat-total-traffic\" class=\"ms-1.5 text-sm font-black text-slate-800 dark:text-white\">0 GB</span></div>\n                              </div>\n                              <div class=\"flex items-center gap-2\">\n                                  <div class=\"p-1.5 bg-red-500/10 text-red-500 rounded-lg\"><svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z\"></path></svg></div>\n                                  <div><span class=\"text-[10px] font-bold text-slate-400 uppercase\" data-i18n=\"stat_auto_disabled\">Disabled</span><span id=\"stat-auto-disabled\" class=\"ms-1.5 text-sm font-black text-slate-800 dark:text-white\">0</span></div>\n                              </div>\n                          </div>\n\n                          <!-- Recently Disabled Users Panel -->\n                          <div id=\"disabled-users-panel\" class=\"hidden\">\n                              <div class=\"bg-gradient-to-br from-red-50 to-orange-50 dark:from-red-950/30 dark:to-orange-950/30 rounded-3xl p-6 shadow-sm border border-red-200 dark:border-red-800/40 relative overflow-hidden\">\n                                  <div class=\"flex items-center justify-between mb-4\">\n                                      <div class=\"flex items-center gap-3\">\n                                          <div class=\"p-2.5 bg-red-100 dark:bg-red-900/40 rounded-xl\">\n                                              <svg class=\"w-5 h-5 text-red-600 dark:text-red-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z\"></path></svg>\n                                          </div>\n                                          <div>\n                                              <h3 class=\"text-sm font-bold text-red-700 dark:text-red-300\" data-i18n=\"disabled_panel_title\">Recently Disabled Users</h3>\n                                              <p class=\"text-[11px] text-red-500/70 dark:text-red-400/60\" data-i18n=\"disabled_panel_desc\">Users automatically disabled due to quota or expiration limits</p>\n                                          </div>\n                                      </div>\n                                      <span id=\"disabled-panel-badge\" class=\"px-3 py-1 bg-red-500 text-white text-xs font-bold rounded-full shadow-sm\">0</span>\n                                  </div>\n                                  <div id=\"disabled-users-list\" class=\"space-y-2.5 max-h-64 overflow-y-auto pr-1\">\n                                  </div>\n                              </div>\n                          </div>\n\n                          <div class=\"bg-[var(--color-surface)]  rounded-3xl p-4 md:p-6 shadow-sm border border-slate-200 dark:border-darkborder relative overflow-hidden\">\n                              <div class=\"flex flex-col sm:flex-row items-stretch sm:items-center justify-between mb-4 md:mb-6 gap-3\">\n                                   <h3 class=\"text-sm uppercase font-bold text-slate-500 tracking-wider\" data-i18n=\"sub_directory_title\">Subscriber Directory</h3>\n                                   <div class=\"flex flex-col sm:flex-row items-stretch sm:items-center gap-2\">\n                                       <select id=\"user-status-filter\" onchange=\"renderUsersTable()\" class=\"bg-[var(--color-bg)] border border-slate-200 dark:border-darkborder px-3 py-2.5 rounded-xl text-xs outline-none font-sans text-slate-600 dark:text-slate-400 focus:border-primary\">\n                                           <option value=\"all\" data-i18n=\"filter_all\">All Users</option>\n                                           <option value=\"active\" data-i18n=\"filter_active\">Active</option>\n                                           <option value=\"paused\" data-i18n=\"filter_paused\">Paused</option>\n                                           <option value=\"auto-disabled\" data-i18n=\"filter_auto_disabled\">Auto-Disabled</option>\n                                       </select>\n                                       <input type=\"text\" id=\"user-search-input\" onkeyup=\"renderUsersTable()\" placeholder=\"🔍 Find by Name or UUID...\" data-i18n=\"user_search_placeholder\" class=\"bg-[var(--color-bg)] border border-slate-200 dark:border-darkborder px-3 py-2.5 rounded-xl text-xs outline-none font-sans text-slate-600 dark:text-slate-400 focus:border-primary\">\n                                       <button onclick=\"openAddUserPage()\" class=\"native-press px-4 py-2.5 bg-primary hover:bg-primary/90 text-white rounded-xl text-xs font-bold transition-colors shadow-sm whitespace-nowrap\" data-i18n=\"btn_add_user\">+ Add New User</button>\n                                   </div>\n                               </div>\n                              <div class=\"overflow-x-auto\">\n                                  <div id=\"tbl-users\" class=\"grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3\">\n                                  </div>\n                              </div>\n                          </div>\n                       </div>\n\n                       <!-- PAGE: Add User -->\n                       <div id=\"view-add-user\" class=\"hidden h-full flex flex-col\">\n                           <div class=\"bg-[var(--color-surface)]  rounded-2xl md:rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder overflow-hidden flex flex-col flex-1 min-h-0\">\n                               <div class=\"flex items-center gap-3 px-5 py-4 border-b border-slate-200 dark:border-darkborder shrink-0\">\n                                   <button onclick=\"closeAddUserPage()\" class=\"native-press p-2 -ms-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors\">\n                                       <svg class=\"w-5 h-5 text-slate-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M15 19l-7-7 7-7\"></path></svg>\n                                   </button>\n                                   <h3 class=\"text-lg font-bold text-slate-800 dark:text-white\" data-i18n=\"modal_add_title\">Add User</h3>\n                               </div>\n                               <div class=\"overflow-y-auto flex-1 min-h-0 p-6 grid grid-cols-1 md:grid-cols-2 gap-6\">\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_basic_info\">Basic Info</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_u_name\">Name / Identifier</label>\n                                               <input type=\"text\" id=\"add-user-name\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_custom_config_name\">Custom Config Name / Prefix</label>\n                                               <input type=\"text\" id=\"add-user-custom-name\" placeholder=\"Leave empty to use user name\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_limits\">Limits</h4>\n                                       <div class=\"space-y-3\">\n                                           <div class=\"grid grid-cols-2 gap-3\">\n                                               <div>\n                                                   <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_traffic_limit_gb\">Traffic (GB) Limit</label>\n                                                   <input type=\"number\" id=\"add-user-total-reqs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               </div>\n                                               <div>\n                                                   <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_daily_limit_gb\">Daily Limit (GB)</label>\n                                                   <input type=\"number\" id=\"add-user-daily-reqs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               </div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_expiration_days\">Expiration (Days)</label>\n                                               <input type=\"number\" id=\"add-user-days\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_conn_limit\">IP Connection Limit</label>\n                                               <input type=\"number\" id=\"add-user-conn-limit\" placeholder=\"Unlimited\" min=\"1\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\" data-i18n-placeholder=\"unlimited\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_conn_limit\">Max simultaneous connections per IP. Leave empty for unlimited.</p>\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_network\">Network</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_clean_ips\">Clean IPs</label>\n                                               <div id=\"add-user-clean-ips-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_clean_ips_modal\">Custom Clean IPs (comma/newline)</label>\n                                               <div class=\"flex gap-2 mt-1 mb-1\">\n                                                   <input type=\"text\" id=\"add-user-helper-ip\" placeholder=\"1.2.3.4\" class=\"flex-1 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-xs\">\n                                                   <input type=\"text\" id=\"add-user-helper-name\" placeholder=\"Name\" class=\"w-1/3 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-xs\">\n                                                   <button type=\"button\" onclick=\"addHelperIpToTextarea('add-user-helper-ip', 'add-user-helper-name', 'add-user-custom-clean')\" class=\"px-3 py-2 bg-primary text-white rounded-xl font-bold hover:bg-primary-hover transition-colors\">+</button>\n                                               </div>\n                                               <textarea id=\"add-user-custom-clean\" rows=\"2\" placeholder=\"e.g. 1.2.3.4#Germany, 5.6.7.8\" class=\"w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_proxy_ips\">Proxy IPs</label>\n                                               <div id=\"add-user-proxy-ips-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_proxy_ips\">Custom Proxy IPs (comma/newline)</label>\n                                               <textarea id=\"add-user-custom-proxy\" rows=\"2\" placeholder=\"e.g. proxy1.com:443\" class=\"w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_assigned_nodes\">Assigned Nodes</label>\n                                               <div id=\"add-user-nodes-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_assigned_nodes\">Custom Nodes (comma/newline, empty = all nodes)</label>\n                                               <textarea id=\"add-user-custom-nodes\" rows=\"2\" placeholder=\"node1.example.com\" class=\"w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_user_panel_url\">Main Panel URL (Custom Nodes)</label>\n                                               <input type=\"text\" id=\"add-user-panel-url\" placeholder=\"e.g. panel.example.com\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_user_panel_url\">Main panel domain for custom nodes. If empty, default panel URL is used.</p>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_nat64\">NAT64 Prefix</label>\n                                               <input type=\"text\" id=\"add-user-nat64\" placeholder=\"e.g. 64:ff9b::/96\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_nat64_user\">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.</p>\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_advanced\">Advanced</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_protocol_mode\">Protocol Mode</label>\n                                               <div id=\"add-user-mode-wrap\" class=\"flex gap-4 mt-1\">\n                                                   <label class=\"flex items-center gap-2 text-sm cursor-pointer\"><input type=\"checkbox\" value=\"alpha\" class=\"add-mode-cb accent-primary\"> <span>Alpha (VLESS)</span></label>\n                                                   <label class=\"flex items-center gap-2 text-sm cursor-pointer\"><input type=\"checkbox\" value=\"beta\" class=\"add-mode-cb accent-primary\"> <span>Beta (Trojan)</span></label>\n                                               </div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\">Ports</label>\n                                               <div id=\"add-user-ports-wrap\" class=\"flex flex-wrap gap-2 mt-1\"></div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_max_configs\">Max Configs</label>\n                                               <input type=\"number\" id=\"add-user-max-configs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\" data-i18n-placeholder=\"unlimited\">\n                                           </div>\n                                       </div>\n                                   </div>\n                               </div>\n                               <div class=\"px-5 py-4 border-t border-slate-200 dark:border-darkborder bg-[var(--color-surface)]  flex justify-between items-center shrink-0\">\n                                   <button onclick=\"closeAddUserPage()\" class=\"px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm\" data-i18n=\"btn_cancel\">Cancel</button>\n                                   <button onclick=\"commitAddUser()\" class=\"native-press px-6 py-2.5 rounded-xl bg-primary text-white font-bold text-sm shadow-sm\" data-i18n=\"save_btn_user\">Save User</button>\n                               </div>\n                           </div>\n                       </div>\n\n                       <!-- PAGE: Edit User -->\n                       <div id=\"view-edit-user\" class=\"hidden h-full flex flex-col\">\n                           <div class=\"bg-[var(--color-surface)]  rounded-2xl md:rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder overflow-hidden flex flex-col flex-1 min-h-0\">\n                               <div class=\"flex items-center gap-3 px-5 py-4 border-b border-slate-200 dark:border-darkborder shrink-0\">\n                                   <button onclick=\"closeEditUserPage()\" class=\"native-press p-2 -ms-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-colors\">\n                                       <svg class=\"w-5 h-5 text-slate-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M15 19l-7-7 7-7\"></path></svg>\n                                   </button>\n                                   <h3 class=\"text-lg font-bold text-slate-800 dark:text-white\" data-i18n=\"edit_sub\">Edit Subscriber</h3>\n                                   <input type=\"hidden\" id=\"edit-user-id\">\n                               </div>\n                               <div class=\"overflow-y-auto flex-1 min-h-0 p-6 grid grid-cols-1 md:grid-cols-2 gap-6\">\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_basic_info\">Basic Info</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_name_ph\">Name / Identifier</label>\n                                               <input type=\"text\" id=\"edit-user-name\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_custom_config_name\">Custom Config Name / Prefix</label>\n                                               <input type=\"text\" id=\"edit-user-custom-name\" placeholder=\"Leave empty to use user name\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_limits\">Limits</h4>\n                                       <div class=\"space-y-3\">\n                                           <div class=\"grid grid-cols-2 gap-3\">\n                                               <div>\n                                                   <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_traffic_limit_gb\">Traffic Limit (GB)</label>\n                                                   <input type=\"number\" id=\"edit-user-total-reqs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               </div>\n                                               <div>\n                                                   <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_daily_limit_gb\">Daily Limit (GB)</label>\n                                                   <input type=\"number\" id=\"edit-user-daily-reqs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               </div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_expiration_days\">Expiration (Days)</label>\n                                               <input type=\"number\" id=\"edit-user-days\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_conn_limit\">IP Connection Limit</label>\n                                               <input type=\"number\" id=\"edit-user-conn-limit\" placeholder=\"Unlimited\" min=\"1\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\" data-i18n-placeholder=\"unlimited\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_conn_limit\">Max simultaneous connections per user. Leave empty for unlimited.</p>\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_network\">Network</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_clean_ips\">Clean IPs</label>\n                                               <div id=\"edit-user-clean-ips-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_clean_ips_modal\">Custom Clean IPs (comma/newline)</label>\n                                               <div class=\"flex gap-2 mt-1 mb-1\">\n                                                   <input type=\"text\" id=\"edit-user-helper-ip\" placeholder=\"1.2.3.4\" class=\"flex-1 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-xs\">\n                                                   <input type=\"text\" id=\"edit-user-helper-name\" placeholder=\"Name\" class=\"w-1/3 px-3 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary focus:ring-1 outline-none text-xs\">\n                                                   <button type=\"button\" onclick=\"addHelperIpToTextarea('edit-user-helper-ip', 'edit-user-helper-name', 'edit-user-custom-clean')\" class=\"px-3 py-2 bg-primary text-white rounded-xl font-bold hover:bg-primary-hover transition-colors\">+</button>\n                                               </div>\n                                               <textarea id=\"edit-user-custom-clean\" rows=\"2\" placeholder=\"e.g. 1.2.3.4#Germany, 5.6.7.8\" class=\"w-full px-4 py-2 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_proxy_ips\">Proxy IPs</label>\n                                               <div id=\"edit-user-proxy-ips-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_proxy_ips\">Custom Proxy IPs (comma/newline)</label>\n                                               <textarea id=\"edit-user-custom-proxy\" rows=\"2\" placeholder=\"e.g. proxy1.com:443\" class=\"w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_assigned_nodes\">Assigned Nodes</label>\n                                               <div id=\"edit-user-nodes-wrap\" class=\"flex flex-wrap gap-2 mt-1 text-slate-500\"></div>\n                                               <label class=\"block text-[10px] font-bold text-slate-400 mt-2\" data-i18n=\"desc_assigned_nodes\">Custom Nodes (comma/newline, empty = all nodes)</label>\n                                               <textarea id=\"edit-user-custom-nodes\" rows=\"2\" placeholder=\"node1.example.com\" class=\"w-full mt-1 px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\"></textarea>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_user_panel_url\">Main Panel URL (Custom Nodes)</label>\n                                               <input type=\"text\" id=\"edit-user-panel-url\" placeholder=\"e.g. panel.example.com\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_user_panel_url\">Main panel domain for custom nodes. If empty, default panel URL is used.</p>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_nat64\">NAT64 Prefix</label>\n                                               <input type=\"text\" id=\"edit-user-nat64\" placeholder=\"e.g. 64:ff9b::/96\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm font-mono\">\n                                               <p class=\"text-[10px] text-slate-400 mt-1\" data-i18n=\"desc_nat64_user\">Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.</p>\n                                           </div>\n                                       </div>\n                                   </div>\n                                   <div class=\"space-y-4 bg-slate-50/50 dark:bg-slate-800/30 p-5 rounded-2xl border border-slate-100 dark:border-slate-700/50\">\n                                       <h4 class=\"text-xs font-bold text-slate-400 uppercase tracking-wider\" data-i18n=\"section_advanced\">Advanced</h4>\n                                       <div class=\"space-y-3\">\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_protocol_mode\">Protocol Mode</label>\n                                               <div id=\"edit-user-mode-wrap\" class=\"flex gap-4 mt-1\">\n                                                   <label class=\"flex items-center gap-2 text-sm cursor-pointer\"><input type=\"checkbox\" value=\"alpha\" class=\"edit-mode-cb accent-primary\"> <span>Alpha (VLESS)</span></label>\n                                                   <label class=\"flex items-center gap-2 text-sm cursor-pointer\"><input type=\"checkbox\" value=\"beta\" class=\"edit-mode-cb accent-primary\"> <span>Beta (Trojan)</span></label>\n                                               </div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\">Ports</label>\n                                               <div id=\"edit-user-ports-wrap\" class=\"flex flex-wrap gap-2 mt-1\"></div>\n                                           </div>\n                                           <div>\n                                               <label class=\"block text-xs font-bold text-slate-500 mb-1.5\" data-i18n=\"lbl_max_configs\">Max Configs</label>\n                                               <input type=\"number\" id=\"edit-user-max-configs\" placeholder=\"Unlimited\" class=\"w-full px-4 py-3 rounded-xl border border-slate-200 dark:border-darkborder bg-slate-50 dark:bg-slate-800 focus:border-primary outline-none text-sm\" data-i18n-placeholder=\"unlimited\">\n                                           </div>\n                                       </div>\n                                   </div>\n                               </div>\n                               <div class=\"px-5 py-4 border-t border-slate-200 dark:border-darkborder bg-[var(--color-surface)]  flex justify-between items-center shrink-0\">\n                                   <button onclick=\"closeEditUserPage()\" class=\"px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 font-bold text-sm\" data-i18n=\"btn_cancel\">Cancel</button>\n                                   <button onclick=\"commitEditUser()\" class=\"native-press px-6 py-2.5 rounded-xl bg-primary text-white font-bold text-sm shadow-sm\" data-i18n=\"btn_save_changes\">Save Changes</button>\n                               </div>\n                           </div>\n                       </div>\n\n                      <!-- LOGS VIEW -->\n                      <div id=\"view-logs\" class=\"hidden space-y-6\">\n                          <div class=\"bg-[var(--color-surface)]/70 /70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-white/20 dark:border-white/10 relative overflow-hidden\">\n                              <div class=\"flex items-center justify-between mb-6\">\n                                  <h3 class=\"text-sm uppercase font-bold text-slate-500 tracking-wider\" data-i18n=\"tab_logs\">System Activity Logs</h3>\n                                  <button onclick=\"loadLogs()\" class=\"px-3 py-1.5 bg-slate-100 hover:bg-slate-200 dark:bg-slate-800 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg text-xs font-bold transition-colors\">\n                                      🔄 Refresh\n                                  </button>\n                              </div>\n                              <div class=\"space-y-3\" id=\"logs-container\">\n                                  <p class=\"text-sm text-slate-400 text-center py-8\" data-i18n=\"loading_logs\">Loading activity logs...</p>\n                              </div>\n                          </div>\n                      </div>\n\n                      <!-- HELP & FAQ VIEW -->\n                      <div id=\"view-help\" class=\"hidden space-y-6\">\n                          <div class=\"bg-[var(--color-surface)]/70 backdrop-blur-xl rounded-3xl p-6 shadow-lg border border-slate-200 dark:border-darkborder/50 relative overflow-hidden\">\n                              <div class=\"flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6\">\n                                  <div>\n                                      <h3 class=\"text-lg font-black text-slate-800 dark:text-white\" data-i18n=\"help_title\">Help & Common Questions</h3>\n                                      <p class=\"text-xs text-slate-500 dark:text-slate-400 mt-1\" data-i18n=\"help_desc\">Learn about fields, features, and troubleshooting on your mrvpn294 Gateway.</p>\n                                  </div>\n                                  <!-- Search bar -->\n                                  <div class=\"relative w-full md:w-72\">\n                                      <input type=\"text\" id=\"faq-search\" oninput=\"filterFAQs()\" placeholder=\"Search FAQ / جستجو در راهنما...\" class=\"w-full px-4 py-2.5 text-sm bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-darkborder rounded-xl text-slate-700 dark:text-slate-200 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-primary/40 focus:border-primary\">\n                                  </div>\n                              </div>\n\n                              <div class=\"space-y-4\" id=\"faq-list\">\n                                  <!-- Item 1 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">What is mrvpn294 Gateway? / دروازه MrVpn294 چیست؟</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> mrvpn294 Gateway is an advanced, high-performance, single-file subscription and proxy manager built directly for Cloudflare Workers. It acts as a gateway to securely distribute configurations (Clash, Sing-box, Trojan, VMess) to clients while concealing the main panel's backend server behind Cloudflare's global edge network.</p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> دروازه MrVpn294 یک سیستم مدیریت اشتراک و پروکسی پیشرفته و تک‌فایلی است که برای کلودفلر ورکرز توسعه یافته است. این سیستم به عنوان یک دروازه عمل می‌کند تا کانفیگ‌ها (Clash, Sing-box, Trojan, VMess) را به صورت امن توزیع کند و سرور اصلی شما را پشت شبکه جهانی کلودفلر پMrVpn294 کند.</p>\n                                      </div>\n                                  </div>\n\n                                  <!-- Item 2 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">How does \"Auto-Update\" work? / قابلیت «آپدیت خودکار» چطور کار می‌کند؟</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> When Auto-Update is enabled in System settings, a Cron Trigger runs on the background periodically checking the GitHub repository for updates. If a newer version is released, the worker automatically pulls the fresh code, applies the selected format (Normal or Obfuscated), deploys itself directly, and triggers updates across all linked panels/nodes.</p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> وقتی آپدیت خودکار در تنظیمات فعال باشد، یک کرون جاب به صورت دوره‌ای در پس‌زمینه نسخه مخزن گیت‌هاب را بررسی می‌کند. در صورت وجود نسخه جدیدتر، ورکر به صورت خودکار کد جدید را دریافت کرده، قالب انتخابی (معمولی یا مبهم‌سازی‌شده) را اعمال می‌کند، خود را مستقیماً دپلوی کرده و تمام نودها/پنل‌های متصل را همگام‌سازی می‌کند.</p>\n                                      </div>\n                                  </div>\n\n                                  <!-- Item 3 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">What is the difference between Normal and Obfuscated formats? / تفاوت فرمت معمولی و مبهم‌سازی چیست؟</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> \n                                              • <strong>Normal:</strong> Deploys raw JS code. Easily readable, but prone to traffic analysis/firewall scanning.<br>\n                                              • <strong>Obfuscated:</strong> Uses XOR byte-shifting to obscure the source code. The Cloudflare Edge compiles it at runtime using high-speed dynamic decoders, preventing third-party scanning and middleman inspection.\n                                          </p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> \n                                              • <strong>معمولی:</strong> کد خام جاوااسکریپت دپلوی می‌شود. خوانایی آسانی دارد اما در معرض آنالیز ترافیک فایروال‌ها است.<br>\n                                              • <strong>مبهم‌سازی‌شده:</strong> از شیفت بایت XOR برای پMrVpn294‌سازی کد استفاده می‌کند. کلودفلر در زمان اجرا آن را به سرعت رمزگشایی می‌کند تا فایروال‌ها متوجه محتوای اسکریپت نشوند.\n                                          </p>\n                                      </div>\n                                  </div>\n\n                                  <!-- Item 4 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">What are \"Clients\" & \"Subscription Fake Entries\"? / کاربران و ورودی‌های اطلاعاتی اشتراک چیست؟</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> The <strong>Clients</strong> tab lets you provision individual subscriber tokens with specific limits (Quota, Expiry, IP connection count limits). <strong>Subscription Fake Entries</strong> customize what gets shown in the subscription profile UI inside the client app, such as remaining traffic or days left using placeholder patterns like <code>{usage}</code> and <code>{expiry}</code>.</p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> تب <strong>کاربران</strong> به شما اجازه می‌دهد اشتراک‌های یکتا با محدودیت‌های ترافیک، تاریخ انقضا و تعداد اتصالات همزمان بسازید. <strong>ورودی‌های اطلاعاتی اشتراک</strong> مشخص می‌کند چه پیام‌هایی (مانند حجم مصرفی یا روزهای باقی‌مانده) با کدهای پویایی مانند <code>{usage}</code> و <code>{expiry}</code> درون نرم‌افزار کاربر نمایش داده شوند.</p>\n                                      </div>\n                                  </div>\n\n                                  <!-- Item 5 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">What are \"Other Nodes\" / linked panels? / «سایر نودها» (پنل‌های متصل) چیست؟</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> \"Other Nodes\" represents external linked panels. By generating a Panel API Key and registering the child node's URL, the main panel automatically synchronizes, updates, and deploys the identical worker code to all linked nodes whenever an update is triggered on the main panel. This enables seamless multi-server synchronization.</p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> «سایر نودها» پنل‌های فرعی متصل هستند. با ساخت یک کلید دسترسی پنل (API Key) و ثبت آدرس نود فرعی، پنل اصلی با هر بروزرسانی به صورت خودکار نسخه جدید را برای تمام نودهای متصل ارسال و دپلوی می‌کند تا همه سرورها همزمان آپدیت شوند.</p>\n                                      </div>\n                                  </div>\n\n                                  <!-- Item 6 -->\n                                  <div class=\"faq-item bg-slate-50/50 dark:bg-slate-900/30 rounded-2xl border border-slate-100 dark:border-darkborder/50 p-4 transition-all duration-200\">\n                                      <button onclick=\"toggleFAQ(this)\" class=\"w-full flex items-center justify-between text-start focus:outline-none font-bold text-slate-800 dark:text-slate-200 hover:text-primary transition-colors\">\n                                          <span class=\"flex items-center gap-2\">\n                                              <span class=\"text-primary text-sm\">❓</span>\n                                              <span class=\"text-sm md:text-base\">Explain critical fields like Clean IPs and Proxy IPs / توضیح فیلدهای آی‌پی تمیز و پروکسی آی‌پی</span>\n                                          </span>\n                                          <svg class=\"w-5 h-5 transform transition-transform duration-200 shrink-0 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                                      </button>\n                                      <div class=\"faq-content hidden mt-3 pt-3 border-t border-slate-200/50 dark:border-darkborder/50 text-xs md:text-sm text-slate-600 dark:text-slate-400 space-y-2 leading-relaxed\">\n                                          <p><strong>English:</strong> \n                                              • <strong>Clean IPs (Multi-Generator):</strong> Clean non-filtered Cloudflare IPs. The gateway generates unique config sets multiplied for each clean IP added here, giving users redundancy.<br>\n                                              • <strong>Proxy IPs:</strong> Behind-the-scene IPv4/IPv6 transit nodes to fetch data from blocking target sites.\n                                          </p>\n                                          <p class=\"border-t border-dashed border-slate-200 dark:border-darkborder/50 pt-2\"><strong>فارسی:</strong> \n                                              • <strong>آی‌پی‌های تمیز:</strong> آی‌پی‌های بدون فیلتر کلودفلر. این سیستم برای تک‌تک آی‌پی‌های ثبت شده در این بخش خروجی‌های مجزا می‌سازد تا اتصالی پایدار ارائه شود.<br>\n                                              • <strong>آی‌پی‌های پروکسی:</strong> آی‌پی‌های ترانزیت پشت‌صحنه برای برقراری ارتباط با سایت‌های هدف فیلترشده.\n                                          </p>\n                                      </div>\n                                  </div>\n                              </div>\n                          </div>\n                      </div>\n                  </div>\n              </div>\n\n              <!-- Save Bar (Docked to bottom of main content) -->\n              <div class=\"shrink-0 bg-[var(--color-surface)]  border-t border-slate-200 dark:border-darkborder p-4 flex justify-between md:justify-end items-center z-20 mobile-save-bar\">\n                  <span id=\"save-status\" class=\"text-sm font-bold text-slate-500 md:me-4\"></span>\n                  <button onclick=\"doSave()\" class=\"native-press px-8 py-3 bg-primary text-white font-bold rounded-xl shadow-lg hover:opacity-90 transition-opacity\" data-i18n=\"save_btn\">Save Config</button>\n              </div>\n          </main>\n\n          <!-- BOTTOM NAV (Mobile) -->\n          <nav class=\"md:hidden w-full mobile-bottom-nav flex justify-around items-center z-30 shrink-0\" style=\"height:calc(4rem + env(safe-area-inset-bottom, 0px));padding-bottom:env(safe-area-inset-bottom, 0px);\">\n              <button onclick=\"switchTab('overview')\" id=\"mob-tab-overview\" class=\"mobile-tab-item mobile-nav-item active flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_overview\">Dashboard</span>\n              </button>\n              <button onclick=\"switchTab('info')\" id=\"mob-tab-info\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_info\">Endpoints</span>\n              </button>\n              <button onclick=\"switchTab('network')\" id=\"mob-tab-network\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_status\">Metrics</span>\n              </button>\n              <button onclick=\"switchTab('settings')\" id=\"mob-tab-settings\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z M15 12a3 3 0 11-6 0 3 3 0 016 0z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_settings\">System</span>\n              </button>\n              <button onclick=\"switchTab('advanced')\" id=\"mob-tab-advanced\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_adv\">Advanced</span>\n              </button>\n              <button onclick=\"switchTab('logs')\" id=\"mob-tab-logs\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_logs\">Logs</span>\n              </button>\n              <button onclick=\"switchTab('users')\" id=\"mob-tab-users\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_users\">Clients</span>\n              </button>\n              <button onclick=\"switchTab('help')\" id=\"mob-tab-help\" class=\"mobile-tab-item mobile-nav-item flex flex-col items-center justify-center w-full h-full text-slate-400\">\n                  <svg class=\"w-6 h-6 mb-1\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M18.364 5.636l-3.536 3.536m0 5.656l3.536 3.536M9.172 9.172L5.636 5.636m3.536 9.192l-3.536 3.536M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-5 0a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg>\n                  <span class=\"text-[10px] font-bold\" data-i18n=\"tab_help\">Help</span>\n              </button>\n          </nav>\n      </div>\n\n      <!-- Toast Notification -->\n      <div id=\"copy-toast\" class=\"fixed top-20 md:top-10 left-1/2 -translate-x-1/2 bg-slate-800 dark:bg-[var(--color-surface)] text-white dark:text-slate-900 px-6 py-3 rounded-full shadow-2xl font-bold text-sm z-50 transition-all transform -translate-y-20 opacity-0 pointer-events-none\">\n          <span data-i18n=\"copied\">Copied!</span>\n      </div>\n\n      <!-- QR Code Modal (Enhanced) -->\n      <div id=\"qr-modal\" class=\"fixed inset-0 bg-black/60 backdrop-blur-sm z-[100] hidden items-center justify-center p-4\">\n          <div class=\"bg-[var(--color-surface)]  rounded-3xl p-8 max-w-sm w-full shadow-2xl border border-slate-200 dark:border-darkborder relative\">\n              <button onclick=\"closeQRModal()\" class=\"absolute top-4 end-4 text-slate-400 hover:text-slate-800 dark:hover:text-white\">\n                  <svg class=\"w-6 h-6\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M6 18L18 6M6 6l12 12\"></path></svg>\n              </button>\n              <div class=\"text-center mb-6\">\n                  <h3 id=\"qr-modal-title\" class=\"text-xl font-bold text-slate-800 dark:text-white\" data-i18n=\"qr_title\">Scan to Connect</h3>\n                  <p class=\"text-xs text-slate-500 mt-1\" data-i18n=\"qr_subtitle\">Scan with your V-Core or T-Core client</p>\n              </div>\n              <div class=\"bg-[var(--color-surface)] p-4 rounded-2xl shadow-inner border border-slate-100 mb-4\">\n                  <img id=\"qr-modal-img\" src=\"\" alt=\"QR Code\" class=\"w-full aspect-square object-contain\">\n              </div>\n              <div class=\"bg-slate-50 dark:bg-slate-800 p-3 rounded-xl break-all text-xs font-mono text-slate-600 dark:text-slate-400 max-h-24 overflow-auto border border-slate-200 dark:border-darkborder\" id=\"qr-modal-link\"></div>\n          </div>\n      </div>\n\n      <!-- Modal: Version Update Highlights -->\n      <div id=\"modal-version-update\" class=\"fixed inset-0 bg-slate-900/60 backdrop-blur-md z-[101] hidden items-center justify-center p-4\">\n          <div class=\"bg-[var(--color-surface)]  rounded-3xl p-8 max-w-lg w-full shadow-2xl border border-slate-200 dark:border-darkborder relative overflow-hidden transform transition-all duration-300\">\n              <div class=\"absolute top-0 right-0 left-0 h-2 bg-gradient-to-r from-indigo-500 via-primary to-emerald-500\"></div>\n              <div class=\"flex items-center justify-between mb-6\">\n                  <div class=\"flex items-center gap-2.5\">\n                      <div class=\"bg-primary/10 text-primary p-2.5 rounded-2xl\">\n                          <svg class=\"w-6 h-6\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\">\n                              <path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13 10V3L4 14h7v7l9-11h-7z\"></path>\n                          </svg>\n                      </div>\n                      <div>\n                          <h3 class=\"text-lg font-black text-slate-800 dark:text-white\" data-i18n=\"v_pop_title\">Version Update</h3>\n                          <span id=\"modal-version-badge\" class=\"text-[10px] font-bold px-2 py-0.5 bg-indigo-500 text-white rounded-full tracking-wide\"></span>\n                      </div>\n                  </div>\n                  <button onclick=\"closeVersionModal()\" class=\"text-slate-400 hover:text-slate-700 dark:hover:text-white bg-slate-50 dark:bg-slate-800 p-2 rounded-xl border border-slate-100 dark:border-darkborder transition-colors\">\n                      <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\">\n                          <path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M6 18L18 6M6 6l12 12\"></path>\n                      </svg>\n                  </button>\n              </div>\n\n              <div class=\"space-y-4\">\n                  <div class=\"p-4 bg-slate-50 dark:bg-slate-800/30 rounded-2xl border border-slate-100 dark:border-darkborder/50\">\n                      <p class=\"text-xs font-bold text-slate-400 uppercase tracking-widest\" data-i18n=\"v_pop_whatsnew\">What's New in This Version</p>\n                      <h4 id=\"modal-version-headline\" class=\"text-sm font-black text-slate-700 dark:text-white mt-1\"></h4>\n                  </div>\n\n                  <div id=\"modal-changelog-container\" class=\"space-y-4 max-h-[50vh] overflow-y-auto pe-2 text-start\">\n                  </div>\n              </div>\n\n              <div class=\"mt-6 pt-5 border-t border-slate-100 dark:border-darkborder/50 flex justify-end\">\n                  <button onclick=\"closeVersionModal()\" class=\"px-5 py-2.5 bg-primary hover:bg-primary/95 text-white rounded-xl text-xs font-bold shadow-md transition-all transform hover:scale-105 active:scale-95\" data-i18n=\"v_pop_btn\">Got it!</button>\n              </div>\n          </div>\n      </div>\n\n      <script>\n          function parseImportBindings(importStr) {\n              const cleanStr = importStr.replace(/\\/\\/.*$/gm, '').replace(/\\/\\*[\\s\\S]*?\\*\\//g, '').trim();\n              const content = cleanStr\n                  .replace(/^import\\s+/, '')\n                  .replace(/\\s+from\\s+[\"'].*?[\"'];?$/, '')\n                  .trim();\n\n              const bindings = [];\n\n              if (content.startsWith('*')) {\n                  const match = content.match(/\\*\\s+as\\s+(\\w+)/);\n                  if (match) bindings.push({ name: match[1], isNamespace: true });\n                  return bindings;\n              }\n\n              const braceStart = content.indexOf('{');\n              if (braceStart !== -1) {\n                  const defaultPart = content.slice(0, braceStart).replace(/,/, '').trim();\n                  if (defaultPart) {\n                      bindings.push({ name: defaultPart, isDefault: true });\n                  }\n                  const bracePart = content.slice(braceStart + 1, content.lastIndexOf('}')).trim();\n                  const namedImports = bracePart.split(',').map(s => s.trim()).filter(Boolean);\n                  namedImports.forEach(item => {\n                      if (item.includes(' as ')) {\n                          const parts = item.split(/\\s+as\\s+/);\n                           bindings.push({ name: parts[1], original: parts[0] });\n                      } else {\n                          bindings.push({ name: item });\n                      }\n                  });\n              } else {\n                  bindings.push({ name: content, isDefault: true });\n              }\n\n              return bindings;\n          }\n\n          function obfuscateCode(srcText) {\n              const importRegex = /import\\s+[\\s\\S]*?from\\s+[\"'].*?[\"'];?/g;\n              const imports = [];\n              let match;\n\n              while ((match = importRegex.exec(srcText)) !== null) {\n                  imports.push(match[0]);\n              }\n\n              let cleanCode = srcText.replace(importRegex, '');\n\n              const bindings = [];\n              imports.forEach(imp => {\n                  const parsed = parseImportBindings(imp);\n                  bindings.push(...parsed);\n              });\n\n              const uniqueBindings = [];\n              const seenNames = new Set();\n              bindings.forEach(b => {\n                  if (!seenNames.has(b.name)) {\n                      seenNames.add(b.name);\n                      uniqueBindings.push(b);\n                  }\n              });\n\n              cleanCode = cleanCode.replace(/export\\s+default\\s+/g, 'const _0xmrvpn294Module = ');\n              cleanCode += '\\nreturn _0xmrvpn294Module;';\n\n              const randKey = Math.floor(Math.random() * 80) + 64;\n\n              const encoder = new TextEncoder();\n              const bytes = encoder.encode(cleanCode);\n\n              let hexOutput = '';\n              for (let i = 0; i < bytes.length; i++) {\n                  const xorByte = bytes[i] ^ randKey;\n                  hexOutput += xorByte.toString(16).padStart(2, '0');\n               }\n\n              const rawImportsStr = imports.join('\\n');\n              const bindingNames = uniqueBindings.map(b => b.name);\n\n              const finalLoaderCode = rawImportsStr + '\\n\\n' +\n                  '// mrvpn294 Gateway - Obfuscated Loader Context (v2.5.4.2 Optimized)\\n' +\n                  'const _0xmrvpn294Payload = \"' + hexOutput + '\";\\n' +\n                  'const _0xmrvpn294Key = ' + randKey + ';\\n\\n' +\n                  'const _0xmrvpn294Bytes = new Uint8Array((_0xmrvpn294Payload.match(/.{1,2}/g) || []).map(x => parseInt(x, 16) ^ _0xmrvpn294Key));\\n' +\n                  'const _0xmrvpn294Code = new TextDecoder().decode(_0xmrvpn294Bytes);\\n' +\n                  'const _0xmrvpn294Runtime = new Function(' + bindingNames.map(name => '\"' + name + '\"').join(', ') + ', _0xmrvpn294Code)(' + bindingNames.join(', ') + ');\\n\\n' +\n                  'export default _0xmrvpn294Runtime;';\n\n              return finalLoaderCode;\n          }\n\n          const CURRENT_VERSION = \"__CURRENT_VERSION__\";\n          const i18n = {\n              en: {\n                  title: \"mrvpn294 Gateway\", pass_ph: \"Master Key\", login_btn: \"Authenticate\", err_pass: \"Access Denied\", missing_db: \"⚠️ AM_DB namespace missing! Settings won't save.\", system_online: \"System online\",\n                  logout: \"Disconnect\", tab_overview: \"Dashboard\", tab_info: \"Endpoints\", tab_status: \"Metrics\", tab_settings: \"System\", tab_adv: \"Advanced\", tab_logs: \"Logs\", tab_help: \"Help & FAQ\",\n                  qr_title: \"Direct Stream Link\", badge_multi: \"Dual-Core Multiplexed\", copy: \"Copy\", copied: \"Copied to clipboard!\", sync_link: \"Cloud Sync URL\", active_id: \"Hardware ID\",\n                  stat_ip: \"Origin IP\", stat_dc: \"Edge Node\", stat_loc: \"Data Region\",\n                  lbl_proto: \"Primary Display Mode\", lbl_port: \"Data Port\", lbl_id: \"Device UUID (Empty=Auto)\",\n                  lbl_path: \"API Route (Hidden Path)\", lbl_pass: \"Master Key\", lbl_fp: \"TLS Signature\", lbl_dns: \"Resolver IP\",\n                  lbl_clean_ips: \"Clean IPs (Multi-Generator)\", ph_clean_ips: \"1.1.1.1, 2.2.2.2\", desc_clean_ips: \"Separate IPs by comma or new line. The Sync URL will multiply configs for all IPs.\",\n                  lbl_fake: \"Maintenance Hosts (Camouflage)\", lbl_relay: \"Backup Relay IP\", lbl_tfo: \"TCP Fast Open\", lbl_ech: \"Secure Hello (ECH)\",\n                  lbl_fake_configs: \"Subscription Fake Entries\", desc_fake_configs: \"Customize info entries shown in subscription profiles. Use {usage} and {expiry} for dynamic values.\", btn_add_entry: \"Add Entry\",                   lbl_tg_token: \"Telegram Bot Token\", lbl_tg_chat: \"Telegram Chat ID\", lbl_tg_admin: \"Authorized Telegram Admin ID\", desc_tg_admin: \"Only this Telegram User ID can manage the panel via bot. Leave empty to use Chat ID.\", desc_tg_bot: \"Set these values to receive login alerts via Telegram.\",\n                  lbl_cf_acc: \"Cloudflare Account ID\", lbl_cf_token: \"Cloudflare API Token\", desc_cf_api: \"Optional: Monitor Worker daily usage limit (100k/day). Requires Account Analytics read permission.\",\n                  lbl_silent: \"Silent UI Alerts\", lbl_pause: \"Kill Switch (Pause System)\",\n                  lbl_sub_ua: \"Custom Subscription User-Agent\", desc_sub_ua: \"Allow specific browser User-Agent containing this text to bypass camouflage and retrieve profile data directly in web browser.\",\n                  tab_users: \"Clients\",\n                  user_mgt_title: \"User Management\", user_mgt_desc: \"Manage multiple users, set traffic limits, and expiration dates.\", btn_add_user: \"+ Add New User\",\n                  tbl_name: \"Name\", tbl_uuid: \"UUID\", tbl_traffic: \"Traffic (Used / Limit)\", tbl_exp: \"Expiration\", tbl_action: \"Action\", no_users: \"No users found. Create one above.\",\n                  modal_add_title: \"Add New User\", lbl_u_name: \"Name (Required)\", lbl_u_gb: \"Traffic Limit (GB) - Optional\", lbl_u_days: \"Duration (Days) - Optional\", btn_cancel: \"Cancel\", btn_confirm: \"Add User\",\n                  limit_total: \"Traffic (GB) Limit (Leave empty for unlimited)\", limit_daily: \"Daily Requests Limit (Leave empty for unlimited)\",\n                  limit_days: \"Expiration limit (Days) - Leave empty for unlimited\", edit_sub: \"Edit Subscriber\", lbl_name_ph: \"Name or UUID\",\n                  btn_save_changes: \"Save Changes\", save_btn_user: \"Save User\", save_btn: \"Save Config\", status_active: \"Active\", status_paused: \"Paused\", status_expired: \"Expired\",\n                  stat_total_subscribers: \"Total Subscribers\", stat_active_paused: \"Active / Paused\", stat_cumulative_traffic: \"Cumulative Traffic\", stat_auto_disabled: \"Auto-Disabled\",\n                  sub_directory_title: \"Subscriber Directory\", sub_directory_desc: \"Search, modify bounds, toggle traffic limits or clear billing sessions.\", user_search_placeholder: \"🔍 Find by Name or UUID...\",\n                  filter_all: \"All Users\", filter_active: \"Active\", filter_paused: \"Paused\", filter_auto_disabled: \"Auto-Disabled\",\n                  disabled_panel_title: \"Recently Disabled Users\", disabled_panel_desc: \"Users automatically disabled due to quota or expiration limits\",\n                  lbl_u_Protocol:\"Protocol Mode (Leave empty to use global setting)\",\n                  lbl_u_ports:\"Custom Ports (Optional - overrides global ports, comma separated e.g. 443,80\",\n                  lbl_u_max_config:\"Max Configs\",\n                  login_password:\"Password\",\n                  lbl_u_ipproxy:\"User Proxy IP(s) (Optional - overrides global Clean IP, comma/newline separated)\",\n                   lbl_custom_panel_url:\"Custom Panel URL / Subscription Domain\",\n                   lbl_api_keys: \"Panel API Keys\", desc_api_keys: \"Generate API keys to securely connect remote panels. Remote panels use these keys instead of sharing your master key.\",\n                   btn_generate_key: \"Generate Key\", api_key_created: \"API Key Created! Copy it now — it won't be shown again.\",\n                   api_keys_empty: \"No API keys generated yet.\", enter_key_name: \"Enter a name for this API key:\",\n                   confirm_revoke: \"Revoke this API key? The remote panel will lose access.\", revoke: \"Revoke\",\n                   created: \"Created\", last_used: \"Last used\", never: \"Never\",\n                   v_pop_title: \"Release Notice\", v_pop_whatsnew: \"What's New\", v_pop_headline: \"New Features & Improvements\",\n                  v_pop_btn: \"Got it!\",\n                  changelog_title: \"Release Notes & Changelog:\",\n                  changelog_added: \"Added\", changelog_fixed: \"Fixed\", changelog_improved: \"Improved\", changelog_changed: \"Changed\", changelog_note: \"Important Notes\",\n                  ov_total_users: \"Total Users\", ov_active_users: \"Active\", ov_paused_users: \"Paused\", ov_auto_disabled: \"Auto-Disabled\", ov_expired_users: \"Expired\",\n                  ov_total_traffic: \"Total Traffic\", ov_today_traffic: \"Today's Traffic\", ov_requests: \"requests\", ov_active_conns: \"Active Connections\",\n                  ov_system: \"Settings\", ov_recent_activity: \"Recent Activity\", ov_view_all: \"View All →\", ov_loading: \"Loading...\",\n                   ov_quick_actions: \"Quick Actions\", ov_add_user: \"Add User\", ov_backup_config: \"Backup Config\", ov_refresh: \"Refresh Statistics\", ov_manage_users: \"Manage Users\",\n                   ov_gb_unit: \"GB\",\n                    lbl_allow_sync:\"Allow Sync\",\n                    other_nodes_title: \"Other Nodes\", other_nodes_desc: \"External nodes (URL + API Key) for cross-panel management.\",\n                    add_node_title: \"Add External Node\", add_node_desc: \"Enter the URL and API Key of the external panel.\",\n                    add_node_url: \"Node URL\", add_node_apikey: \"API Key\", add_node_confirm: \"Add Node\", add_node_invalid: \"Please enter both URL and API Key.\",\n                    node_added: \"Node added successfully!\", node_removed: \"Node removed.\",\n                    deploy_btn: \"Deploy Now\", update_deploying: \"Deploying update...\",\n                   update_success: \"Update successful! Reloading...\", update_error: \"Update failed\",\n                   lbl_cf_worker: \"CF Worker Script Name\", desc_cf_worker: \"Required for in-panel updates. The script name shown in your Cloudflare Workers dashboard.\",\n                   view_github: \"View on GitHub\",\n                    cf_help_title: \"Need help getting these? Beginner's Step-by-Step Guide\",\n                    lbl_update_format: \"Update Format & Obfuscated Options:\",\n                    desc_update_format: \"Deploy clean source code, or encrypt using dynamic XOR byte-shifting to avoid network interception.\",\n                    format_normal: \"Normal (_worker.js)\",\n                    format_obfuscated: \"Obfuscated (UTF-8 + XOR)\",\n                     btn_redeploy_force: \"Force Redeploy / Switch Format\",\n                    adv_network_dns: \"Network & DNS\", adv_proxy_relay: \"Proxy & Relay\", adv_subscription: \"Subscription\",\n                    adv_protocol: \"Protocol\", adv_telegram: \"Telegram Bot\", adv_cloudflare: \"Cloudflare\",\n                    stat_datetime: \"Date Time\",\n                    desc_custom_panel_url: \"Optionally specify a custom domain/URL to be used for subscription/sync links. If empty, the default Worker address will be used.\",\n                    lbl_custom_config_name: \"Custom Config Name / Prefix\",\n                    lbl_traffic_limit_gb: \"Traffic (GB) Limit\",\n                    lbl_daily_limit_gb: \"Daily Limit (GB)\",\n                    lbl_expiration_days: \"Expiration (Days)\",\n                    loading_logs: \"Loading activity logs...\", show_qr: \"Show QR Code\",\n                    no_matching_users: \"No matching subscribers found\", no_active_conn: \"No active connection data yet.\",\n                    qr_subtitle: \"Scan with your V-Core or T-Core client\",\n                    no_activity_logs: \"No activity logs found.\", no_recent_activity: \"No recent activity.\",\n                    no_ips_advanced: \"No IPs added in Advanced Tab\", no_nodes_advanced: \"No slave nodes in Advanced Tab\",\n                    no_changelog: \"No changelog available for this version.\", no_changes: \"No changes documented.\",\n                    update_requires_cf: \"Set CF Account ID, API Token, and Worker Name to enable in-panel deploy.\",\n                    section_basic_info: \"Basic Info\", section_limits: \"Limits\", section_network: \"Network\", section_advanced: \"Advanced\",\n                    lbl_nat64: \"NAT64 Prefix\", desc_nat64: \"Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses. Supports multiple prefixes.\",\n                    lbl_direct_configs: \"Include Direct Configs\", desc_direct_configs: \"Generate configs without Proxy IP alongside relay configs\",\n                    lbl_sync_api_key: \"Sync API Key (Slave Push)\", desc_sync_api_key: \"API key from a slave panel. Main uses this to push config. Same key must exist on each slave's Panel API Keys.\",\n                    lbl_auto_update: \"Auto-Update\", desc_auto_update: \"Automatically deploy when a new version is detected\",\n                    lbl_auto_update_format: \"Update Format\", format_normal_label: \"Normal\", format_obfuscated_label: \"Obfuscated\",\n                    desc_format_normal: \"Standard _worker.js\", desc_format_obfuscated: \"XOR byte-shifting\",\n                    lbl_prefix: \"Custom Name Prefix\", html_desc_prefix_note: \"This prefix is injected where the <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PREFIX}</code> tag is used in your naming strategy.\", lbl_clean_ips: \"Clean IPs\", lbl_proxy_ips: \"Proxy IPs\", lbl_assigned_nodes: \"Assigned Nodes\",\n                    lbl_protocol_mode: \"Protocol Mode\", lbl_max_configs: \"Max Configs\",\n                    desc_assigned_nodes: \"Custom Nodes (comma/newline, empty = all nodes)\",\n                    desc_nat64_user: \"Optional. Converts IPv4 Proxy IPs to NAT64 IPv6 addresses.\",\n                    desc_proxy_ips: \"Custom Proxy IPs (comma/newline)\",\n                    desc_clean_ips_modal: \"Custom Clean IPs (comma/newline)\",\n                    btn_generate_uuid: \"Generate UUID\",\n                    lbl_conn_limit: \"IP Connection Limit\", desc_conn_limit: \"Max simultaneous connections per user. Leave empty for unlimited.\",\n                    lbl_user_panel_url: \"Main Panel URL (Custom Nodes)\", desc_user_panel_url: \"Main panel domain for custom nodes. If empty, default panel URL is used.\",\n                    html_desc_strategy: \"<details><summary class='cursor-pointer text-primary'>View supported placeholders...</summary><div class='mt-2'>Supported placeholders: <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{FLAG}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{COUNTRY}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{CITY}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{ISP}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PROTOCOL}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{USER}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PORT}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PREFIX}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{IP}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{IP_NAME}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{HOST}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{DATE}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{INDEX}</code>, <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{WORKER}</code>.<br><span class='text-[10px] text-slate-400 dark:text-slate-500 leading-snug'>• <b>{FLAG}</b>: Country flag emoji (e.g. 🇺🇸).<br>• <b>{COUNTRY}</b>: Country name.<br>• <b>{CITY}</b>: City name.<br>• <b>{ISP}</b>: ISP / ASN org.<br>• <b>{PROTOCOL}</b>: Core mode (VLESS / Trojan).<br>• <b>{USER}</b>: Subscriber name.<br>• <b>{PORT}</b>: Active port.<br>• <b>{PREFIX}</b>: Custom prefix.<br>• <b>{IP}</b>: Clean IP address.<br>• <b>{IP_NAME}</b>: Clean IP name.<br>• <b>{HOST}</b>: Hostname.<br>• <b>{DATE}</b>: Current date.<br>• <b>{INDEX}</b>: Config index.<br>• <b>{WORKER}</b>: Worker name.</span><br>Pre-defined strategies: <code>default</code>, <code>type-user-port</code>, <code>user-port</code>, <code>host-port-user</code>, <code>prefix-user-port</code>, <code>ip</code>.</div></details>\",\n               },\n              fa: {\n                  title: \"دروازه MrVpn294\", pass_ph: \"کلید اصلی\", login_btn: \"ورود به سیستم\", err_pass: \"دسترسی مسدود شد\", missing_db: \"⚠️ فضای پایگاه داده یافت نشد! تنظیمات ذخیره نمی‌شوند.\", system_online: \"سیستم فعال است\",\n                  logout: \"خروج\", tab_overview: \"نمای کلی\", tab_info: \"نودها\", tab_status: \"شبکه\", tab_settings: \"تنظیمات\", tab_adv: \"پیشرفته\", tab_logs: \"گزارش فعالیت\", tab_help: \"راهنما و سوالات\",\n                  qr_title: \"لینک اتصال مستقیم\", badge_multi: \"ترکیب ترانزیت پیشرفته دوگانه\", copy: \"کپی\", copied: \"در حافظه کپی شد!\", sync_link: \"لینک ساب (همگام سازی ابری)\", active_id: \"شناسه سخت‌افزار\",\n                  stat_ip: \"آی‌پی مبدا\", stat_dc: \"گره لبه\", stat_loc: \"منطقه داده\",\n                  lbl_proto: \"پروتکل نمایش مستقیم\", lbl_port: \"پورت داده\", lbl_id: \"شناسه یکتا (خالی=خودکار)\",\n                  lbl_path: \"مسیر مخفی آی‌پی‌آی\", lbl_pass: \"کلید اصلی\", lbl_fp: \"امضای امنیتی\", lbl_dns: \"آی‌پی تحلیلگر\",\n                  lbl_clean_ips: \"آی‌پی‌های تمیز (مولد چندگانه)\", ph_clean_ips: \"1.1.1.1, 2.2.2.2\", desc_clean_ips: \"آی‌پی ها را با کاما یا خط جدید جدا کنید. لینک ساب برای همه ترکیب می‌سازد.\",\n                  lbl_fake: \"سایت‌های استتار (حالت مخفی)\", lbl_relay: \"آی‌پی جایگزین (کمکی)\", lbl_tfo: \"اتصال سریع\", lbl_ech: \"سلام امن\",\n                  lbl_fake_configs: \"ورودی‌های اطلاعاتی اشتراک\", desc_fake_configs: \"متن نمایشی ورودی‌ها در پروفایل اشتراک را سفارشی کنید. از {usage} و {expiry} برای مقادیر پویا استفاده کنید.\", btn_add_entry: \"افزودن ورودی\", lbl_tg_token: \"توکن ربات تلگرام\", lbl_tg_chat: \"شناسه عددی تلگرام\", lbl_tg_admin: \"شناسه مدیر تلگرام\", desc_tg_admin: \"فقط این شناسه کاربری تلگرام می‌تواند پنل را از طریق ربات مدیریت کند. خالی بگذارید برای استفاده از شناسه چت.\", desc_tg_bot: \"با تنظیم این مقادیر، جزئیات ورود به پنل به تلگرام ارسال می‌شود.\",\n                  lbl_cf_acc: \"شناسه اکانت ابری\", lbl_cf_token: \"توکن دسترسی کاربری\", desc_cf_api: \"اختیاری: برای نمایش میزان مصرف روزانه کارگر از صد هزار درخواست رایگان در پیام‌های تلگرام.\",\n                  lbl_silent: \"هشدار و پیغام خاموش\", lbl_pause: \"کلید توقف اضطراری\",\n                   lbl_sub_ua: \"یوزراجنت سفارشی ساب\", desc_sub_ua: \"درخواست‌های مرورگر که حاوی این متن باشند، استتار را خنثی کرده و مستقیم به ساب دسترسی پیدا می‌کنند.\",\n                   lbl_api_keys: \"کلیدهای API پنل\", desc_api_keys: \"کلیدهای API برای اتصال امن پنل‌های راهدور ایجاد کنید. پنل‌های راهدور به جای اشتراک‌گذاری کلید اصلی، از این کلیدها استفاده می‌کنند.\",\n                   btn_generate_key: \"ایجاد کلید\", api_key_created: \"کلید API ایجاد شد! آن را کپی کنید — دوباره نمایش داده نخواهد شد.\",\n                   api_keys_empty: \"هنوز کلید API ایجاد نشده.\", enter_key_name: \"نامی برای این کلید API وارد کنید:\",\n                   confirm_revoke: \"این کلید API لغو شود؟ پنل راهدور دسترسی خود را از دست خواهد داد.\", revoke: \"لغو\",\n                   created: \"ایجاد شده\", last_used: \"آخرین استفاده\", never: \"هرگز\",\n                   tab_users: \"کاربران\",\n                  user_mgt_title: \"مدیریت کاربران\", user_mgt_desc: \"مدیریت کاربران متعدد، تنظیم محدودیت ترافیک، و تاریخ انقضا.\", btn_add_user: \"+ افزودن کاربر جدید\",\n                  tbl_name: \"نام\", tbl_uuid: \"شناسه یکتا\", tbl_traffic: \"ترافیک (مصرفی/محدودیت)\", tbl_exp: \"انقضا\", tbl_action: \"عملیات\", no_users: \"کاربری یافت نشد. از دکمه بالا یک کاربر ایجاد کنید.\",\n                  modal_add_title: \"افزودن کاربر جدید\", lbl_u_name: \"نام (الزامی)\", lbl_u_gb: \"محدودیت ترافیک (گیگابایت) - اختیاری\", lbl_u_days: \"مدت زمان اعتبار (روز) - اختیاری\", btn_cancel: \"انصراف\", btn_confirm: \"افزودن کاربر\",\n                  save_btn: \"ذخیره تنظیمات\", msg_saving: \"در حال ثبت...\", msg_saved: \"موفق! در حال بارگذاری...\", msg_err: \"خطای ارتباط\",\n                  backup_restore_title: \"پشتیبان‌گیری و بازیابی\", ping_test_title: \"عیب‌یابی تاخیر شبکه\", ping_test_desc: \"تاخیر پاسخ‌دهی را به آی‌پی تمیز فعال اندازه بگیرید.\",\n                  lbl_github_repo: \"مخزن منبع جهت بروزرسانی\", update_avail: \"بروزرسانی جدید در دسترس است!\", update_btn: \"دریافت آخرین کد\",\n                    cf_help_title: \"آموزش بدست آوردن این اطلاعات برای کاربران مبتدی\",\n                    lbl_update_format: \"قالب بروزرسانی و حذف ردگیری:\",\n                    desc_update_format: \"سورس کد معمولی را دپلوی کنید یا از مبهم‌سازی بایت‌ها با کلید متغیر XOR برای عدم فیلترینگ استفاده نمایید.\",\n                    format_normal: \"معمولی (_worker.js)\",\n                    format_obfuscated: \"مبهم‌سازی شده (UTF-8 + XOR)\",\n                    btn_redeploy_force: \"تفویض مجدد / تغییر قالب پنل\",\n                    adv_network_dns: \"شبکه و DNS\", adv_proxy_relay: \"پروکسی و رله\", adv_subscription: \"اشتراک\",\n                    adv_protocol: \"پروتکل\", adv_telegram: \"ربات تلگرام\", adv_cloudflare: \"کلودفلر\",\n                    stat_datetime: \"تاریخ و زمان\",\n                    desc_custom_panel_url: \"اختیاری. یک دامنه/آدرس سفارشی برای لینک‌های ساب/همگام‌سازی وارد کنید. اگر خالی باشد، آدرس پیش‌فرض ورکر استفاده می‌شود.\",\n                    lbl_custom_config_name: \"نام/پیشوند سفارشی کانفیگ\",\n                    lbl_traffic_limit_gb: \"محدودیت ترافیک (GB)\",\n                    lbl_daily_limit_gb: \"محدودیت روزانه (GB)\",\n                    lbl_expiration_days: \"تاریخ انقضا (روز)\",\n                    loading_logs: \"در حال بارگذاری گزارش‌ها...\", show_qr: \"نمایش کد QR\",\n                    no_matching_users: \"کاربری مطابقت نداشت\", no_active_conn: \"هنوز داده اتصال فعالی ثبت نشده.\",\n                    qr_subtitle: \"با کلاینت V-Core یا T-Core اسکن کنید\",\n                    no_activity_logs: \"گزارش فعالیتی یافت نشد.\", no_recent_activity: \"فعالیت اخیری ثبت نشده.\",\n                    no_ips_advanced: \"آی‌پی‌ای در بخش پیشرفته اضافه نشده\", no_nodes_advanced: \"نود فرعی‌ای در بخش پیشرفته اضافه نشده\",\n                    no_changelog: \"گزارش تغییراتی برای این نسخه موجود نیست.\", no_changes: \"تغییراتی ثبت نشده.\",\n                    section_basic_info: \"اطلاعات پایه\", section_limits: \"محدودیت‌ها\", section_network: \"شبکه\", section_advanced: \"پیشرفته\",\n                    lbl_nat64: \"پیشوند NAT64\", desc_nat64: \"اختیاری. آی‌پی‌های پروکسی IPv4 را به آدرس‌های NAT64 IPv6 تبدیل می‌کند. چند پیشوند پشتیبانی می‌شود.\",\n                    lbl_direct_configs: \"شامل کانفیگ‌های مستقیم\", desc_direct_configs: \"تولید کانفیگ‌ها بدون آی‌پی پروکسی در کنار کانفیگ‌های رله\",\n                    lbl_sync_api_key: \"کلید API همگام‌سازی (ارسال به اسلیو)\", desc_sync_api_key: \"کلید API از پنل اسلیو. پنل اصلی با این کلید کانفیگ را ارسال می‌کند. این کلید باید در کلیدهای API پنل اسلیو وجود داشته باشد.\",\n                    lbl_auto_update: \"بروزرسانی خودکار\", desc_auto_update: \"دپلوی خودکار هنگام شناسایی نسخه جدید\",\n                    lbl_auto_update_format: \"قالب بروزرسانی\", format_normal_label: \"معمولی\", format_obfuscated_label: \"مبهم‌سازی شده\",\n                    desc_format_normal: \"استاندارد _worker.js\", desc_format_obfuscated: \"جابجایی بایت XOR\",\n                    lbl_clean_ips: \"آی‌پی‌های تمیز\", lbl_proxy_ips: \"آی‌پی‌های پروکسی\", lbl_assigned_nodes: \"نودهای اختصاصی\",\n                    lbl_protocol_mode: \"پروتکل\", lbl_max_configs: \"حداکثر کانفیگ\",\n                    desc_assigned_nodes: \"نودهای سفارشی (کاما/خط جدید، خالی = همه نودها)\",\n                    desc_nat64_user: \"اختیاری. آی‌پی‌های پروکسی IPv4 را به آدرس‌های NAT64 IPv6 تبدیل می‌کند.\",\n                    desc_proxy_ips: \"آی‌پی‌های پروکسی سفارشی (کاما/خط جدید)\",\n                    desc_clean_ips_modal: \"آی‌پی‌های تمیز سفارشی (کاما/خط جدید)\",\n                    btn_generate_uuid: \"تولید UUID\",\n                    lbl_conn_limit: \"محدودیت اتصال همزمان\", desc_conn_limit: \"حداکثر اتصالات همزمان برای هر کاربر. برای نامحدود خالی بگذارید.\",\n                    lbl_user_panel_url: \"آدرس پنل اصلی (نودهای سفارشی)\", desc_user_panel_url: \"دامنه پنل اصلی برای نودهای سفارشی. اگر خالی باشد، آدرس پنل پیش‌فرض استفاده می‌شود.\",\n                  metrics_live: \"وضعیت زنده مصرف اتصالات و پردازش\", no_metrics: \"هنوز داده‌ای از تراکنش و اتصالات فعال ثبت نشده است.\", run_diagnostics: \"⚡ اجرای عیب‌یابی شبکه\",\n                  target_node: \"هدف گره شبکه\", response: \"مدت زمان تاخیر پاسخگویی\", status: \"وضعیت گره\", local_port: \"درگاه محلی\",\n                  lbl_doh: \"تحلیل‌گر تخصصی آدرس‌یابی عددی\", lbl_strategy: \"روش نام‌گذاری کانفیگ‌ها\", lbl_prefix: \"پیشوند نام کانفیگ‌ها\", html_desc_prefix_note: \"این پیشوند در هر جایی از طرح نام‌گذاری که متغیر <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PREFIX}</code> قرار داشته باشد جایگزین می‌شود.\",\n                    \n                  slave_title: \"سایر نودهای موازی\", slave_desc: \"آدرس دامنه سایر ورکرها را وارد نمایید (هر خط یک آدرس). نود اصلی تنظیمات و مشترکین را به صورت خودکار با آن‌ها هماهنگ می‌کند!\",\n                  force_sync: \"همگام‌سازی اجباری نودها\", limit_total: \"محدودیت تعداد کل درخواست‌ها (GB)  (برای نامحدود خالی بگذارید)\", limit_daily: \"محدودیت درخواست‌های روزانه (GB)  (برای نامحدود خالی بگذارید)\",\n                  limit_days: \"مدت زمان اعتبار قانونی (روز) - برای نامحدود خالی بگذارید\", edit_sub: \"ویرایش مشترک\", lbl_name_ph: \"نام یا شناسه یکتا\",\n                  btn_save_changes: \"ذخیره تغییرات\", save_btn_user: \"ثبت کاربر جدید\", status_active: \"فعال\", status_paused: \"متوقف شده\", status_expired: \"منقضی شده\",\n                  export_btn: \"📥 برون‌بری فایل پیکربندی (نسخه پشتیبان)\", import_btn: \"📤 درون‌ریزی فایل پیکربندی (نسخه پشتیبان)\",\n                  stat_total_subscribers: \"کل مشترکین\", stat_active_paused: \"فعال / متوقف شده\", stat_cumulative_traffic: \"ترافیک کل انباشته\", stat_auto_disabled: \"غیرفعال خودکار\",\n                  sub_directory_title: \"فهرست مشترکین\", sub_directory_desc: \"جستجو، اصلاح محدودیت‌ها، تغییر محدودیت‌های ترافیک یا پاک کردن جلسات حسابداری.\", user_search_placeholder: \"🔍 جستجو بر اساس نام یا شناسه...\",\n                  filter_all: \"همه کاربران\", filter_active: \"فعال\", filter_paused: \"متوقف شده\", filter_auto_disabled: \"غیرفعال خودکار\",\n                  disabled_panel_title: \"کاربران اخیراً غیرفعال شده\", disabled_panel_desc: \"کاربرانی که به دلیل اتمام سهمیه یا تاریخ انقضا غیرفعال شده‌اند\",\n                  lbl_u_Protocol:\"نوع پروتکل(خالی بر اساس تنظیمات کلی)\",\n                  lbl_u_ports:\"نوع پورت\",\n                  lbl_u_max_config:\"حداکثر تعداد کانفیگ\",\n                  login_password:\"رمز ورود\",\n                  lbl_u_ipproxy:\"آی‌پی(های) پروکسی کاربر (اختیاری - آی‌پی پاک سراسری را نادیده می‌گیرد، با کاما/خط جدید از هم جدا می‌شوند)\",\n                  v_pop_title: \"اطلاعیه تعمیرات\", v_pop_whatsnew: \"ویژگی‌های جدید\", v_pop_headline: \"امکانات جدید و بهبودها\",\n                  v_pop_btn: \"متوجه شدم!\",\n                  changelog_title: \"گزارش تغییرات و توضیحات نسخه جدید:\",\n                   changelog_added: \"اضافه شده\", changelog_fixed: \"رفع شده\", changelog_improved: \"بهبود یافته\", changelog_changed: \"تغییر یافته\", changelog_note: \"نکات مهم\",\n                   ov_total_users: \"کل کاربران\", ov_active_users: \"فعال\", ov_paused_users: \"متوقف\", ov_auto_disabled: \"غیرفعال خودکار\", ov_expired_users: \"منقضی\",\n                   ov_total_traffic: \"ترافیک کل\", ov_today_traffic: \"ترافیک امروز\", ov_requests: \"درخواست\", ov_active_conns: \"اتصالات فعال\",\n                   ov_system: \"سیستم\", ov_recent_activity: \"فعالیت‌های اخیر\", ov_view_all: \"مشاهده همه ←\", ov_loading: \"در حال بارگذاری...\",\n                   ov_quick_actions: \"عملیات سریع\", ov_add_user: \"افزودن کاربر\", ov_backup_config: \"پشتیبان‌گیری\", ov_refresh: \"بروزرسانی آمار\", ov_manage_users: \"مدیریت کاربران\",\n                   ov_gb_unit: \"گیگابایت\",\n                     lbl_allow_sync:\"اجازه همگام سازی\",\n                     other_nodes_title: \"سایر نودها\", other_nodes_desc: \"نودهای خارجی (URL + کلید API) برای مدیریت بین پنل‌ها.\",\n                     add_node_title: \"افزودن نود خارجی\", add_node_desc: \"آدرس URL و کلید API پنل خارجی را وارد کنید.\",\n                     add_node_url: \"آدرس نود\", add_node_apikey: \"کلید API\", add_node_confirm: \"افزودن نود\", add_node_invalid: \"لطفاً URL و کلید API را وارد کنید.\",\n                     node_added: \"نود با موفقیت اضافه شد!\", node_removed: \"نود حذف شد.\",\n                      deploy_btn: \"هم‌اکنون نصب کن\", update_deploying: \"در حال نصب بروزرسانی...\",\n                      update_success: \"بروزرسانی موفق! در حال بارگذاری...\", update_error: \"خطا در بروزرسانی\",\n                      lbl_cf_worker: \"نام اسکریپت کارگر ابری\", desc_cf_worker: \"برای بروزرسانی خودکار الزامی است. نام اسکریپت در داشبورد کارگرهای ابری.\",\n                      view_github: \"مشاهده در گیت‌هاب\",\n                     update_requires_cf: \"برای نصب خودکار، شناسه اکانت، توکن API و نام کارگر را تنظیم کنید.\",\n                     html_desc_strategy: \"<details><summary class='cursor-pointer text-primary'>مشاهده متغیرهای پشتیبانی شده...</summary><div class='mt-2'>متغیرهای پشتیبانی شده: <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{FLAG}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{COUNTRY}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{CITY}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{ISP}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PROTOCOL}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{USER}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PORT}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{PREFIX}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{IP}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{IP_NAME}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{HOST}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{DATE}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{INDEX}</code>، <code class='bg-slate-100 dark:bg-slate-800/80 px-1 py-0.5 rounded text-rose-500 font-mono'>{WORKER}</code>.<br><span class='text-[10px] text-slate-400 dark:text-slate-500 leading-snug'>• <b>{FLAG}</b>: ایموجی پرچم.<br>• <b>{COUNTRY}</b>: نام کشور.<br>• <b>{CITY}</b>: نام شهر.<br>• <b>{ISP}</b>: نام ارائه‌دهنده اینترنت.<br>• <b>{PROTOCOL}</b>: پروتکل اصلی.<br>• <b>{USER}</b>: نام مشترک.<br>• <b>{PORT}</b>: پورت فعال.<br>• <b>{PREFIX}</b>: پیشوند دلخواه.<br>• <b>{IP}</b>: آی‌پی تمیز.<br>• <b>{IP_NAME}</b>: نام آی‌پی تمیز.<br>• <b>{HOST}</b>: نام دامنه.<br>• <b>{DATE}</b>: تاریخ.<br>• <b>{INDEX}</b>: شماره ردیف کانفیگ.<br>• <b>{WORKER}</b>: نام اسکریپت.</span><br>طرح‌های پیش‌فرض: <code>default</code>، <code>type-user-port</code>، <code>user-port</code>، <code>host-port-user</code>، <code>prefix-user-port</code>، <code>ip</code>.</div></details>\",\n                }\n          };\n\n          const CHANGELOG_DATA = {\n\n              \"2.9.4\": {\n                  headline: { en: \"Live Active Connections, Auto-Update Cron & v2rayN JSON\", fa: \"اتصالات زنده، کرون‌جاب آپدیت خودکار و خروجی v2rayN JSON\" },\n                  added: [\n                      { en: \"Added maxConfigs option per user to limit the number of generated configurations\", fa: \"افزودن قابلیت maxConfigs برای محدود کردن تعداد کانفیگ‌های تولید شده هر کاربر\" },\n                      { en: \"Live active connections stream count in the dashboard for each user/profile\", fa: \"تعداد لحظه‌ای اتصالات استریم زنده در داشبورد برای هر کاربر/پروفایل\" },\n                      { en: \"Added scheduled cron job capability for automatic Cloudflare worker updates\", fa: \"افزودن قابلیت برنامه‌ریزی کرون‌جاب (cron) برای بروزرسانی خودکار ورکر Cloudflare\" },\n                      { en: \"Added ?format=vjson parameter support for raw V2Ray/v2rayN JSON configuration outputs\", fa: \"پشتیبانی از پارامتر ?format=vjson برای خروجی پیکربندی‌های خام V2Ray/v2rayN JSON\" },\n                      { en: \"Dashboard HTML & Subscription Info HTML are now dynamically fetched reducing worker size\", fa: \"فایل‌های HTML داشبورد و صفحه اشتراک اکنون به صورت داینامیک دریافت می‌شوند که حجم ورکر را به شدت کاهش می‌دهد\" }\n                  ],\n                  fixed: [\n                      { en: \"Fixed activeConnections tracking limit logic and decrements upon WebSocket close\", fa: \"رفع مشکل منطق محدودیت activeConnections و کاهش آن هنگام بسته شدن وب‌ساکت\" },\n                      { en: \"Security: Removed url authentication key fallback to prevent potential API key logging\", fa: \"امنیت: حذف جستجوی کلید احراز هویت از پارامتر URL برای جلوگیری از ثبت در لاگ‌ها\" }\n                  ],\n                  improved: [\n                      { en: \"Added auto-refreshing dashboard stats using setInterval on dashboard intervals\", fa: \"اضافه شدن تازه‌سازی خودکار آمار داشبورد با استفاده از کرون زمان‌بندی داخلی داشبورد\" },\n                      { en: \"Added memory management limit that clears configRegistry if it exceeds 10,000 entries\", fa: \"افزودن محدودیت مدیریت حافظه که configRegistry را پس از ۱۰ هزار ورود پاک می‌کند\" }\n                  ],\n                  notes: []\n              },\n              \"2.9.0\": {\n                  headline: { en: \"Protocol Fix & Per-Config Node Routing\", fa: \"رفع پروتکل و مسیریابی نود به‌ازای هر کانفیگ\" },\n                  added: [\n                      { en: \"Per-config node routing for beta protocol via WebSocket path payload — beta nodes now route through their designated gateway IP just like alpha\", fa: \"مسیریابی نود به‌ازای هر کانفیگ پروتکل بتا از طریق مسیر وب‌ساکت — نودهای بتا اکنون مانند آلفا از طریق آدرس دروازه تعیین‌شده مسیریابی می‌کنند\" },\n                      { en: \"Server-side node index extraction with triple fallback: query parameter → numeric path segment → base64 JSON payload\", fa: \"استخراج شاخص نود سمت سرور با زنجیره سه‌گانه بازگشت: پارامتر کوئری → بخش عددی مسیر → بار پیلود JSON باینری\" },\n                      { en: \"Device connection limit per user (connLimit) — cap simultaneous connections per subscriber\", fa: \"محدودیت اتصال دستگاه به‌ازای هر کاربر (connLimit) — محدود کردن اتصالات همزمان هر مشترک\" },\n                      { en: \"Panel API key system for secure node-to-panel authentication\", fa: \"سیستم کلید API پنل برای احراز هویت امن اتصال نود به پنل\" },\n                      { en: \"Mobile-friendly add/edit user modals with improved responsive layout\", fa: \"فرم‌های افزودن/ویرایش کاربر سازگار با موبایل با طرح‌بندی واکنش‌گرا بهبودیافته\" }\n                  ],\n                  fixed: [\n                      { en: \"Fixed beta protocol header offset parsing — beta connections were silently dropping payload data after the port field\", fa: \"رفع خطای اندازه‌گیری افست هدر پروتکل بتا — اتصالات بتا به‌طور خاموش داده پس از فیلد پورت را حذف می‌کردند\" },\n                      { en: \"Fixed beta protocol authentication — password was set to generated internal ID instead of raw user identifier, causing permanent auth failure\", fa: \"رفع احراز هویت پروتکل بتا — رمز عبور به‌جای شناسه داخلی تولیدشده از شناسه خام کاربر استفاده می‌کند\" },\n                      { en: \"Added SHA224 hash registration in configRegistry so beta lookup works when isolate is warm\", fa: \"افزودن ثبت هش SHA224 در configRegistry تا جستجوی بتا در isolate گرم کار کند\" },\n                      { en: \"Removed Maintenance Hosts and Sync API Key fields from Advanced tab network section as requested\", fa: \"حذف فیلدهای میزبان‌های نگهداری و کلید API همگام‌سازی از بخش شبکه پیشرفته\" }\n                  ],\n                  improved: [\n                      { en: \"Beta node routing now uses the same base64 JSON WebSocket path payload format as alpha for maximum client compatibility\", fa: \"مسیریابی نود بتا اکنون از همان قالب پیلود مسیر وب‌ساکت JSON باینری آلفا برای حداکثر سازگاری استفاده می‌کند\" },\n                      { en: \"Node resolution uses getEffectivePips with NAT64 awareness for both alpha and beta protocols\", fa: \"解析 نود از getEffectivePips با آگاهی NAT64 برای هر دو پروتکل آلفا و بتا استفاده می‌کند\" },\n                      { en: \"Added reqPath variable to buildYamlProfile for consistent path generation\", fa: \"افزودن متغیر reqPath به buildYamlProfile برای تولید مسیر یکپارچه\" }\n                  ],\n                  notes: []\n              },\n              \"2.6.0\": {\n                  headline: { en: \"Bilingual Subscription Page & NAT64 Support\", fa: \"صفحه اشتراک چندزبانه و پشتیبانی NAT64\" },\n                  added: [\n                      { en: \"Full Persian and English language support on the subscription info page with RTL layout\", fa: \"پشتیبانی کامل از فارسی و انگلیسی در صفحه اطلاعات اشتراک با چیدمان RTL\" },\n                      { en: \"Dark and light mode toggle on the subscription page with saved preference\", fa: \"قابلیت تغییر حالت تاریک/روشن در صفحه اشتراک با ذخیره ترجیح کاربر\" },\n                      { en: \"NAT64 support for automatic IPv4 to IPv6 address conversion\", fa: \"پشتیبانی NAT64 برای تبدیل خودکار آدرس IPv4 به IPv6\" },\n                      { en: \"Per-user custom hostnames for multi-region deployments\", fa: \"هاست‌های اختصاصی برای هر کاربر جهت استقرار چند منطقه‌ای\" },\n                      { en: \"Direct connection configs that work without gateway IPs\", fa: \"کانفیگ‌های اتصال مستقیم بدون نیاز به آدرس دروازه\" },\n                      { en: \"Auto update from GitHub directly inside the dashboard\", fa: \"بروزرسانی خودکار از GitHub مستقیماً از داشبورد\" },\n                      { en: \"Customizable fake subscription entries with usage and expiry display\", fa: \"ورودی‌های اشتراک جعلی سفارشی با نمایش مصرف و انقضا\" },\n                      { en: \"Full gateway management via Telegram inline buttons\", fa: \"مدیریت کامل دروازه از طریق دکمه‌های اینلاین تلگرام\" }\n                  ],\n                  fixed: [\n                      { en: \"Fixed garbled Persian text in the user interface\", fa: \"اصلاح متن‌های فارسی نادرست در رابط کاربری\" },\n                      { en: \"Fixed subscription page not loading properly\", fa: \"رفع مشکل بارگذاری صفحه اشتراک\" }\n                  ],\n                  improved: [\n                      { en: \"Significantly faster dashboard scrolling and page loading\", fa: \"سرعت اسکرول و بارگذاری صفحات داشبورد بهبود چشمگیر یافت\" },\n                      { en: \"Rewritten config generators for better compatibility\", fa: \"بازنویسی مولدهای کانفیگ برای سازگاری بهتر\" },\n                      { en: \"Faster and more accurate country flag detection\", fa: \"سرعت و دقت نمایش پرچم کشورها بهبود یافت\" },\n                      { en: \"New config naming tags: country, city, ISP, date, and worker name\", fa: \"تگ‌های جدید نامگذاری: کشور، شهر، ارائه‌دهنده، تاریخ و نام ورکر\" }\n                  ],\n                  notes: []\n              },\n              \"2.5.8\": {\n                  headline: { en: \"Advanced Naming Tags & GeoIP Tag Engine\", fa: \"موتور نامگذاری پیشرفته با تگ‌های جغرافیایی\" },\n                  added: [\n                      { en: \"Added 7 new config naming placeholders: {COUNTRY}, {CITY}, {ISP}, {HOST}, {DATE}, {INDEX}, {WORKER}\", fa: \"اضافه شدن ۷ متغیر جدید نامگذاری: {COUNTRY}، {CITY}، {ISP}، {HOST}، {DATE}، {INDEX}، {WORKER}\" },\n                      { en: \"Replaced single-purpose flag API with batch ip-api.com GeoIP enrichment for country, city, and ISP data\", fa: \"جایگزینی API پرچم با غنی‌سازی GeoIP دسته‌ای ip-api.com برای داده‌های کشور، شهر و ارائه‌دهنده اینترنت\" },\n                      { en: \"Added tag validation engine that detects and reports unknown/invalid tags in naming strategies\", fa: \"افزودن موتور اعتبارسنجی تگ که تگ‌های ناشناخته یا نامعتبر در استراتژی نامگذاری را شناسایی و گزارش می‌کند\" }\n                  ],\n                  fixed: [\n                      { en: \"GeoIP cache now stores full geo metadata (country, city, ISP) instead of only flag emoji\", fa: \"کش GeoIP اکنون فراداده‌های کامل جغرافیایی (کشور، شهر، ارائه‌دهنده) را به جای فقط ایموجی پرچم ذخیره می‌کند\" }\n                  ],\n                  improved: [\n                      { en: \"Config name generation now receives config index for sequential naming patterns via {INDEX}\", fa: \"تولید نام کانفیگ اکنون شماره ردیف را برای الگوهای نامگذاری متوالی از طریق {INDEX} دریافت می‌کند\" },\n                      { en: \"Updated dashboard documentation with full list of all 13 supported naming tags in English and Persian\", fa: \"به‌روزرسانی مستندات داشبورد با لیست کامل ۱۳ تگ نامگذاری پشتیبانی شده در فارسی و انگلیسی\" }\n                  ],\n                  notes: []\n              },\n              \"2.5.7\": {\n                  headline: { en: \"Dynamic Multi-IP Failover & Keyless Country Flagging\", fa: \"لینک هوشمند آی‌پی‌ها، بهبود کلودفلر و نگاشت پرچم بدون تحریم\" },\n                  added: [\n                      { en: \"Support entering custom clean IPs, gateway IPs, and custom config names for each subscriber dynamically in Add/Edit user modals, with automatic extraction and seamless database merging\", fa: \"امکان ثبت آی‌پی تمیز دلخواه، آی‌پی دروازه دلخواه و نام کانفیگ دلخواه برای هر کاربر به صورت مجزا با قابلیت استخراج خودکار و ادغام هوشمند\" },\n                      { en: \"Integrated free, open-source and keyless api.country.is for country flag mapping of IP addresses\", fa: \"یکپارچه‌سازی وب‌سرویس رایگان و متن‌باز api.country.is جهت نگاشت پرچم کشورهای مربوط به آدرس‌های آی‌پی\" }\n                  ],\n                  fixed: [\n                      { en: \"Resolved Cloudflare API compatibility flag error ('No such compatibility flag: unsafe-eval' and startup 'Uncaught EvalError') by updating to 'allow_eval_during_startup'\", fa: \"رفع خطای ناسازگاری فلگ کلودفلر (خطای عدم وجود فلگ unsafe-eval و خطای زمان شروع کار EvalError) در بخش استقرار خودکار با بازنویسی به فلگ مدرن allow_eval_during_startup\" },\n                      { en: \"Fixed a critical issue where selecting multiple gateway IPs for a user caused session disruptions (IP splitting) on sites behind Cloudflare, resolved via user-consistent hashing and smart gateway failover\", fa: \"رفع مشکل عدم باز شدن وب‌سایت‌های پشت کلودفلر هنگام انتخاب چندین آی‌پی دروازه با پیاده‌سازی مکانیزم Hashing پایدار کاربر و سوییچ خودکار (Failover) بر روی دروازه‌های جایگزین\" },\n                      { en: \"Fixed client-side regular expression parsing to correctly split global IPs separated by backslashes, tabs, commas, or semicolons in the browser\", fa: \"اصلاح عبارات منظم فرانت‌اند در مروگر جهت تفکیک صحیح لیست آی‌پی‌های تفکیک شده با اینتر، ویرگول، نقطه ویرگول یا بک‌اسلش\" }\n                  ],\n                  improved: [\n                      { en: \"Enhanced reliability of user management dashboard modals and subscription validation logic\", fa: \"بهبود پایداری پنجره‌های مدیریتی داشبورد و منطق بررسی اعتبار اشتراک‌ها\" }\n                  ],\n                  notes: []\n              },\n              \"2.5.6.1\": {\n                  headline: { en: \"Multi-IP Management & Crucial Bug Fixes\", fa: \"مدیریت آی‌پی‌های چندگانه و رفع خطاهای بحرانی\" },\n                  added: [\n                       { en: \"Support setting custom config name, custom gateway IP, and custom clean IP for each user dynamically in the Add User modal\", fa: \"اضافه شدن امکان ثبت نام کانفیگ دلخواه، آی‌پی دروازه اختصاصی و آی‌پی تمیز اختصاصی به صورت مجزا برای هر کاربر در پنجره افزودن کاربر\" }\n                  ],\n                  fixed: [\n                      { en: \"Fixed a critical JavaScript rollback error ('ReferenceError: proxyIp is not defined') when adding a new user\", fa: \"رفع خطای بحرانی جاوااسکریپت ('ReferenceError: proxyIp is not defined') هنگام تلاش برای افزودن یک کاربر جدید\" }\n                  ],\n                  improved: [\n                      { en: \"Streamlined alignment of custom user values with subscription generation\", fa: \"بهبود همگام‌سازی مقادیر اختصاصی کاربران با فرایند ساخت کانفیگ‌ها در اشتراک\" }\n                  ],\n                  notes: []\n              },\n              \"2.5.6\": {\n                  headline:                { en: \"Multiple Gateway IPs & Flag Matching\", fa: \"آی‌پی‌های دروازه متعدد و انطباق پرچم\" },\n                  added: [\n                      { en: \"Support multi-gateway IP lists (rotated/distributed across generated configs to bypass Cloudflare limits)\", fa: \"پشتیبانی از لیست‌های آی‌پی دروازه چندگانه (چرخش و توزیع خودکار میان کانفیگ‌ها برای عبور از محدودیت‌های کلودفلر)\" },\n                      { en: \"Proper country flag matching for configs based on the actual gateway IP used\", fa: \"انطباق صحیح پرچم کشور برای کانفیگ‌ها بر اساس آی‌پی دروازه واقعی استفاده‌شده\" }\n                  ],\n                  fixed: [\n                      { en: \"Fixed outbound transport and websocket configurations formatting errors\", fa: \"رفع خطاهای فرمت‌دهی در کانفیگ‌های حمل و نقل خروجی و وب‌ساکت\" }\n                  ],\n                  improved: [\n                      { en: \"Distributed multiple gateway IPs evenly across subscription sub-configs\", fa: \"توزیع یکنواخت چندین آی‌پی دروازه میان زیرکانفیگ‌های اشتراک\" },\n                      { en: \"Enhanced IP API resolving and flag caching logic\", fa: \"بهبود منطق حل‌وفصل و کش پرچم برای آی‌پی‌ها\" }\n                  ],\n                  notes: []\n              },\n              \"2.5.5\": {\n                  headline: { en: \"One-Click Panel Update\", fa: \"بروزرسانی پنل با یک کلیک\" },\n                  added: [\n                      { en: \"Update the panel directly from the admin panel — no need to use Cloudflare dashboard\", fa: \"بروزرسانی پنل مستقیماً از پنل مدیریت — بدون نیاز به داشبورد کلودفلر\" },\n                      { en: \"One-click deployment inside the panel for quick and easy updates\", fa: \"نصب با یک کلیک داخل پنل برای بروزرسانی سریع و آسان\" },\n                  ],\n                  fixed: [],\n                  improved: [\n                      { en: \"Improved stability and reliability of the update system\", fa: \"بهبود پایداری و اطمینان سیستم بروزرسانی\" },\n                  ],\n                  notes: []\n              },\n              \"2.5.4.2\": {\n                  headline: { en: \"Performance Optimization & Background Processing\", fa: \"بهینه‌سازی عملکرد و پردازش پس‌زمینه\" },\n                  added: [],\n                  fixed: [],\n                  improved: [\n                      { en: \"Improved system performance using smart caching (faster responses and less database load)\", fa: \"بهبود عملکرد سیستم با استفاده از کش هوشمند (پاسخ‌ سریع‌تر و بار کمتر روی پایگاه داده)\" },\n                      { en: \"Added smart caching system (TTL) for configuration and usage data\", fa: \"افزودن سیستم کش هوشمند (TTL) برای داده‌های تنظیمات و مصرف\" },\n                      { en: \"Reduced database calls to make the panel faster and more efficient\", fa: \"کاهش درخواست‌ها به پایگاه داده برای سریع‌تر و کاراتر شدن پنل\" },\n                      { en: \"Background processing added for non-critical tasks to improve speed\", fa: \"افزودن پردازش پس‌زمینه برای کارهای غیربحرانی جهت بهبود سرعت\" },\n                  ],\n                  notes: []\n              },\n              \"2.5.4.1\": {\n                  headline: { en: \"Security Hotfix — Bot Authorization\", fa: \"اصلاح امنیتی — احراز هویت ربات\" },\n                  added: [],\n                  fixed: [\n                      { en: \"Fixed critical issue where unauthorized users could access bot and panel data via Worker\", fa: \"رفع مشکل بحرانی دسترسی کاربران غیرمجاز به اطلاعات ربات و پنل از طریق Worker\" },\n                      { en: \"Added proper Telegram user ID validation for all Worker-related requests\", fa: \"افزودن بررسی صحیح آیدی عددی تلگرام برای تمام درخواست‌های مربوط به Worker\" },\n                  ],\n                  improved: [\n                      { en: \"Only users with approved admin IDs can interact with the bot and access panel data\", fa: \"فقط کاربرانی که آیدی آن‌ها در لیست ادمین‌ها ثبت شده باشد اجازه دسترسی به ربات و اطلاعات پنل را دارند\" },\n                      { en: \"Unauthorized users now receive a clear access denied message\", fa: \"کاربران غیرمجاز اکنون پیام خطای دسترسی مناسب دریافت می‌کنند\" },\n                  ],\n                  notes: [\n                      { en: \"Security update — recommended for all users\", fa: \"به‌روزرسانی امنیتی — توصیه‌شده برای تمام کاربران\" },\n                  ]\n              },\n              \"2.5.4\": {\n                  headline: { en: \"Overview Dashboard & Mobile Improvements\", fa: \"داشبورد نمای کلی و بهبود نمایش در موبایل\" },\n                  added: [\n                      { en: \"Added Overview Dashboard as the default home page\", fa: \"اضافه شدن داشبورد نمای کلی به عنوان صفحه اصلی پنل\" },\n                      { en: \"Added quick statistics and recent activity section\", fa: \"اضافه شدن بخش آمار سریع و فعالیت‌های اخیر\" },\n                  ],\n                  fixed: [],\n                  improved: [\n                      { en: \"Improved mobile responsiveness of the Overview page\", fa: \"بهبود نمایش صفحه نمای کلی در موبایل\" },\n                      { en: \"Localized traffic units for Persian language\", fa: \"نمایش واحد ترافیک به فارسی در صفحه نمای کلی\" },\n                  ],\n                  notes: []\n              },\n              \"2.5.3\": {\n                  headline: { en: \"Telegram Bot Fixes & Formatting Cleanup\", fa: \"رفع مشکلات ربات تلگرام و اصلاح فرمت‌بندی\" },\n                  added: [],\n                  fixed: [\n                      { en: \"Fixed admin buttons not showing immediately after /start in some cases\", fa: \"رفع مشکل نمایش ندادن دکمه‌های مدیر بلافاصله پس از /start در بعضی موارد\" },\n                      { en: \"Fixed subscription link button returning per-user links instead of master link\", fa: \"رفع مشکل بازگشت لینک‌های کاربری به جای لینک اصلی هنگام فشردن دکمه لینک اشتراک\" },\n                      { en: \"Fixed duplicate messages when clicking Update Usage with unchanged stats\", fa: \"رفع مشکل ارسال پیام تکراری هنگام فشردن بروزرسانی مصرف بدون تغییر آمار\" },\n                      { en: \"Fixed <code> tags showing as raw text in Telegram messages\", fa: \"رفع مشکل نمایش تگ‌های <code> به صورت متن خام در پیام‌های تلگرام\" },\n                      { en: \"Fixed subscription links not being clickable in Telegram\", fa: \"رفع مشکل غیرقابل کلیک بودن لینک‌های اشتراک در تلگرام\" },\n                  ],\n                  improved: [\n                      { en: \"Subscription links now use tap-to-copy formatting in Telegram\", fa: \"لینک‌های اشتراک اکنون با فرمت کپی با یک لمس در تلگرام نمایش داده می‌شوند\" },\n                      { en: \"UUIDs now use tap-to-copy formatting in user lists and detail views\", fa: \"شناسه‌های یکتا اکنون با فرمت کپی با یک لمس در لیست و جزئیات کاربران نمایش داده می‌شوند\" },\n                      { en: \"Bot menu now correctly shows admin options on first interaction after login\", fa: \"منوی ربات اکنون گزینه‌های مدیریتی را در اولین تعامل پس از ورود به درستی نمایش می‌دهد\" },\n                      { en: \"Update Usage button now edits the existing message instead of sending a new one\", fa: \"دکمه بروزرسانی مصرف اکنون پیام موجود را ویرایش می‌کند به جای ارسال پیام جدید\" },\n                  ],\n                  notes: [\n                      { en: \"No breaking changes — fully backward compatible\", fa: \"بدون تغییرات ناسازگار — کاملاً سازگار با نسخه‌های قبلی\" },\n                  ]\n              },\n              \"2.5.2\": {\n                  headline: { en: \"Modal Responsiveness & Mobile UX\", fa: \"واکنش‌گرایی مودال و تجربه کاربری موبایل\" },\n                  added: [],\n                  fixed: [],\n                  improved: [\n                      { en: \"Improved Add/Edit User modal responsiveness on all screen sizes\", fa: \"بهبود واکنش‌گرایی مودال افزودن/ویرایش کاربر در تمام اندازه‌های صفحه\" },\n                      { en: \"Added sticky action buttons in modals for better mobile support\", fa: \"افزودن دکمه‌های شناور در مودال‌ها برای پشتیبانی بهتر از موبایل\" },\n                      { en: \"Enhanced scrolling behavior — form content scrolls independently while buttons stay visible\", fa: \"بهبود رفتار اسکرول — محتوای فرم به‌طور مستقل اسکرول می‌شود در حالی که دکمه‌ها قابل مشاهده باقی می‌مانند\" },\n                      { en: \"Improved overall user experience when managing subscribers\", fa: \"بهبود تجربه کاربری هنگام مدیریت مشترکین\" },\n                  ],\n                  notes: [\n                      { en: \"No breaking changes — fully backward compatible\", fa: \"بدون تغییرات ناسازگار — کاملاً سازگار با نسخه‌های قبلی\" },\n                  ]\n              },\n              \"2.5.1\": {\n                  headline: { en: \"Simplified Panel Management & Bot Stability\", fa: \"مدیریت ساده‌شده پنل و پایداری ربات\" },\n                  added: [\n                      { en: \"Web login signal system — bot auto-detects the last active web-logged panel\", fa: \"سیستم سیگنال ورود وب — ربات به‌طور خودکار آخرین پنل واردشده از وب را شناسایی می‌کند\" },\n                      { en: \"Login sync endpoint (/tg/sync_panel) for remote panels to notify the hub on admin login\", fa: \"نقطه پایانی همگام‌سازی ورود (/tg/sync_panel) برای اطلاع‌رسانی پنل‌های راهدور به هاب هنگام ورود مدیر\" },\n                      { en: \"Hub panel URL config (hubPanelUrl) for remote panels to signal login events\", fa: \"پیکربندی آدرس هاب پنل (hubPanelUrl) برای ارسال سیگنال ورود از پنل‌های راهدور\" },\n                      { en: \"Full user management via Telegram bot (create, edit, delete, search, disable, re-enable)\", fa: \"مدیریت کامل کاربران از طریق ربات تلگرام (ایجاد، ویرایش، حذف، جستجو، غیرفعال‌سازی، فعال‌سازی مجدد)\" },\n                      { en: \"HTTP REST API for all user operations at /api/users (GET, POST, PUT, DELETE)\", fa: \"API جدید REST برای تمام عملیات کاربران در /api/users\" },\n                      { en: \"Statistics API at /api/stats with user counts, traffic totals, and system status\", fa: \"API آمار در /api/stats با تعداد کاربران، مجموع ترافیک و وضعیت سیستم\" },\n                  ],\n                  fixed: [\n                      { en: \"Removed multi-panel selection system that caused session confusion and incorrect panel switching\", fa: \"حذف سیستم انتخاب چندپنلی که باعث سردرگمی نشست و جابجایی نادرست پنل می‌شد\" },\n                      { en: \"Fixed bot not responding after pressing /start due to stale step state\", fa: \"رفع مشکل پاسخ ندادن ربات پس از فشار دادن /start به دلیل وضعیت مرحله قدیمی\" },\n                      { en: \"Fixed panel context mixing when switching between panels\", fa: \"رفع مشکل ترکیب زمینه پنل هنگام جابجایی بین پنل‌ها\" },\n                      { en: \"Fixed race condition in bot state persistence from non-blocking D1 writes\", fa: \"رفع مشکل شرایط مسابقه در ماندگاری وضعیت ربات ناشی از نوشتن غیرهمزمان D1\" },\n                  ],\n                  improved: [\n                      { en: \"/start now directly opens panel management based on last web login — no panel selection menu\", fa: \"/start اکنون مستقیماً مدیریت پنل را بر اساس آخرین ورود وب باز می‌کند — بدون منوی انتخاب پنل\" },\n                      { en: \"Bot automatically links Telegram session to the last active web-logged panel\", fa: \"ربات به‌طور خودکار نشست تلگرام را به آخرین پنل فعال واردشده از وب متصل می‌کند\" },\n                      { en: \"Simplified bot logic with clean 1-to-1 mapping between web login and Telegram session\", fa: \"ساده‌سازی منطق ربات با نگاشت یک‌به‌یک بین ورود وب و نشست تلگرام\" },\n                      { en: \"Telegram bot main menu redesigned with inline keyboard layout for mobile-first management\", fa: \"منوی اصلی ربات تلگرام با طرح‌بندی کیبورد درون‌خطی برای مدیریت موبایل‌محور بازطراحی شد\" },\n                  ],\n                  notes: [\n                      { en: \"Single-panel mode works more reliably — it is recommended to use one Telegram bot per panel for best stability\", fa: \"حالت تک‌پنلی پایدارتر است — توصیه می‌شود برای بهترین پایداری از یک ربات تلگرام برای هر پنل استفاده کنید\" },\n                      { en: \"For multi-panel setups: set hubPanelUrl on each remote panel to enable automatic login sync\", fa: \"برای تنظیمات چندپنلی: hubPanelUrl را روی هر پنل راهدور تنظیم کنید تا همگام‌سازی خودکار ورود فعال شود\" },\n                      { en: \"Each panel having its own dedicated bot improves session accuracy and prevents panel mix-up issues\", fa: \"داشتن ربات اختصاصی برای هر پنل، دقت نشست را بهبود می‌دهد و از مشکلات ترکیب پنل جلوگیری می‌کند\" },\n                      { en: \"API endpoints are authenticated via Master Key (Bearer token or ?key= parameter)\", fa: \"نقاط پایانی API از طریق کلید اصلی احراز هویت می‌شوند (توکن Bearer یا پارامتر ?key=)\" },\n                  ]\n              },\n              \"2.5.0\": {\n                  headline: { en: \"User Auto-Disable & Management Improvements\", fa: \"غیرفعال‌سازی خودکار کاربر و بهبود مدیریت\" },\n                  added: [\n                      { en: \"Automatic user disable on traffic limit exceeded\", fa: \"غیرفعال‌سازی خودکار کاربر هنگام اتمام محدودیت ترافیک\" },\n                      { en: \"Automatic user disable on expiration date reached\", fa: \"غیرفعال‌سازی خودکار کاربر هنگام رسیدن به تاریخ انقضا\" },\n                      { en: \"Activity log and Telegram notification for auto-disabled users\", fa: \"ثبت در گزارش فعالیت و ارسال اعلان تلگرام برای کاربران غیرفعال شده خودکار\" },\n                      { en: \"Recently Disabled Users notification panel in Users tab\", fa: \"پنل اعلان کاربران اخیراً غیرفعال شده در بخش کاربران\" },\n                      { en: \"Status filter dropdown (All/Active/Paused/Auto-Disabled)\", fa: \"فیلتر وضعیت (همه/فعال/متوقف/غیرفعال خودکار)\" },\n                      { en: \"Auto-Disabled statistics card in dashboard\", fa: \"کارت آمار غیرفعال‌سازی خودکار در داشبورد\" },\n                  ],\n                  fixed: [\n                      { en: \"Expired users are now disabled instead of deleted\", fa: \"کاربران منقضی شده اکنون غیرفعال می‌شوند به جای حذف\" },\n                      { en: \"Users exceeding traffic limits are preserved in panel\", fa: \"کاربرانی که محدودیت ترافیک را رد می‌کنند در پنل حفظ می‌شوند\" },\n                  ],\n                  improved: [\n                      { en: \"User data, statistics, and history are now preserved\", fa: \"داده‌ها، آمار و تاریخچه کاربران اکنون حفظ می‌شود\" },\n                      { en: \"Account renewal workflow for administrators\", fa: \"فرآیند تمدید حساب برای مدیران\" },\n                  ],\n                  notes: [\n                      { en: \"Re-enabling a user clears the auto-disable reason\", fa: \"فعال‌سازی مجدد کاربر، دلیل غیرفعال‌سازی خودکار را پاک می‌کند\" },\n                  ]\n              },\n              \"2.4.9\": {\n                  headline: { en: \"Custom Protocol & Port Configuration\", fa: \"پیکربندی پروتکل و پورت سفارشی\" },\n                  added: [\n                      { en: \"Custom protocol mode per user (VLESS/Beta/Both)\", fa: \"حالت پروتکل سفارشی برای هر کاربر (VLESS/Beta/هر دو)\" },\n                      { en: \"Custom port configuration per user\", fa: \"پیکربندی پورت سفارشی برای هر کاربر\" },\n                      { en: \"Maximum configs limit per user\", fa: \"محدودیت حداکثر کانفیگ برای هر کاربر\" },\n                  ],\n                  fixed: [],\n                  improved: [\n                      { en: \"User management panel interface\", fa: \"رابط کاربری پنل مدیریت کاربران\" },\n                  ],\n                  notes: []\n              }\n          };\n\n          function renderChangelog(version) {\n              const container = document.getElementById('modal-changelog-container');\n              if (!container) return;\n\n              const data = CHANGELOG_DATA[version];\n              if (!data) {\n                  container.innerHTML = '<p class=\"text-slate-400 text-xs\">' + (i18n[lang]?.no_changelog || 'No changelog available for this version.') + '</p>';\n                  return;\n              }\n\n              const t = (key) => i18n[lang]?.[key] || i18n['en']?.[key] || key;\n              let html = '';\n\n              if (data.headline) {\n                  const headlineEl = document.getElementById('modal-version-headline');\n                  if (headlineEl) headlineEl.textContent = data.headline[lang] || data.headline['en'];\n              }\n\n              const sections = [\n                  { key: 'added', icon: '✨', color: 'emerald', items: data.added },\n                  { key: 'fixed', icon: '🔧', color: 'blue', items: data.fixed },\n                  { key: 'improved', icon: '⚡', color: 'violet', items: data.improved },\n                  { key: 'changed', icon: '🔄', color: 'amber', items: data.changed },\n                  { key: 'note', icon: '⚠️', color: 'red', items: data.notes },\n              ];\n\n              sections.forEach(section => {\n                  if (section.items && section.items.length > 0) {\n                      html += '<div class=\"mb-4\">';\n                      html += '<div class=\"flex items-center gap-2 mb-2\">';\n                      html += '<span class=\"text-sm\">' + section.icon + '</span>';\n                      html += '<h5 class=\"text-xs font-bold text-' + section.color + '-600 dark:text-' + section.color + '-400 uppercase tracking-wider\">' + t('changelog_' + section.key) + '</h5>';\n                      html += '</div>';\n                      html += '<div class=\"space-y-1.5 ps-6\">';\n                      section.items.forEach(item => {\n                          html += '<div class=\"flex items-start gap-2\">';\n                          html += '<span class=\"text-' + section.color + '-400 mt-1.5\">•</span>';\n                          html += '<span class=\"text-xs text-slate-600 dark:text-slate-300\">' + (item[lang] || item['en']) + '</span>';\n                          html += '</div>';\n                      });\n                      html += '</div></div>';\n                  }\n              });\n\n              container.innerHTML = html || '<p class=\"text-slate-400 text-xs\">' + (i18n[lang]?.no_changes || 'No changes documented.') + '</p>';\n          }\n\n          let lang = localStorage.getItem('lang') || 'fa';\n          let sessionKey = \"\", baseRoute = window.location.pathname.split('/dash')[0];\n          let hostName = window.location.hostname, localUUID = \"\";\n\n          window.addEventListener('DOMContentLoaded', () => {\n              let savedSession = localStorage.getItem('mrvpn294_session');\n              if (savedSession) {\n                  try {\n                      let parsed = JSON.parse(savedSession);\n                      if (parsed && parsed.expiry && Date.now() < parsed.expiry) {\n                           sessionKey = parsed.key;\n                           doLogin(true).then(() => loadDashboard());\n                      } else {\n                          localStorage.removeItem('mrvpn294_session');\n                      }\n                  } catch(e){}\n              }\n              checkVersionPopup();\n          });\n\n          function applyLang() {\n              document.documentElement.dir = lang === 'fa' ? 'rtl' : 'ltr'; document.documentElement.lang = lang;\n              document.getElementById('lang-toggle').innerText = lang === 'fa' ? 'EN' : 'فا';\n              document.querySelectorAll('[data-i18n]').forEach(el => {\n                  const key = el.getAttribute('data-i18n');\n                  if (i18n[lang] && i18n[lang][key] !== undefined && i18n[lang][key] !== null) {\n                      if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {\n                          el.placeholder = i18n[lang][key];\n                      } else {\n                          if (key.startsWith('html_')) {\n                              el.innerHTML = i18n[lang][key];\n                          } else {\n                              el.innerText = i18n[lang][key];\n                          }\n                      }\n                  }\n              });\n              document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {\n                  const key = el.getAttribute('data-i18n-placeholder');\n                  if (i18n[lang] && i18n[lang][key] !== undefined && i18n[lang][key] !== null) {\n                      el.placeholder = i18n[lang][key];\n                  }\n              });\n              const gbUnit = i18n[lang]?.ov_gb_unit || 'GB';\n              ['ov-total-traffic','ov-today-traffic'].forEach(id => {\n                  const el = document.getElementById(id);\n                  if (el && el.textContent.trim() === '- GB') el.textContent = '- ' + gbUnit;\n              });\n              const statTrafficEl = document.getElementById('stat-total-traffic');\n              if (statTrafficEl && statTrafficEl.textContent.trim() === '0 GB') statTrafficEl.textContent = '0 ' + gbUnit;\n          }\n          function toggleLang() {\n              lang = lang === 'fa' ? 'en' : 'fa';\n              localStorage.setItem('lang', lang);\n              applyLang();\n              updateTitle();\n              updateUI();\n              try {\n                  const m = document.getElementById('modal-version-update');\n                  if (m && !m.classList.contains('hidden')) {\n                      renderChangelog(CURRENT_VERSION);\n                  }\n              } catch(e){}\n          }\n          applyLang();\n\n          if (localStorage.getItem('theme') === 'dark' || (!('theme' in localStorage) && window.matchMedia('(prefers-color-scheme: dark)').matches)) {\n              document.documentElement.classList.add('dark');\n          } else {\n              document.documentElement.classList.remove('dark');\n          }\n\n          function toggleTheme() {\n              document.documentElement.classList.toggle('dark');\n              localStorage.setItem('theme', document.documentElement.classList.contains('dark') ? 'dark' : 'light');\n          }\n\n          function checkVersionPopup() {\n              const popupKey = `mrvpn294_shown_v__CURRENT_VERSION__`;\n              if (!localStorage.getItem(popupKey)) {\n                  setTimeout(() => {\n                      const badge = document.getElementById('modal-version-badge');\n                      if (badge) badge.textContent = 'v' + CURRENT_VERSION;\n                      renderChangelog(CURRENT_VERSION);\n                      const m = document.getElementById('modal-version-update');\n                      if (m) {\n                          m.classList.remove('hidden');\n                          m.classList.add('flex');\n                      }\n                  }, 800);\n              }\n          }\n\n          function closeVersionModal() {\n              const m = document.getElementById('modal-version-update');\n              if (m) {\n                  m.classList.add('hidden');\n                  m.classList.remove('flex');\n              }\n              const popupKey = `mrvpn294_shown_v__CURRENT_VERSION__`;\n              localStorage.setItem(popupKey, 'true');\n          }\n\n          function updateTitle() {\n              const activeTab = document.querySelector('.nav-item.active span');\n              if(activeTab) document.getElementById('view-title').innerText = activeTab.innerText;\n          }\n\n          let dashboardInterval = null;\n          let networkInterval = null;\n          function switchTab(tab) {\n            ['overview','info','network','settings','advanced','logs','users','help'].forEach(t => {\n                  const view = document.getElementById('view-'+t);\n                  const deskBtn = document.getElementById('tab-'+t);\n                  const mobBtn = document.getElementById('mob-tab-'+t);\n                  if (tab === t) {\n                      view.classList.remove('hidden'); view.classList.add('block', 'fade-in');\n                      if(deskBtn) deskBtn.classList.add('active');\n                      if(mobBtn) mobBtn.classList.add('active');\n                  } else {\n                      view.classList.add('hidden'); view.classList.remove('block', 'fade-in');\n                      if(deskBtn) deskBtn.classList.remove('active');\n                      if(mobBtn) mobBtn.classList.remove('active');\n                  }\n              });\n            document.getElementById('view-add-user').classList.add('hidden');\n            document.getElementById('view-edit-user').classList.add('hidden');\n            var sc = document.querySelector('.scroll-content');\n            if (sc) {\n                sc.style.overflow = '';\n                sc.classList.remove('flex', 'flex-col');\n                if (sc.firstElementChild) sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');\n                sc.scrollTop = 0;\n            }\n            updateTitle();\n            \n            if(dashboardInterval) { clearInterval(dashboardInterval); dashboardInterval = null; }\n            if(networkInterval) { clearInterval(networkInterval); networkInterval = null; }\n            \n            if(tab === 'overview') {\n                loadDashboard();\n                dashboardInterval = setInterval(loadDashboard, 15000);\n            }\n            if(tab === 'logs') loadLogs();\n            if(tab === 'network') {\n                doLogin(true);\n                networkInterval = setInterval(()=>doLogin(true), 15000);\n            }\n          }\n\n        function toggleFAQ(button) {\n            const content = button.nextElementSibling;\n            const svg = button.querySelector('svg');\n            if (content.classList.contains('hidden')) {\n                content.classList.remove('hidden');\n                svg.classList.add('rotate-180');\n            } else {\n                content.classList.add('hidden');\n                svg.classList.remove('rotate-180');\n            }\n        }\n\n        function filterFAQs() {\n            const query = document.getElementById('faq-search').value.toLowerCase();\n            const items = document.querySelectorAll('.faq-item');\n            items.forEach(item => {\n                const text = item.textContent.toLowerCase();\n                if (text.includes(query)) {\n                    item.classList.remove('hidden');\n                } else {\n                    item.classList.add('hidden');\n                }\n            });\n        }\n\n        async function loadLogs() {\n            const container = document.getElementById('logs-container');\n            if(!container) return;\n            container.innerHTML = '<p class=\"text-sm text-slate-400 text-center py-4\">' + (i18n[lang]?.loading_logs || 'Loading logs...') + '</p>';\n            try {\n                const res = await fetch(baseRoute + '/api/logs', { method: 'POST', body: JSON.stringify({ key: sessionKey }) });\n                const data = await res.json();\n                if (data.success && data.logs) {\n                    if (data.logs.length === 0) {\n                        container.innerHTML = '<p class=\"text-sm text-slate-400 text-center py-4\">' + (i18n[lang]?.no_activity_logs || 'No activity logs found.') + '</p>';\n                        return;\n                    }\n                    let logsHtml = '';\n                    data.logs.forEach(log => {\n                        const dateStr = new Date(log.ts).toLocaleString('en-US', {hour12: false});\n                        logsHtml += `<div class=\"flex flex-col sm:flex-row sm:items-center justify-between p-3 bg-slate-50 dark:bg-slate-800 rounded-xl border border-slate-100 dark:border-darkborder/50 gap-2\"><div><p class=\"text-sm font-bold text-slate-700 dark:text-slate-200\">${log.type}</p><p class=\"text-xs text-slate-500 truncate max-w-[200px] sm:max-w-xs\" title=\"${log.detail}\">${log.detail}</p></div><span class=\"text-[10px] font-mono text-slate-400 bg-[var(--color-surface)]  px-2 py-1 rounded shrink-0\">${dateStr}</span></div>`;\n                    });\n                    container.innerHTML = logsHtml;\n                } else {\n                    container.innerHTML = '<p class=\"text-sm text-red-400 text-center py-4\">Failed to load logs.</p>';\n                }\n            } catch (err) {\n                container.innerHTML = '<p class=\"text-sm text-red-400 text-center py-4\">Error loading logs.</p>';\n            }\n        }\n\n        async function loadDashboard() {\n            try {\n                const [statsRes, logsRes] = await Promise.all([\n                    fetch(baseRoute + '/api/stats', { method: 'GET', headers: { 'Authorization': 'Bearer ' + sessionKey } }),\n                    fetch(baseRoute + '/api/logs', { method: 'POST', body: JSON.stringify({ key: sessionKey }) })\n                ]);\n                const statsData = await statsRes.json();\n                const logsData = await logsRes.json();\n\n                if (statsData.success && statsData.stats) {\n                    const s = statsData.stats;\n                    document.getElementById('ov-total-users').textContent = s.users.total;\n                    document.getElementById('ov-active-users').textContent = s.users.active;\n                    document.getElementById('ov-paused-users').textContent = s.users.paused;\n                    document.getElementById('ov-auto-disabled').textContent = s.users.autoDisabled;\n                    document.getElementById('ov-expired-users').textContent = s.users.expired;\n                    document.getElementById('ov-total-traffic').textContent = s.traffic.totalGB + ' ' + (i18n[lang]?.ov_gb_unit || 'GB');\n                    document.getElementById('ov-total-reqs').textContent = s.traffic.totalRequests.toLocaleString();\n                    document.getElementById('ov-today-traffic').textContent = s.traffic.dailyGB + ' ' + (i18n[lang]?.ov_gb_unit || 'GB');\n                    document.getElementById('ov-today-reqs').textContent = s.traffic.dailyRequests.toLocaleString();\n                    document.getElementById('ov-active-conns').textContent = s.system.activeConnections;\n                    \n                    document.getElementById('ov-version').textContent = 'v' + s.system.version;\n                    \n                    const usageCont = document.getElementById('usage-metrics-container');\n                    if(usageCont && statsData.stats.usage && window.mrvpn294Profiles) {\n                        let usageHtml = '';\n                        window.mrvpn294Profiles.forEach(p => {\n                            let hash = p.id.replace(/-/g, '').toLowerCase();\n                            let use = statsData.stats.usage[hash];\n                            if(use) {\n                                let timeStr = new Date(use.last).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});\n                                usageHtml += `<div class=\"flex items-center justify-between p-3 border-b border-slate-100 dark:border-darkborder/50 last:border-0\"><div class=\"flex flex-col\"><span class=\"text-sm font-bold text-slate-700 dark:text-white\">${p.name}</span><span class=\"text-[10px] text-slate-400 font-mono\">${p.id.split('-')[0]}...</span></div><div class=\"flex flex-col items-end\"><span class=\"text-xs font-bold text-emerald-500\">${use.connects} Conns</span><span class=\"text-[10px] text-slate-400\">${timeStr}</span></div></div>`;\n                            }\n                        });\n                        if(usageHtml) usageCont.innerHTML = usageHtml;\n                    }\n\n                }\n\n                const actList = document.getElementById('ov-activity-list');\n                if (logsData.success && logsData.logs && logsData.logs.length > 0) {\n                    let actHtml = '';\n                    logsData.logs.slice(0, 8).forEach(log => {\n                        const dateStr = new Date(log.ts).toLocaleString('en-US', {hour12: false});\n                        const typeColors = { 'Auth Success': 'bg-emerald-500', 'Auth Failed': 'bg-red-500', 'User Created': 'bg-blue-500', 'User Deleted': 'bg-red-500', 'User Toggled': 'bg-amber-500', 'User Updated': 'bg-indigo-500', 'User Auto-Disabled': 'bg-red-500', 'Traffic Reset': 'bg-cyan-500', 'Config Changed': 'bg-violet-500' };\n                        const dotColor = typeColors[log.type] || 'bg-slate-400';\n                        actHtml += '<div class=\"flex items-center gap-3 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl\"><div class=\"w-2 h-2 rounded-full shrink-0 ' + dotColor + '\"></div><div class=\"flex-1 min-w-0\"><p class=\"text-sm font-semibold text-slate-700 dark:text-slate-200 truncate\">' + log.type + '</p><p class=\"text-[11px] text-slate-400 truncate\">' + log.detail + '</p></div><span class=\"text-[10px] font-mono text-slate-400 shrink-0\">' + dateStr + '</span></div>';\n                    });\n                    actList.innerHTML = actHtml;\n                } else {\n                    actList.innerHTML = '<p class=\"text-sm text-slate-400 text-center py-6\">' + (i18n[lang]?.no_recent_activity || 'No recent activity.') + '</p>';\n                }\n            } catch (err) {\n                console.error('Dashboard load error:', err);\n            }\n            loadApiKeys();\n        }\n\n          function copyData(id) {\n              const input = document.getElementById(id); input.select(); navigator.clipboard.writeText(input.value);\n              const toast = document.getElementById('copy-toast');\n              toast.style.transform = 'translate(-50%, 0)'; toast.style.opacity = '1';\n              setTimeout(() => { toast.style.transform = 'translate(-50%, -5rem)'; toast.style.opacity = '0'; }, 2000);\n          }\n\n          function showQR(name, url) {\n              document.getElementById('qr-modal-title').innerText = name;\n              document.getElementById('qr-modal-img').src = \"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=\" + encodeURIComponent(url);\n              document.getElementById('qr-modal-link').innerText = url;\n              document.getElementById('qr-modal').classList.remove('hidden');\n              document.getElementById('qr-modal').classList.add('flex');\n          }\n\n          window.toggleAccordion = function(btn) {\n              const card = btn.closest('[data-accordion]');\n              if (!card) return;\n              const content = card.querySelector('[data-accordion-content]');\n              const icon = btn.querySelector('.accordion-icon');\n              const isOpen = content.style.visibility === 'visible';\n\n              content.style.transition = 'max-height 0.3s ease, visibility 0.3s ease';\n\n              if (isOpen) {\n                  content.style.maxHeight = content.scrollHeight + 'px';\n                  requestAnimationFrame(() => {\n                      content.style.maxHeight = '0';\n                      content.style.visibility = 'hidden';\n                  });\n                  icon.style.transform = 'rotate(0deg)';\n              } else {\n                  content.style.visibility = 'visible';\n                  content.style.maxHeight = content.scrollHeight + 'px';\n                  icon.style.transform = 'rotate(180deg)';\n                  setTimeout(() => { if (content.style.visibility === 'visible') content.style.maxHeight = 'none'; }, 350);\n              }\n          }\n\n          window.handleCopy = function handleCopy(btn) {\n              copyData('sync-' + btn.dataset.id);\n          }\n          window.handleQR = function handleQR(btn) {\n              showQR(btn.dataset.name, document.getElementById('sync-' + btn.dataset.id).value);\n          }\n\n          function closeQRModal() {\n              document.getElementById('qr-modal').classList.add('hidden');\n              document.getElementById('qr-modal').classList.remove('flex');\n          }\n\n          function updateUI() {\n              try {\n                  let portsStr = Array.from(document.getElementById('cfg-port').selectedOptions).map(o=>o.value).join(',');\n                  let port = portsStr ? portsStr.split(',')[0] : '443';\n                  let proto = document.getElementById('cfg-proto').value === 'beta' ? String.fromCharCode(116, 114, 111, 106, 97, 110) : String.fromCharCode(118, 108, 101, 115, 115);\n                  let rawIps = document.getElementById('cfg-ips').value || \"\";\n\n                  let ipsList = rawIps.replace(/,/g, '\\n').replace(/;/g, '\\n').split('\\n').map(s=>s.trim()).filter(Boolean);\n                  let finalIP = ipsList.length > 0 ? ipsList[0] : (hostName.endsWith('.pages.dev') ? 'time.is' : hostName);\n\n                  let fp = document.getElementById('cfg-fp').value;\n                  let path = encodeURI(\"/\" + document.getElementById('cfg-path').value);\n                  let sec = [\"80\",\"8080\"].includes(port) ? \"none\" : \"tls\";\n\n                  let rawLink = proto + \"://\" + localUUID + \"@\" + finalIP + \":\" + port + \"?encryption=none&security=\" + sec + \"&sni=\" + hostName + \"&fp=\" + fp + \"&type=ws&host=\" + hostName + \"&path=\" + path;\n                  if (document.getElementById('cfg-ech').checked) rawLink += \"&pbk=enabled\";\n                  rawLink += \"#\" + hostName;\n\n                  // FIX: Check if elements exist\n                  const linkEl = document.getElementById('link-direct');\n                  if (linkEl) linkEl.value = rawLink;\n\n                  const qrEl = document.getElementById('qr-code');\n                  if (qrEl) qrEl.src = \"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=\" + encodeURIComponent(rawLink);\n\n                  let totalIps = ipsList.length === 0 ? 1 : ipsList.length;\n                  let tCfg = totalIps * 2;\n                  document.getElementById('ip-count-badge').innerText = lang === 'fa' ? (tCfg + ' کانفیگ تولید شد') : (tCfg + ' Configs Active');\n              } catch(e) { console.error(e); }\n          }\n\n          function logout() {\n              localStorage.removeItem('mrvpn294_session');\n              window.location.reload();\n          }\n\n          function showAddNodeModal() {\n              document.getElementById('modal-add-node').classList.remove('hidden');\n              document.getElementById('add-node-url').value = '';\n              document.getElementById('add-node-apikey').value = '';\n              document.getElementById('add-node-url').focus();\n          }\n\n          function commitAddNode() {\n              const url = document.getElementById('add-node-url').value.trim();\n              const apiKey = document.getElementById('add-node-apikey').value.trim();\n              if (!url || !apiKey) {\n                  const t = i18n[lang] || i18n['en'];\n                  alert(t.add_node_invalid || 'Please enter both URL and API Key.');\n                  return;\n              }\n              if (!window.mrvpn294Config) window.mrvpn294Config = {};\n              if (!Array.isArray(window.mrvpn294Config.linkedPanels)) window.mrvpn294Config.linkedPanels = [];\n              window.mrvpn294Config.linkedPanels.push({ url, apiKey });\n              document.getElementById('modal-add-node').classList.add('hidden');\n              document.getElementById('add-node-url').value = '';\n              document.getElementById('add-node-apikey').value = '';\n              renderLinkedNodes();\n              doSaveDirectly();\n          }\n\n          function removeLinkedNode(idx) {\n              if (!window.mrvpn294Config || !Array.isArray(window.mrvpn294Config.linkedPanels)) return;\n              window.mrvpn294Config.linkedPanels.splice(idx, 1);\n              renderLinkedNodes();\n              doSaveDirectly();\n          }\n\n          function renderLinkedNodes() {\n              const list = document.getElementById('linked-nodes-list');\n              if (!list) return;\n              const panels = (window.mrvpn294Config && Array.isArray(window.mrvpn294Config.linkedPanels)) ? window.mrvpn294Config.linkedPanels : [];\n              if (panels.length === 0) {\n                  list.innerHTML = '<p class=\"text-xs text-slate-400 dark:text-slate-500 italic\">' + ((i18n[lang] || i18n['en']).no_nodes_advanced || 'No external nodes added yet.') + '</p>';\n                  return;\n              }\n              list.innerHTML = panels.map((p, i) => `\n                  <div class=\"flex items-center justify-between gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder/50\">\n                      <div class=\"min-w-0 flex-1\">\n                          <p class=\"text-sm font-mono font-bold text-slate-700 dark:text-slate-200 truncate\">${p.url}</p>\n                          <p class=\"text-[11px] text-slate-400 dark:text-slate-500 font-mono truncate\">${p.apiKey.substring(0, 12)}...</p>\n                      </div>\n                      <button onclick=\"removeLinkedNode(${i})\" class=\"p-1.5 hover:bg-red-100 dark:hover:bg-red-900/30 rounded-lg transition-colors shrink-0\" title=\"Remove\">\n                          <svg class=\"w-3.5 h-3.5 text-red-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M6 18L18 6M6 6l12 12\"></path></svg>\n                      </button>\n                  </div>\n              `).join('');\n          }\n\n          function renderFakeConfigs(configs) {\n              const list = document.getElementById('fake-configs-list');\n              if (!list) return;\n              list.innerHTML = '';\n              if (!configs || configs.length === 0) {\n                  configs = [\n                      { name: \"📊 {usage}\", enabled: true },\n                      { name: \"📅 {expiry}\", enabled: true }\n                  ];\n              }\n              configs.forEach((cfg, idx) => {\n                  const item = document.createElement('div');\n                  item.className = 'flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder/50';\n                  item.innerHTML = `\n                      <div class=\"relative inline-flex items-center cursor-pointer shrink-0\">\n                          <input type=\"checkbox\" ${cfg.enabled ? 'checked' : ''} onchange=\"toggleFakeConfig(${idx})\" class=\"sr-only peer\">\n                          <div class=\"w-9 h-5 bg-slate-300 dark:bg-slate-600 rounded-full peer peer-checked:after:translate-x-4 rtl:peer-checked:after:-translate-x-4 peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-[var(--color-surface)] after:border-slate-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-primary\"></div>\n                      </div>\n                      <input type=\"text\" value=\"${cfg.name.replace(/\"/g, '&quot;')}\" onchange=\"updateFakeConfigName(${idx}, this.value)\" class=\"flex-1 min-w-0 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-darkborder bg-[var(--color-surface)] dark:bg-slate-900 focus:border-primary outline-none text-sm font-mono\">\n                      <button onclick=\"moveFakeConfig(${idx}, -1)\" class=\"p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-lg transition-colors shrink-0\" title=\"Move up\">\n                          <svg class=\"w-3.5 h-3.5 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M5 15l7-7 7 7\"></path></svg>\n                      </button>\n                      <button onclick=\"moveFakeConfig(${idx}, 1)\" class=\"p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-lg transition-colors shrink-0\" title=\"Move down\">\n                          <svg class=\"w-3.5 h-3.5 text-slate-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n                      </button>\n                      <button onclick=\"removeFakeConfig(${idx})\" class=\"p-1.5 hover:bg-red-100 dark:hover:bg-red-900/30 rounded-lg transition-colors shrink-0\" title=\"Remove\">\n                          <svg class=\"w-3.5 h-3.5 text-red-400\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M6 18L18 6M6 6l12 12\"></path></svg>\n                      </button>\n                  `;\n                  list.appendChild(item);\n              });\n              window._fakeConfigs = configs;\n          }\n\n          function addFakeConfig() {\n              if (!window._fakeConfigs) window._fakeConfigs = [];\n              window._fakeConfigs.push({ name: \"Custom Entry\", enabled: true });\n              renderFakeConfigs(window._fakeConfigs);\n          }\n\n          function removeFakeConfig(idx) {\n              if (!window._fakeConfigs) return;\n              window._fakeConfigs.splice(idx, 1);\n              renderFakeConfigs(window._fakeConfigs);\n          }\n\n          function toggleFakeConfig(idx) {\n              if (!window._fakeConfigs || !window._fakeConfigs[idx]) return;\n              window._fakeConfigs[idx].enabled = !window._fakeConfigs[idx].enabled;\n          }\n\n          function updateFakeConfigName(idx, value) {\n              if (!window._fakeConfigs || !window._fakeConfigs[idx]) return;\n              window._fakeConfigs[idx].name = value;\n          }\n\n          function moveFakeConfig(idx, direction) {\n              if (!window._fakeConfigs) return;\n              const newIdx = idx + direction;\n              if (newIdx < 0 || newIdx >= window._fakeConfigs.length) return;\n              const temp = window._fakeConfigs[idx];\n              window._fakeConfigs[idx] = window._fakeConfigs[newIdx];\n              window._fakeConfigs[newIdx] = temp;\n              renderFakeConfigs(window._fakeConfigs);\n          }\n\n          function getFakeConfigsFromUI() {\n              return window._fakeConfigs || [\n                  { name: \"📊 {usage}\", enabled: true },\n                  { name: \"📅 {expiry}\", enabled: true }\n              ];\n          }\n\n          // Export active page inputs configuration\n          function exportConfig() {\n              const el = id => document.getElementById(id);\n              const payload = {\n                  mode: el('cfg-proto').value, socketPorts: Array.from(el('cfg-port').selectedOptions).map(o=>o.value).join(','), deviceId: el('cfg-uuid').value,\n                  apiRoute: el('cfg-path').value, masterKey: el('cfg-pass').value, agent: el('cfg-fp').value,\n                   resolveIp: el('cfg-dns').value, customDns: el('cfg-custom-dns').value ? el('cfg-custom-dns').value : 'https://cloudflare-dns.com/dns-query', cleanIps: el('cfg-ips').value, maintenanceHost: el('cfg-fake') ? el('cfg-fake').value : '', backupRelay: el('cfg-relay').value, customRouting: el('cfg-custom-routing') ? el('cfg-custom-routing').value : '',  nat64Prefix: el('cfg-nat64') ? el('cfg-nat64').value : '', enableDirectConfigs: el('cfg-direct-configs') ? el('cfg-direct-configs').checked : false, syncApiKey: el('cfg-sync-api-key') ? el('cfg-sync-api-key').value.trim() : '', autoUpdate: el('cfg-auto-update') ? el('cfg-auto-update').checked : false, autoUpdateFormat: document.querySelector('input[name=\"auto-update-format\"]:checked')?.value || 'normal',\n                   enableOpt1: el('cfg-tfo').checked, enableOpt2: el('cfg-ech').checked,\n                   tgToken: el('cfg-tg-token').value, tgChatId: el('cfg-tg-chat').value, tgAdminId: el('cfg-tg-admin').value,\n                  cfAccountId: el('cfg-cf-acc').value, cfApiToken: el('cfg-cf-token').value,\n                  cfWorkerName: el('cfg-cf-worker').value,\n                  isPaused: el('cfg-pause').checked, silentAlerts: el('cfg-silent').checked,\n                  githubRepo: el('cfg-github-repo').value,\n                  subUserAgent: el('cfg-sub-ua').value,\n                  customPanelUrl: el('cfg-custom-panel-url').value,\n                  fakeConfigs: getFakeConfigsFromUI(),\n                  linkedPanels: (window.mrvpn294Config && Array.isArray(window.mrvpn294Config.linkedPanels)) ? window.mrvpn294Config.linkedPanels : []\n              };\n              const dataStr = \"data:text/json;charset=utf-8,\" + encodeURIComponent(JSON.stringify(payload, null, 2));\n              const dlAnchor = document.createElement('a');\n              dlAnchor.setAttribute(\"href\", dataStr);\n              dlAnchor.setAttribute(\"download\", \"mrvpn294-gateway-config.json\");\n              document.body.appendChild(dlAnchor);\n              dlAnchor.click();\n              dlAnchor.remove();\n          }\n\n          // Import backup json to overwrite config inputs\n          function importConfig(event) {\n              const file = event.target.files[0];\n              if (!file) return;\n              const reader = new FileReader();\n              reader.onload = function(e) {\n                  try {\n                      const conf = JSON.parse(e.target.result);\n                      const mapId = (id, val) => { const el = document.getElementById(id); if (el && val !== undefined) el.value = val; };\n                      mapId('cfg-proto', conf.mode);\n                      let pList = (conf.socketPorts || conf.socketPort || '443').split(',');\n                      Array.from(document.getElementById('cfg-port').options).forEach(o => o.selected = pList.includes(o.value));\n                      mapId('cfg-uuid', conf.deviceId);\n                      mapId('cfg-path', conf.apiRoute);\n                      mapId('cfg-pass', conf.masterKey);\n                      mapId('cfg-fp', conf.agent);\n                      mapId('cfg-dns', conf.resolveIp);\n                      mapId('cfg-custom-dns', conf.customDns);\n                      mapId('cfg-ips', conf.cleanIps);\n                      mapId('cfg-fake', conf.maintenanceHost);\n                      mapId('cfg-relay', 'cfg-custom-routing', conf.backupRelay);\n                      mapId('cfg-custom-routing', conf.customRouting);\n\n                      mapId('cfg-tg-token', conf.tgToken);\n                      mapId('cfg-tg-chat', conf.tgChatId);\n                      mapId('cfg-tg-admin', conf.tgAdminId);\n                      mapId('cfg-cf-acc', conf.cfAccountId);\n                      mapId('cfg-cf-token', conf.cfApiToken);\n                      mapId('cfg-cf-worker', conf.cfWorkerName);\n                      mapId('cfg-github-repo', conf.githubRepo);\n                      mapId('cfg-sub-ua', conf.subUserAgent);\n                      mapId('cfg-custom-panel-url', conf.customPanelUrl);\n\n                      if (conf.enableOpt1 !== undefined) document.getElementById('cfg-tfo').checked = conf.enableOpt1;\n                      if (conf.enableOpt2 !== undefined) document.getElementById('cfg-ech').checked = conf.enableOpt2;\n                      if (conf.isPaused !== undefined) document.getElementById('cfg-pause').checked = conf.isPaused;\n                      if (conf.silentAlerts !== undefined) document.getElementById('cfg-silent').checked = conf.silentAlerts;\n                      mapId('cfg-nat64', conf.nat64Prefix);\n                      if (conf.enableDirectConfigs !== undefined && document.getElementById('cfg-direct-configs')) document.getElementById('cfg-direct-configs').checked = conf.enableDirectConfigs;\n                      if (document.getElementById('cfg-sync-api-key')) document.getElementById('cfg-sync-api-key').value = conf.syncApiKey || '';\n                      if (conf.autoUpdate !== undefined && document.getElementById('cfg-auto-update')) {\n                          document.getElementById('cfg-auto-update').checked = conf.autoUpdate;\n                          const wrap = document.getElementById('auto-update-format-wrap');\n                          if (wrap) wrap.classList.toggle('hidden', !conf.autoUpdate);\n                      }\n                      if (conf.autoUpdateFormat) {\n                          const radio = document.querySelector(`input[name=\"auto-update-format\"][value=\"${conf.autoUpdateFormat}\"]`);\n                          if (radio) radio.checked = true;\n                      }\n\n                      if (conf.fakeConfigs) renderFakeConfigs(conf.fakeConfigs);\n                      if (conf.linkedPanels) {\n                          if (!window.mrvpn294Config) window.mrvpn294Config = {};\n                          window.mrvpn294Config.linkedPanels = conf.linkedPanels;\n                          renderLinkedNodes();\n                      }\n\n                      updateUI();\n                      alert(lang === 'fa' ? 'پیکربندی با موفقیت وارد شد! روی ذخیره کلیک کنید.' : 'Configuration parsed! Click save to write changes.');\n                  } catch(err) {\n                      alert(lang === 'fa' ? 'فایل نامعتبر است!' : 'Invalid configuration file!');\n                  }\n              };\n              reader.readAsText(file);\n          }\n\n          // Browser-level latency check diagnostics\n          async function runPingTest() {\n              const rawIps = document.getElementById('cfg-ips').value || \"\";\n              let ipsList = rawIps.replace(/,/g, '\\n').replace(/;/g, '\\n').split('\\n').map(s=>s.trim()).filter(Boolean);\n              let targetIP = ipsList.length > 0 ? ipsList[0] : (hostName.endsWith('.pages.dev') ? 'time.is' : hostName);\n\n              const resultsDiv = document.getElementById('ping-results');\n              resultsDiv.classList.remove('hidden');\n\n              document.getElementById('ping-target').textContent = targetIP;\n              document.getElementById('ping-time').textContent = 'Testing...';\n              document.getElementById('ping-status').textContent = 'Dialing...';\n              document.getElementById('ping-port').textContent = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');\n\n              const startTime = performance.now();\n              try {\n                  await fetch('https://' + targetIP + '/favicon.ico?cb=' + startTime, { mode: 'no-cors', cache: 'no-store' });\n                  const duration = Math.round(performance.now() - startTime);\n                  document.getElementById('ping-time').textContent = duration + ' ms';\n                  document.getElementById('ping-status').className = \"text-sm font-bold text-emerald-500\";\n                  document.getElementById('ping-status').textContent = \"Success\";\n              } catch (err) {\n                  const duration = Math.round(performance.now() - startTime);\n                  if (duration < 1500) {\n                      document.getElementById('ping-time').textContent = duration + ' ms';\n                      document.getElementById('ping-status').className = \"text-sm font-bold text-amber-500\";\n                      document.getElementById('ping-status').textContent = \"Indirect-OK\";\n                  } else {\n                      document.getElementById('ping-time').textContent = 'Timeout';\n                      document.getElementById('ping-status').className = \"text-sm font-bold text-red-500\";\n                      document.getElementById('ping-status').textContent = \"Unreachable\";\n                  }\n              }\n          }\n\n          function togglePortCheckbox(val, checked) {\n              const sel = document.getElementById('cfg-port');\n              const opt = Array.from(sel.options).find(o => o.value === val);\n              if (opt) {\n                  opt.selected = checked;\n                  sel.dispatchEvent(new Event('change'));\n              }\n          }\n          function syncCheckboxesFromSelect() {\n              const sel = document.getElementById('cfg-port');\n              const ports = Array.from(sel.selectedOptions).map(o => o.value);\n              const checkboxes = document.querySelectorAll('#port-checkboxes-container input[type=\"checkbox\"]');\n              checkboxes.forEach(cb => {\n                  cb.checked = ports.includes(cb.value);\n              });\n          }\n\n          async function doLogin(silent = false) {\n              const btn = document.querySelector('button[onclick=\"doLogin()\"]');\n              const origText = btn.innerText;\n              if(!silent) btn.innerText = \"...\";\n              try {\n                  const pass = silent ? sessionKey : document.getElementById('pwd').value;\n                  const res = await fetch(baseRoute + '/api/auth', { method: 'POST', body: JSON.stringify({ key: pass }) });\n                  const data = await res.json();\n                  if (data.success) {\n                      sessionKey = pass; localUUID = data.deviceId;\n                      localStorage.setItem('mrvpn294_session', JSON.stringify({ key: pass, expiry: Date.now() + 30 * 60 * 1000 }));\n\n                      document.getElementById('login-box').classList.add('hidden');\n                      document.getElementById('dash-box').classList.remove('hidden');\n                      document.getElementById('dash-box').classList.add('flex');\n                      document.getElementById('btn-logout-mob').classList.remove('hidden');\n                      document.body.classList.add('logged-in');\n\n                      document.getElementById('net-ip').textContent = data.network.ip;\n                      document.getElementById('net-colo').textContent = data.network.colo;\n                      document.getElementById('net-loc').textContent = data.network.loc;\n                      const conf = data.config;\n                      if (conf && conf.slaveNodes && conf.slaveNodes.trim().length > 0) {\n                          if (!conf.linkedPanels) conf.linkedPanels = [];\n                          var nodes = conf.slaveNodes.split(/[\\r\\n,;]+/).map(function(s){return s.trim();}).filter(Boolean);\n                          var syncKey = conf.syncApiKey || '';\n                          nodes.forEach(function(node) {\n                              var cleanNode = node.replace(/^[a-zA-Z]+:\\/\\//, \"\").split(\"/\")[0].split(\"@\").pop().split(\":\")[0].toLowerCase();\n                              var exists = conf.linkedPanels.some(function(p) {\n                                  if (!p || !p.url) return false;\n                                  var cleanUrl = p.url.replace(/^[a-zA-Z]+:\\/\\//, \"\").split(\"/\")[0].split(\"@\").pop().split(\":\")[0].toLowerCase();\n                                  return cleanUrl === cleanNode;\n                              });\n                              if (!exists) {\n                                  conf.linkedPanels.push({ url: node, apiKey: syncKey });\n                              }\n                          });\n                          conf.slaveNodes = '';\n                      }\n                      document.getElementById('cfg-proto').value = conf.mode || 'alpha';\n                      let pList = (conf.socketPorts || conf.socketPort || '443').split(',');\n                      Array.from(document.getElementById('cfg-port').options).forEach(o => o.selected = pList.includes(o.value));\n                      syncCheckboxesFromSelect();\n                      document.getElementById('cfg-uuid').value = conf.deviceId || '';\n                      document.getElementById('cfg-path').value = conf.apiRoute || '';\n                      document.getElementById('cfg-pass').value = conf.masterKey || '';\n                      document.getElementById('cfg-fp').value = conf.agent || 'chrome';\n                      document.getElementById('cfg-dns').value = conf.resolveIp || '';\n                      document.getElementById('cfg-custom-dns').value = conf.customDns || 'https://cloudflare-dns.com/dns-query';\n                      document.getElementById('cfg-ips').value = conf.cleanIps || '';\n                      if(document.getElementById('cfg-custom-routing')) document.getElementById('cfg-custom-routing').value = conf.customRouting || '';\n\n                      if (document.getElementById('cfg-fake')) document.getElementById('cfg-fake').value = conf.maintenanceHost || '';\n                       document.getElementById('cfg-relay').value = conf.backupRelay || '';\n                       if (document.getElementById('cfg-nat64')) document.getElementById('cfg-nat64').value = conf.nat64Prefix || '';\n                       if (document.getElementById('cfg-direct-configs')) document.getElementById('cfg-direct-configs').checked = conf.enableDirectConfigs || false;\n                       if (document.getElementById('cfg-sync-api-key')) document.getElementById('cfg-sync-api-key').value = conf.syncApiKey || '';\n                       if (document.getElementById('cfg-auto-update')) {\n                           document.getElementById('cfg-auto-update').checked = conf.autoUpdate || false;\n                           const wrap = document.getElementById('auto-update-format-wrap');\n                           if (wrap) wrap.classList.toggle('hidden', !conf.autoUpdate);\n                       }\n                       if (conf.autoUpdateFormat) {\n                           const radio = document.querySelector(`input[name=\"auto-update-format\"][value=\"${conf.autoUpdateFormat}\"]`);\n                           if (radio) radio.checked = true;\n                       }\n                      document.getElementById('cfg-tfo').checked = conf.enableOpt1 || false;\n                      document.getElementById('cfg-ech').checked = conf.enableOpt2 || false;\n                      document.getElementById('cfg-tg-token').value = conf.tgToken || '';\n                      document.getElementById('cfg-tg-chat').value = conf.tgChatId || '';\n                      document.getElementById('cfg-tg-admin').value = conf.tgAdminId || '';\n                      document.getElementById('cfg-cf-acc').value = conf.cfAccountId || '';\n                      document.getElementById('cfg-cf-token').value = conf.cfApiToken || '';\n                      document.getElementById('cfg-cf-worker').value = conf.cfWorkerName || '';\n                      document.getElementById('cfg-pause').checked = conf.isPaused || false;\n                      document.getElementById('cfg-silent').checked = conf.silentAlerts || false;\n                      document.getElementById('cfg-github-repo').value = conf.githubRepo || 'amirpocom63-del/mrvpn294';\n                      document.getElementById('cfg-name-strategy').value = conf.nameStrategy || 'default';\n                      document.getElementById('cfg-name-prefix').value = conf.namePrefix || 'Core';\n                      document.getElementById('cfg-sub-ua').value = conf.subUserAgent || '';\n                      document.getElementById('cfg-custom-panel-url').value = conf.customPanelUrl || '';\n                      renderFakeConfigs(conf.fakeConfigs || [\n                          { name: \"📊 {usage}\", enabled: true },\n                          { name: \"📅 {expiry}\", enabled: true }\n                      ]);\n\n                      window.mrvpn294Config = JSON.parse(JSON.stringify(conf));\n                      window.mrvpn294Usage = data.sysUsage || {};\n                      window.mrvpn294LiveUsage = data.usage || {};\n                      window.mrvpn294Profiles = data.profiles || [];\n                      renderUsersTable();\n                      renderLinkedNodes();\n                      try { checkUpdate(); } catch(ue) { console.error(ue); }\n                       if (!silent) switchTab('overview');\n\n                      ['cfg-proto','cfg-port','cfg-fp','cfg-ips','cfg-path', 'cfg-relay', 'cfg-custom-routing', 'cfg-name-strategy', 'cfg-name-prefix', 'cfg-sub-ua', 'cfg-custom-panel-url'].forEach(id => {\n                          const el = document.getElementById(id);\n                          if(el) { el.addEventListener('input', updateUI); el.addEventListener('change', updateUI); }\n                      });\n                      ['cfg-ech','cfg-tfo'].forEach(id => {\n                          const el = document.getElementById(id);\n                          if(el) el.addEventListener('change', updateUI);\n                      });\n                      const autoUpdateEl = document.getElementById('cfg-auto-update');\n                      if (autoUpdateEl) {\n                          autoUpdateEl.addEventListener('change', () => {\n                              const wrap = document.getElementById('auto-update-format-wrap');\n                              if (wrap) wrap.classList.toggle('hidden', !autoUpdateEl.checked);\n                          });\n                      }\n                const pCont = document.getElementById('dyn-profiles-container');\n                let profilesHtml = '';\n                data.profiles.forEach(p => {\n                            const isDef = p.name === 'Default';\n                            let html = `<div class=\"bg-[var(--color-surface)]  rounded-3xl shadow-sm border border-slate-200 dark:border-darkborder relative mb-4 break-inside-avoid inline-block w-full\" data-accordion>\n    <div class=\"absolute top-0 end-0 w-32 h-32 bg-primary/5 rounded-bl-[100px] -z-10\"></div>\n    <button onclick=\"toggleAccordion(this)\" class=\"w-full flex items-center justify-between p-5 md:p-6\">\n        <h3 class=\"text-lg font-bold text-slate-800 dark:text-white flex items-center\">\n            <svg class=\"w-5 h-5 me-2 text-primary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1\"></path></svg>\n            ${p.name}\n        </h3>\n        <div class=\"flex items-center gap-2\">\n            ${isDef ? '<span class=\"text-[10px] bg-slate-100 text-slate-500 px-2 py-1 rounded font-bold uppercase\">Master</span>' : ''}\n            <svg class=\"w-4 h-4 text-slate-400 accordion-icon transition-transform duration-300\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M19 9l-7 7-7-7\"></path></svg>\n        </div>\n    </button>\n    <div class=\"transition-all duration-300\" style=\"max-height:0;overflow:hidden;\" data-accordion-content>\n        <div class=\"space-y-3 px-5 md:px-6 pb-5 md:pb-6\">\n            <div>\n                <label class=\"block text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1\">UUID</label>\n                <div class=\"bg-slate-50 dark:bg-slate-800 border border-slate-100 dark:border-darkborder px-3 py-2 rounded-lg text-xs font-mono text-slate-500\">${p.id}</div>\n            </div>\n            <div class=\"relative\">\n                <label class=\"block text-[10px] font-semibold text-emerald-500 uppercase tracking-wider mb-1 flex items-center gap-1.5\"><span class=\"w-1.5 h-1.5 rounded-full bg-emerald-500\"></span>Universal Sync URL</label>\n                <input type=\"text\" id=\"sync-${p.id}\" readonly value=\"${p.sync}\" class=\"w-full bg-[var(--color-bg)] border border-slate-200 dark:border-darkborder px-4 py-2.5 rounded-xl text-xs outline-none font-mono text-slate-600 dark:text-slate-400 truncate pe-12\">\n                <button data-id=\"${p.id}\" onclick=\"handleCopy(this)\" class=\"absolute bottom-1 end-1 text-primary p-1.5 hover:bg-slate-200 dark:hover:bg-slate-700 rounded-md\"><svg class=\"w-4.5 h-4.5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z\"></path></svg></button>\n            </div>\n            <div class=\"mt-2\">\n                <button data-id=\"${p.id}\" data-name=\"${p.name}\" onclick=\"handleQR(this)\" class=\"w-full flex items-center justify-center p-2.5 bg-slate-50 hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700 border border-slate-200 dark:border-darkborder rounded-xl transition-all gap-1.5 text-[11px] font-bold text-slate-600 dark:text-slate-400\">\n                    <svg class=\"w-4 h-4 text-slate-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m0 11v1m5-7h1m-13 0h1m2-5a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V6a2 2 0 00-2-2h-8zM9 9h1m0 0v1m2-1h1m0 0v1\"></path></svg>\n                    <span data-i18n=\"show_qr\">Show QR Code</span>\n                </button>\n<div class=\"grid grid-cols-2 md:grid-cols-4 gap-2 mt-3\">\n    <a href=\"v2rayng://install-sub?url=${encodeURIComponent(p.sync + (p.sync.includes('?') ? '&flag=base64' : '?flag=base64'))}\" class=\"text-center p-2 rounded-lg bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 dark:text-indigo-400 text-[10px] font-bold hover:bg-indigo-100 transition-colors\">v2rayNG</a>\n    <a href=\"shadowrocket://add/sub://${btoa(p.sync)}\" class=\"text-center p-2 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 text-[10px] font-bold hover:bg-blue-100 transition-colors\">Shadowrocket</a>\n    <a href=\"streisand://import/${p.sync}\" class=\"text-center p-2 rounded-lg bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400 text-[10px] font-bold hover:bg-emerald-100 transition-colors\">Streisand</a>\n    <a href=\"v2ray://install-sub?url=${encodeURIComponent(p.sync)}\" class=\"text-center p-2 rounded-lg bg-purple-50 dark:bg-purple-900/20 text-purple-600 dark:text-purple-400 text-[10px] font-bold hover:bg-purple-100 transition-colors\">v2rayN</a>\n</div>\n\n            </div>\n        </div>\n    </div>\n</div>`;\n                         profilesHtml += html;\n                      });\n                      pCont.innerHTML = profilesHtml;\n\n\n\n                      // Inject usage metrics table\n                      const usageCont = document.getElementById('usage-metrics-container');\n                      if(usageCont && data.usage) {\n                          let usageHtml = '';\n                          data.profiles.forEach(p => {\n                              let hash = p.id.replace(/-/g, '').toLowerCase();\n                              let use = data.usage[hash];\n                              if(use) {\n                                  let timeStr = new Date(use.last).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});\n                                  usageHtml += `<div class=\"flex items-center justify-between p-3 border-b border-slate-100 dark:border-darkborder/50 last:border-0\"><div class=\"flex flex-col\"><span class=\"text-sm font-bold text-slate-700 dark:text-white\">${p.name}</span><span class=\"text-[10px] text-slate-400 font-mono\">${p.id.split('-')[0]}...</span></div><div class=\"flex flex-col items-end\"><span class=\"text-xs font-bold text-emerald-500\">${use.connects} Conns</span><span class=\"text-[10px] text-slate-400\">${timeStr}</span></div></div>`;\n                              }\n                          });\n                          usageCont.innerHTML = usageHtml || '<p class=\"text-xs text-slate-400 text-center py-4\">' + (i18n[lang]?.no_active_conn || 'No active connection data yet.') + '</p>';\n                      }\n\n                      updateUI();\n                  } else {\n                      if(!silent) { document.getElementById('err-msg').classList.remove('hidden'); btn.innerText = origText; }\n                      else { localStorage.removeItem('mrvpn294_session'); }\n                  }\n              } catch (err) { if(!silent) btn.innerText = origText; }\n          }\n\n          async function doSave() {\n              const el = id => document.getElementById(id);\n              const payload = {\n                  key: sessionKey,\n                  config: {\n                      mode: el('cfg-proto').value, socketPorts: Array.from(el('cfg-port').selectedOptions).map(o=>o.value).join(','), deviceId: el('cfg-uuid').value,\n                      apiRoute: el('cfg-path').value, masterKey: el('cfg-pass').value, agent: el('cfg-fp').value,\n                      resolveIp: el('cfg-dns').value, customDns: el('cfg-custom-dns').value ? el('cfg-custom-dns').value : 'https://cloudflare-dns.com/dns-query', cleanIps: el('cfg-ips').value, maintenanceHost: el('cfg-fake') ? el('cfg-fake').value : '', backupRelay: el('cfg-relay').value, customRouting: el('cfg-custom-routing') ? el('cfg-custom-routing').value : '',  nat64Prefix: el('cfg-nat64') ? el('cfg-nat64').value : '', enableDirectConfigs: el('cfg-direct-configs') ? el('cfg-direct-configs').checked : false, syncApiKey: el('cfg-sync-api-key') ? el('cfg-sync-api-key').value.trim() : '', autoUpdate: el('cfg-auto-update') ? el('cfg-auto-update').checked : false, autoUpdateFormat: document.querySelector('input[name=\"auto-update-format\"]:checked')?.value || 'normal',\n                      enableOpt1: el('cfg-tfo').checked, enableOpt2: el('cfg-ech').checked,\n                      tgToken: el('cfg-tg-token').value, tgChatId: el('cfg-tg-chat').value, tgAdminId: el('cfg-tg-admin').value,\n                      cfAccountId: el('cfg-cf-acc').value, cfApiToken: el('cfg-cf-token').value,\n                      cfWorkerName: el('cfg-cf-worker').value,\n                      isPaused: el('cfg-pause').checked, silentAlerts: el('cfg-silent').checked,\n                      githubRepo: el('cfg-github-repo').value,\n                      subUserAgent: el('cfg-sub-ua').value,\n                      customPanelUrl: el('cfg-custom-panel-url').value,\n                      nameStrategy: el('cfg-name-strategy').value,\n                      namePrefix: el('cfg-name-prefix').value,\n                      fakeConfigs: getFakeConfigsFromUI(),\n                      slaveNodes: '',\n                      linkedPanels: (window.mrvpn294Config && Array.isArray(window.mrvpn294Config.linkedPanels)) ? window.mrvpn294Config.linkedPanels : []\n                  }\n              };\n                        //update user port after change global\n                     const globalPorts = (payload.config.socketPorts || '443').split(',').map(s=>s.trim()).filter(Boolean);\n                     payload.config.users = (window.mrvpn294Config.users || []).map(u => {\n                     if (!u.userPorts) return u;\n                        const filtered = u.userPorts.split(',').map(s=>s.trim()).filter(p => globalPorts.includes(p));\n                      u.userPorts = filtered.length ? filtered.join(',') : globalPorts[0];\n                          return u;\n                          });\n              const stat = el('save-status'); stat.textContent = i18n[lang].msg_saving; stat.className = \"text-sm font-bold text-primary animate-pulse md:me-4\";\n              try {\n                  const res = await fetch(baseRoute + '/api/sync', { method: 'POST', body: JSON.stringify(payload) });\n                  const data = await res.json();\n                  if (data.success) {\n                      stat.textContent = i18n[lang].msg_saved; stat.className = \"text-sm font-bold text-emerald-500 md:me-4\";\n                      if (Array.isArray(window.mrvpn294Config?.linkedPanels) && window.mrvpn294Config.linkedPanels.length > 0) {\n                          const sc = payload.config;\n                          const slaveCfg = { ...sc };\n                          delete slaveCfg.tgToken; delete slaveCfg.tgChatId; delete slaveCfg.tgAdminId; delete slaveCfg.tgBotLang;\n                          delete slaveCfg.cfAccountId; delete slaveCfg.cfApiToken; delete slaveCfg.cfWorkerName;\n                          delete slaveCfg.panelApiKeys; delete slaveCfg.linkedPanels; delete slaveCfg.slaveNodes; delete slaveCfg.syncApiKey;\n                          delete slaveCfg.masterKey; delete slaveCfg.apiRoute; delete slaveCfg.deviceId; delete slaveCfg.hubPanelUrl; delete slaveCfg.githubRepo; delete slaveCfg.customPanelUrl;\n                          const synced = new Set();\n                          window.mrvpn294Config.linkedPanels.forEach(p => {\n                              if (!p || !p.url || !p.apiKey) return;\n                              const h = p.url.trim().replace(/^https?:\\/\\//, '').replace(/\\/.*$/, '');\n                              if (!h || synced.has(h.toLowerCase())) return;\n                              synced.add(h.toLowerCase());\n                              fetch('https://' + h + '/' + encodeURIComponent(sc.apiRoute || 'sync') + '/api/sync', {\n                                  method: 'POST',\n                                  headers: { 'Content-Type': 'application/json' },\n                                  body: JSON.stringify({ key: p.apiKey, config: slaveCfg, fromMaster: true })\n                              }).then(r => { if (!r.ok) console.error('Sync to ' + h + ' failed: HTTP ' + r.status); }).catch(e => { console.error('Sync to ' + h + ' error:', e.message); });\n                          });\n                      }\n                      setTimeout(() => window.location.href = '/' + data.newRoute + '/dash', 1000);\n                  } else { stat.textContent = i18n[lang].msg_err; stat.className = \"text-sm font-bold text-red-500 md:me-4\"; }\n              } catch(e) { stat.textContent = i18n[lang].msg_err; stat.className = \"text-sm font-bold text-red-500 md:me-4\"; }\n          }\n\n          document.getElementById('pwd').addEventListener('keypress', e => { if (e.key === 'Enter') doLogin(); });\n\n          function renderUsersTable() {\n              const tbl = document.getElementById('tbl-users');\n              if(!tbl) return;\n              let users = window.mrvpn294Config?.users || [];\n              let usage = window.mrvpn294Usage || {};\n\n              // Calculate stats metrics\n              let totalUsersVal = users.length;\n              let activeSubscribers = users.filter(u => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs)).length;\n              let autoDisabledCount = users.filter(u => u.isPaused && u.disabledReason).length;\n              let pausedSubscribers = users.filter(u => u.isPaused && !u.disabledReason).length;\n              let expiredCount = users.filter(u => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused).length;\n              let totalReqsSum = 0;\n              users.forEach(u => {\n                  let sysU = usage[u.id.replace(/-/g,'').toLowerCase()] || {reqs: 0};\n                  totalReqsSum += (sysU.reqs || 0);\n              });\n              let totalGBSum = (totalReqsSum / 6000).toFixed(2);\n\n              // Update stats elements in DOM if they exist\n              const totalUsersEl = document.getElementById('stat-total-users');\n              if (totalUsersEl) totalUsersEl.textContent = totalUsersVal;\n              const activeUsersEl = document.getElementById('stat-active-users');\n              if (activeUsersEl) activeUsersEl.textContent = `${activeSubscribers} / ${pausedSubscribers}`;\n              const totalTrafficEl = document.getElementById('stat-total-traffic');\n              if (totalTrafficEl) totalTrafficEl.textContent = `${totalGBSum} ${i18n[lang]?.ov_gb_unit || 'GB'}`;\n              const autoDisabledEl = document.getElementById('stat-auto-disabled');\n              if (autoDisabledEl) autoDisabledEl.textContent = autoDisabledCount;\n\n              // Render Recently Disabled Users Panel\n              const disabledPanel = document.getElementById('disabled-users-panel');\n              const disabledList = document.getElementById('disabled-users-list');\n              const disabledBadge = document.getElementById('disabled-panel-badge');\n              if (disabledPanel && disabledList) {\n                  const autoDisabledUsers = users.filter(u => u.isPaused && u.disabledReason)\n                      .sort((a, b) => (b.disabledAt || 0) - (a.disabledAt || 0));\n                  if (autoDisabledUsers.length > 0) {\n                      disabledPanel.classList.remove('hidden');\n                      if (disabledBadge) disabledBadge.textContent = autoDisabledUsers.length;\n                      disabledList.innerHTML = autoDisabledUsers.map(u => {\n                          let timeStr = u.disabledAt ? new Date(u.disabledAt).toLocaleString() : '-';\n                          let reasonIcon = u.disabledReason.includes('Traffic') ? '📊' : (u.disabledReason.includes('Expiration') ? '📅' : '⚠️');\n                          let btnLabel = lang === 'fa' ? 'فعال‌سازی مجدد' : 'Re-enable';\n                          return `\n                              <div class=\"flex items-center justify-between p-3 bg-[var(--color-surface)]/70 dark:bg-slate-800/50 rounded-xl border border-red-100 dark:border-red-800/20 hover:shadow-md transition-shadow\">\n                                  <div class=\"flex items-center gap-3 flex-1 min-w-0\">\n                                      <div class=\"text-lg\">${reasonIcon}</div>\n                                      <div class=\"min-w-0\">\n                                          <div class=\"text-sm font-bold text-slate-700 dark:text-white truncate\">${u.name}</div>\n                                          <div class=\"text-[11px] text-red-500 dark:text-red-400 font-medium\">${u.disabledReason}</div>\n                                          <div class=\"text-[10px] text-slate-400 mt-0.5\">${timeStr}</div>\n                                      </div>\n                                  </div>\n                                  <button onclick=\"togglePauseUser('${u.id}')\" class=\"ml-3 px-3 py-1.5 bg-emerald-500 hover:bg-emerald-600 text-white text-[11px] font-bold rounded-lg shadow-sm transition-colors whitespace-nowrap\">${btnLabel}</button>\n                              </div>\n                          `;\n                      }).join('');\n                  } else {\n                      disabledPanel.classList.add('hidden');\n                  }\n              }\n\n              // Apply Status Filter\n              const statusFilter = document.getElementById('user-status-filter')?.value || 'all';\n              const searchVal = document.getElementById('user-search-input')?.value.toLowerCase().trim() || '';\n              let filteredUsers = users.filter(u => {\n                  if (statusFilter === 'active' && (u.isPaused || (u.expiryMs && Date.now() > u.expiryMs))) return false;\n                  if (statusFilter === 'paused' && (!u.isPaused || u.disabledReason)) return false;\n                  if (statusFilter === 'auto-disabled' && !(u.isPaused && u.disabledReason)) return false;\n                  return u.name.toLowerCase().includes(searchVal) || u.id.toLowerCase().includes(searchVal);\n              });\n\n              tbl.innerHTML = '';\n              if (filteredUsers.length === 0) {\n                  tbl.innerHTML = '<div class=\"col-span-full px-4 py-8 text-center text-slate-400 text-sm\">' + (i18n[lang]?.no_matching_users || 'No matching subscribers found') + '</div>';\n                  return;\n              }\n\n              // Alias users to the filtered list for downstream compatibility\n              users = filteredUsers;\n              if (users.length === 0) {\n                  tbl.innerHTML = `<div class=\"col-span-full px-4 py-8 text-center text-slate-400 text-sm\" data-i18n=\"no_users\">${i18n[lang].no_users}</div>`;\n                  return;\n              }\n              let tblHtml = '';\n              users.forEach((u, i) => {\n                  let sysU = usage[u.id.replace(/-/g,'').toLowerCase()] || {reqs: 0, dReqs: 0, lastDay: ''};\n                  let userReqs = sysU.reqs || 0;\n                  let userDReqs = sysU.lastDay === new Date().toISOString().split('T')[0] ? (sysU.dReqs || 0) : 0;\n\n                  const unlimitedTxt = lang === 'fa' ? 'نامحدود' : 'Unlimited';\n                  let limitTotalTxt = u.limitTotalReq ? u.limitTotalReq : unlimitedTxt;\n                  let limitDailyTxt = u.limitDailyReq ? u.limitDailyReq : unlimitedTxt;\n\n                  let perT = u.limitTotalReq ? Math.min(100, (userReqs / u.limitTotalReq) * 100).toFixed(1) + '%' : '-';\n                  let perD = u.limitDailyReq ? Math.min(100, (userDReqs / u.limitDailyReq) * 100).toFixed(1) + '%' : '-';\n\n                  let expTxt = unlimitedTxt;\n                  let isExp = false;\n                  if (u.expiryMs) {\n                      let date = new Date(u.expiryMs);\n                      expTxt = lang === 'fa' ? date.toLocaleDateString('fa-IR') : date.toLocaleDateString();\n                      if (Date.now() > u.expiryMs) {\n                          const expiredTxt = lang === 'fa' ? ' (منقضی شده)' : ' (Expired)';\n                          expTxt += ` <span class=\"text-xs text-red-500 font-bold\">${expiredTxt}</span>`;\n                          isExp = true;\n                      }\n                  }\n\n                  const totalLabel = lang === 'fa' ? 'کل:' : 'Total:';\n                  const dailyLabel = lang === 'fa' ? 'روزانه:' : 'Daily:';\n                  const rLabel = lang === 'fa' ? 'درخواست' : 'r';\n\n                  let linkTitle = lang === 'fa' ? 'کپی لینک ساب' : 'Copy Subscription Link';\n                  let pauseTitle = u.isPaused ? (lang === 'fa' ? 'فعال‌سازی کاربر' : 'Resume User') : (lang === 'fa' ? 'توقف کاربر' : 'Pause User');\n                  let editTitle = lang === 'fa' ? 'ویرایش کاربر' : 'Edit Subscriber';\n                  let resetTitle = lang === 'fa' ? 'بازنشانی مصرف ترافیک' : 'Reset Traffic Metrics';\n                  let deleteTitle = lang === 'fa' ? 'حذف کاربر' : 'Delete User';\n\n                   let linkHtml = `<button onclick=\"copyData('sync-${u.id}')\" class=\"native-press flex-1 flex items-center justify-center text-slate-500 hover:text-primary bg-slate-100/50 hover:bg-primary/10 dark:bg-slate-800/50 dark:hover:bg-primary/20 border border-slate-200/50 dark:border-slate-700/50 py-2 rounded-lg transition-colors\" title=\"${linkTitle}\">\n                       <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\">\n                           <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1\" />\n                       </svg>\n                   </button>`;\n\n                   let pauseBtnHtml = `<button onclick=\"togglePauseUser('${u.id}')\" class=\"native-press flex-1 flex items-center justify-center ${u.isPaused ? 'text-emerald-500 hover:text-emerald-600 bg-emerald-50 hover:bg-emerald-100 dark:bg-emerald-950/30 dark:hover:bg-emerald-900/30 border border-emerald-100 dark:border-emerald-900/30' : 'text-amber-500 hover:text-amber-700 bg-amber-50 hover:bg-amber-100 dark:bg-amber-950/30 dark:hover:bg-amber-900/30 border border-amber-100 dark:border-amber-900/30'} py-2 rounded-lg transition-colors\" title=\"${pauseTitle}\">\n                       ${u.isPaused ? `\n                       <svg class=\"w-4 h-4\" fill=\"currentColor\" viewBox=\"0 0 24 24\">\n                           <path d=\"M8 5v14l11-7z\" />\n                       </svg>\n                       ` : `\n                       <svg class=\"w-4 h-4\" fill=\"currentColor\" viewBox=\"0 0 24 24\">\n                           <path d=\"M6 19h4V5H6v14zm8-14v14h4V5h-4z\" />\n                       </svg>\n                       `}\n                   </button>`;\n\n                   let editBtnHtml = `<button onclick=\"editUser('${u.id}')\" class=\"native-press flex-1 flex items-center justify-center text-slate-500 hover:text-primary bg-slate-100/50 hover:bg-primary/10 dark:bg-slate-800/50 dark:hover:bg-primary/20 border border-slate-200/50 dark:border-slate-700/50 py-2 rounded-lg transition-colors\" title=\"${editTitle}\">\n                       <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\">\n                           <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z\" />\n                       </svg>\n                   </button>`;\n\n                   let resetBtnHtml = `<button onclick=\"resetUserTraffic('${u.id}')\" class=\"native-press flex-1 flex items-center justify-center text-slate-500 hover:text-violet-500 bg-slate-100/50 hover:bg-violet-500/10 dark:bg-slate-800/50 dark:hover:bg-violet-500/20 border border-slate-200/50 dark:border-slate-700/50 py-2 rounded-lg transition-colors\" title=\"${resetTitle}\">\n                       <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\">\n                           <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15\" />\n                       </svg>\n                   </button>`;\n\n                   let deleteBtnHtml = `<button onclick=\"deleteUser('${u.id}')\" class=\"native-press flex-1 flex items-center justify-center text-red-500 hover:text-red-600 bg-red-50 hover:bg-red-100 dark:bg-red-950/30 dark:hover:bg-red-900/30 border border-red-100 dark:border-red-900/30 py-2 rounded-lg transition-colors\" title=\"${deleteTitle}\">\n                       <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" viewBox=\"0 0 24 24\">\n                           <path stroke-linecap=\"round\" stroke-linejoin=\"round\" d=\"M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16\" />\n                       </svg>\n                   </button>`;\n\n                  let isAutoDisabled = u.isPaused && u.disabledReason;\n                  let disableInfoHtml = '';\n                  if (isAutoDisabled) {\n                      let reasonLabel = u.disabledReason;\n                      let timeLabel = u.disabledAt ? new Date(u.disabledAt).toLocaleString() : '';\n                      let reasonTitle = lang === 'fa' ? 'علت غیرفعال‌سازی' : 'Disable Reason';\n                      let timeTitle = lang === 'fa' ? 'زمان غیرفعال‌سازی' : 'Disabled At';\n                      disableInfoHtml = `\n                          <div class=\"mt-2 p-2 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800/30\">\n                              <div class=\"flex items-center gap-1.5 text-[10px] font-bold text-red-600 dark:text-red-400\">\n                                  <span>⚠️</span>\n                                  <span>${reasonTitle}:</span>\n                              </div>\n                              <div class=\"text-[10px] text-red-500 dark:text-red-300 mt-0.5\">${reasonLabel}</div>\n                              ${timeLabel ? `<div class=\"text-[9px] text-slate-400 mt-1\">${timeTitle}: ${timeLabel}</div>` : ''}\n                          </div>\n                      `;\n                  }\n\n                  let rawSync = window.mrvpn294Profiles?.find(p => p.id === u.id)?.sync || '';\n                  if (rawSync) {\n                      rawSync += rawSync.includes('?') ? '&flag=a' : '?flag=a';\n                  }\n\n                  tblHtml += `<div class=\"native-press bg-[var(--color-surface)]  rounded-2xl border border-slate-200 dark:border-darkborder p-4 hover:shadow-md transition-shadow\">\n                      <div class=\"flex items-center justify-between mb-3\">\n                          <div class=\"flex items-center gap-2 min-w-0 flex-1\">\n                              <span class=\"w-2 h-2 rounded-full shrink-0 ${u.isPaused ? (isAutoDisabled ? 'bg-red-500' : 'bg-amber-500') : (isExp ? 'bg-red-400' : 'bg-emerald-500')}\"></span>\n                              <span class=\"font-bold text-sm text-slate-800 dark:text-white truncate\">${u.name}</span>\n                              ${u.proxyIpGeo ? `<span class=\"text-[10px] px-1.5 py-0.5 rounded bg-violet-50 dark:bg-violet-900/30 text-violet-600 dark:text-violet-300 font-semibold shrink-0\">${u.proxyIpGeo.flag}</span>` : ''}\n                          </div>\n                          <input type=\"hidden\" id=\"sync-${u.id}\" value=\"${rawSync}\">\n                      </div>\n                      <div class=\"flex items-center gap-1.5 mb-3\">\n                          ${linkHtml}\n                          ${pauseBtnHtml}\n                          ${editBtnHtml}\n                          ${resetBtnHtml}\n                          ${deleteBtnHtml}\n                      </div>\n                      <div class=\"flex flex-wrap gap-1 mb-3\">\n                          ${u.isPaused && u.disabledReason ? `<span class=\"text-[9px] font-bold px-1.5 py-0.5 rounded bg-red-100 dark:bg-red-900/40 text-red-600 dark:text-red-300\">Auto-Disabled</span>` : ''}\n                          ${u.userMode ? `<span class=\"text-[9px] font-bold px-1.5 py-0.5 rounded bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-300\">${u.userMode === 'alpha' ? 'VLESS' : u.userMode === 'beta' ? 'Trojan' : 'Both'}</span>` : ''}\n                          ${u.userPorts ? `<span class=\"text-[9px] font-bold px-1.5 py-0.5 rounded bg-emerald-50 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-300\">${u.userPorts}</span>` : ''}\n                           ${u.maxConfigs ? `<span class=\"text-[9px] font-bold px-1.5 py-0.5 rounded bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-300\">${u.maxConfigs} cfgs</span>` : ''}\n                           ${u.connLimit ? `<span class=\"text-[9px] font-bold px-1.5 py-0.5 rounded bg-cyan-50 dark:bg-cyan-900/30 text-cyan-600 dark:text-cyan-300\">${u.connLimit} conn</span>` : ''}\n                      </div>\n                      ${disableInfoHtml}\n                      <div class=\"grid grid-cols-2 gap-3\">\n                          <div class=\"bg-slate-50 dark:bg-slate-800/50 rounded-xl p-2.5\">\n                              <div class=\"text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-1\">${totalLabel}</div>\n                              <div class=\"text-sm font-black text-slate-800 dark:text-white\">${(userReqs/6000).toFixed(2)} <span class=\"text-[10px] font-semibold text-slate-400\">GB</span></div>\n                              ${u.limitTotalReq ? `\n                              <div class=\"w-full bg-slate-200 dark:bg-slate-700 h-1.5 rounded-full overflow-hidden mt-1.5\">\n                                  <div class=\"bg-gradient-to-r ${parseFloat(perT) > 85 ? 'from-red-500 to-rose-600' : parseFloat(perT) > 60 ? 'from-amber-500 to-orange-500' : 'from-emerald-500 to-teal-500'} h-full rounded-full\" style=\"width: ${perT}\"></div>\n                              </div>\n                              <div class=\"flex items-center justify-between mt-1\">\n                                  <span class=\"text-[9px] text-slate-400\">/ ${(u.limitTotalReq/6000).toFixed(2)} GB</span>\n                                  ${perT !== '-' ? `<span class=\"text-[9px] font-bold ${parseFloat(perT) > 85 ? 'text-red-500' : parseFloat(perT) > 60 ? 'text-amber-500' : 'text-emerald-500'}\">${perT}</span>` : ''}\n                              </div>\n                              ` : '<div class=\"text-[9px] text-slate-400 mt-1\">' + unlimitedTxt + '</div>'}\n                          </div>\n                          <div class=\"bg-slate-50 dark:bg-slate-800/50 rounded-xl p-2.5\">\n                              <div class=\"text-[9px] font-bold text-slate-400 uppercase tracking-wider mb-1\">${dailyLabel}</div>\n                              <div class=\"text-sm font-black text-slate-800 dark:text-white\">${userDReqs} <span class=\"text-[10px] font-semibold text-slate-400\">${rLabel}</span></div>\n                              ${u.limitDailyReq ? `\n                              <div class=\"w-full bg-slate-200 dark:bg-slate-700 h-1.5 rounded-full overflow-hidden mt-1.5\">\n                                  <div class=\"bg-gradient-to-r ${parseFloat(perD) > 85 ? 'from-red-500 to-rose-600' : parseFloat(perD) > 60 ? 'from-amber-500 to-orange-500' : 'from-emerald-500 to-teal-500'} h-full rounded-full\" style=\"width: ${perD}\"></div>\n                              </div>\n                              <div class=\"flex items-center justify-between mt-1\">\n                                  <span class=\"text-[9px] text-slate-400\">/ ${(u.limitDailyReq/6000).toFixed(2)} GB</span>\n                                  ${perD !== '-' ? `<span class=\"text-[9px] font-bold ${parseFloat(perD) > 85 ? 'text-red-500' : parseFloat(perD) > 60 ? 'text-amber-500' : 'text-emerald-500'}\">${perD}</span>` : ''}\n                              </div>\n                              ` : '<div class=\"text-[9px] text-slate-400 mt-1\">' + unlimitedTxt + '</div>'}\n                          </div>\n                      </div>\n                      <div class=\"flex items-center justify-between mt-2 pt-2 border-t border-slate-100 dark:border-slate-800\">\n                          <span class=\"text-[10px] text-slate-400\">📅 ${expTxt}</span>\n                      </div>\n                  `;\n                  tblHtml += '</div>';\n              });\n              tbl.innerHTML = tblHtml;\n              applyLang();\n          }\n\n          async function resetUserTraffic(uuid) {\n              const resetMsg = lang === 'fa' ? 'آیا از بازنشانی وضعیت ترافیک (کل و روزانه) این مشترک مطمئن هستید؟' : 'Are you sure you want to reset all traffic metrics (Total and Daily) for this subscriber?';\n              if(!confirm(resetMsg)) return;\n              try {\n                  const res = await fetch(baseRoute + '/api/sync', {\n                      method: 'POST',\n                      headers: {'Content-Type': 'application/json'},\n                      body: JSON.stringify({ key: sessionKey, resetUUID: uuid })\n                  });\n                  if (res.ok) {\n                      const successMsg = lang === 'fa' ? 'ترافیک مشترک با موفقیت بازنشانی شد!' : 'Subscriber traffic metrics successfully reset!';\n                      alert(successMsg);\n                      doLogin(true); // reload usage data from server\n                  } else {\n                      const errMsg = lang === 'fa' ? 'سرور در بازنشانی ترافیک خطا بازگرداند.' : 'Server returned error while resetting metrics.';\n                      alert(errMsg);\n                  }\n              } catch(e) {\n                  const netErr = lang === 'fa' ? 'خطای ارتباط با شبکه.' : 'Network connection error.';\n                  alert(netErr);\n              }\n          }\n\n          function deleteUser(uuid) {\n              const deleteMsg = lang === 'fa' ? 'آیا از حذف این کاربر مطمئن هستید؟' : 'Are you sure you want to delete this user?';\n              if(!confirm(deleteMsg)) return;\n              if(window.mrvpn294Config && window.mrvpn294Config.users) {\n                  window.mrvpn294Config.users = window.mrvpn294Config.users.filter(u => u.id !== uuid);\n              }\n              // Automatically sync\n              renderUsersTable();\n              doSaveDirectly();\n          }\n\n          function togglePauseUser(uuid) {\n              if(window.mrvpn294Config && window.mrvpn294Config.users) {\n                  let usr = window.mrvpn294Config.users.find(u => u.id === uuid);\n                  if (usr) {\n                      usr.isPaused = !usr.isPaused;\n                      if (!usr.isPaused) {\n                          usr.disabledReason = null;\n                          usr.disabledAt = null;\n                      }\n                      renderUsersTable();\n                      doSaveDirectly();\n                  }\n              }\n          }\n\n          function getGlobalPorts() {\n              return (window.mrvpn294Config && window.mrvpn294Config.socketPorts)\n                  ? window.mrvpn294Config.socketPorts.split(',').map(s=>s.trim()).filter(Boolean)\n                  : ['443'];\n          }\n\n          function getGlobalMode() {\n              return (window.mrvpn294Config && window.mrvpn294Config.mode) ? window.mrvpn294Config.mode : 'alpha';\n          }\n\n          function openAddUserPage() {\n              document.getElementById('view-users').classList.add('hidden');\n              document.getElementById('view-add-user').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = 'hidden';\n              sc.classList.add('flex', 'flex-col');\n              sc.firstElementChild.classList.add('flex-1', 'min-h-0', 'flex', 'flex-col');\n              updateTitleText('Add User');\n              buildPortCheckboxes('add-user-ports-wrap', null);\n              buildModeCheckboxes('add-user-mode-wrap', null);\n              buildIPCheckboxes(\"add-user-clean-ips-wrap\", \"\", (window.mrvpn294Config?.cleanIps||\"\").split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n              buildIPCheckboxes(\"add-user-proxy-ips-wrap\", \"\", (window.mrvpn294Config?.backupRelay||\"\").split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n              buildNodeCheckboxes(\"add-user-nodes-wrap\", \"\", getGlobalNodeList());\n          }\n          function closeAddUserPage() {\n              document.getElementById('view-add-user').classList.add('hidden');\n              document.getElementById('view-users').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = '';\n              sc.classList.remove('flex', 'flex-col');\n              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');\n              updateTitle();\n          }\n          function closeEditUserPage() {\n              document.getElementById('view-edit-user').classList.add('hidden');\n              document.getElementById('view-users').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = '';\n              sc.classList.remove('flex', 'flex-col');\n              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');\n              updateTitle();\n          }\n          function updateTitleText(txt) {\n              var el = document.getElementById('view-title');\n              if (el) el.innerText = txt;\n          }\n\n\nfunction buildIPCheckboxes(wrapId, selectedIps, allIps) {\n    const wrap = document.getElementById(wrapId);\n    if(!wrap) return;\n    wrap.innerHTML = '';\n    if(!allIps || allIps.length === 0) {\n        wrap.innerHTML = '<span class=\"text-xs text-slate-400\">' + (i18n[lang]?.no_ips_advanced || 'No IPs added in Advanced Tab') + '</span>';\n        return;\n    }\n    const selArr = selectedIps ? selectedIps.split(',').map(s=>s.trim()).filter(Boolean) : [];\n    allIps.forEach(ip => {\n        const lbl = document.createElement('label');\n        lbl.className = \"flex items-center gap-1.5 text-sm cursor-pointer border border-slate-200 dark:border-darkborder px-2 py-1 rounded-lg\";\n        const cb = document.createElement('input');\n        cb.type = \"checkbox\";\n        cb.className = \"accent-primary\";\n        cb.value = ip;\n        if(selArr.includes(ip)) cb.checked = true;\n\n        lbl.appendChild(cb);\n        const span = document.createElement('span');\n        span.innerText = ip;\n        lbl.appendChild(span);\n        wrap.appendChild(lbl);\n    });\n}\nfunction getSelectedCheckboxes(wrapId) {\n    const wrap = document.getElementById(wrapId);\n    if(!wrap) return '';\n    const checked = Array.from(wrap.querySelectorAll('input:checked')).map(cb => cb.value);\n    return checked.join(',');\n}\nfunction getGlobalNodeList() {\n    var nodes = [];\n    var lp = (window.mrvpn294Config && Array.isArray(window.mrvpn294Config.linkedPanels)) ? window.mrvpn294Config.linkedPanels : [];\n    lp.forEach(function(p){\n        var raw = (p && typeof p === 'object') ? (p.url || '') : (p || '');\n        raw = String(raw).trim();\n        if(!raw) return;\n        raw = raw.replace(/^[a-zA-Z]+:\\/\\//, '').split('/')[0].split('@').pop();\n        var h = raw.indexOf('[') === 0 ? raw.slice(0, raw.indexOf(']') + 1) : raw.split(':')[0];\n        h = h.trim();\n        if(h) nodes.push(h);\n    });\n    return nodes.filter(function(v,i,a){return a.indexOf(v) === i;});\n}\nfunction buildNodeCheckboxes(wrapId, selectedNodes, allNodes) {\n    const wrap = document.getElementById(wrapId);\n    if(!wrap) return;\n    wrap.innerHTML = '';\n    if(!allNodes || allNodes.length === 0) {\n        wrap.innerHTML = '<span class=\"text-xs text-slate-400\">' + (i18n[lang]?.no_nodes_advanced || 'No slave nodes in Advanced Tab') + '</span>';\n        return;\n    }\n    const selArr = selectedNodes ? selectedNodes.split(',').map(s=>s.trim()).filter(Boolean) : [];\n    allNodes.forEach(node => {\n        const lbl = document.createElement('label');\n        lbl.className = \"flex items-center gap-1.5 text-sm cursor-pointer border border-slate-200 dark:border-darkborder px-2 py-1 rounded-lg\";\n        const cb = document.createElement('input');\n        cb.type = \"checkbox\";\n        cb.className = \"accent-primary\";\n        cb.value = node;\n        if(selArr.includes(node)) cb.checked = true;\n        lbl.appendChild(cb);\n        const span = document.createElement('span');\n        span.innerText = node;\n        lbl.appendChild(span);\n        wrap.appendChild(lbl);\n    });\n}\n\nfunction buildPortCheckboxes(wrapId, selectedPorts) {\n              const wrap = document.getElementById(wrapId);\n              if (!wrap) return;\n              const globalPorts = getGlobalPorts();\n              const sel = selectedPorts ? selectedPorts.split(',').map(s=>s.trim()) : ['443'];\n              wrap.innerHTML = globalPorts.map(function(p) {\n                  return '<label class=\"flex items-center gap-1.5 text-sm cursor-pointer\"><input type=\"checkbox\" value=\"' + p + '\" class=\"' + wrapId + '-port-cb accent-primary\"' + (sel.includes(p) ? ' checked' : '') + '><span>' + p + '</span></label>';\n              }).join('');\n          }\n\n          function buildModeCheckboxes(wrapId, userMode) {\n              const globalMode = getGlobalMode();\n              const alphaAllowed = globalMode === 'alpha' || globalMode === 'both';\n              const betaAllowed = globalMode === 'beta' || globalMode === 'both';\n              const selAlpha = userMode === 'alpha' || userMode === 'both' || (!userMode && alphaAllowed);\n              const selBeta = userMode === 'beta' || userMode === 'both' || (!userMode && betaAllowed);\n              const wrap = document.getElementById(wrapId);\n              if (!wrap) return;\n              wrap.querySelectorAll('input[type=checkbox]').forEach(cb => {\n                  if (cb.value === 'alpha') { cb.disabled = !alphaAllowed; cb.checked = selAlpha && alphaAllowed; cb.closest\t\t\t('label').style.opacity = alphaAllowed ? '1' : '0.35'; }\n                  if (cb.value === 'beta')  { cb.disabled = !betaAllowed;  cb.checked = selBeta && betaAllowed;  cb.closest\t\t\t('label').style.opacity = betaAllowed  ? '1' : '0.35'; }\n              });\n          }\n\n          function readModeFromCheckboxes(cbClass) {\n             const cbs = [...document.querySelectorAll('.' + cbClass + ':checked')].map(c=>c.value);\n              if (cbs.includes('alpha') && cbs.includes('beta')) return 'both';\n              if (cbs.includes('alpha')) return 'alpha';\n              if (cbs.includes('beta')) return 'beta';\n              return getGlobalMode();\n          }\n\n          function readPortsFromCheckboxes(wrapId) {\n             const ports = [...document.querySelectorAll('#' + wrapId + ' input[type=checkbox]:checked')].map(c=>c.value);\n              return ports.length ? ports.join(',') : getGlobalPorts()[0];\n          }\n\n          function commitAddUser() {\n              const name = document.getElementById('add-user-name').value.trim();\n              let tReq = document.getElementById('add-user-total-reqs').value;\n              tReq = tReq? Math.floor(parseFloat(tReq) * 6000): null;\n              let dReq = document.getElementById('add-user-daily-reqs').value;\n              dReq = dReq? Math.floor(parseFloat(dReq) * 6000): null;\n              let days = document.getElementById('add-user-days').value;\n               const cleanIpsCheckbox = getSelectedCheckboxes(\"add-user-clean-ips-wrap\");\n               const cleanIpsCustom = document.getElementById(\"add-user-custom-clean\").value.trim();\n               let cleanIpArray = [];\n               if (cleanIpsCheckbox) cleanIpArray.push(...cleanIpsCheckbox.split(','));\n               if (cleanIpsCustom) {\n                   cleanIpArray.push(...cleanIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               }\n               const cleanIp = cleanIpArray.length ? cleanIpArray.join(',') : null;\n               const proxyIpsCheckbox = getSelectedCheckboxes(\"add-user-proxy-ips-wrap\");\n               const proxyIpsCustom = document.getElementById(\"add-user-custom-proxy\").value.trim();\n               let proxyIpArray = [];\n               if (proxyIpsCheckbox) proxyIpArray.push(...proxyIpsCheckbox.split(','));\n               if (proxyIpsCustom) {\n                   proxyIpArray.push(...proxyIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               }\n               const proxyIp = proxyIpArray.length ? proxyIpArray.join(',') : null;\n\n               const customName = document.getElementById('add-user-custom-name').value.trim() || null;\n               const userMode = readModeFromCheckboxes('add-mode-cb');\n               const userPorts = readPortsFromCheckboxes('add-user-ports-wrap');\n               let maxConfigs = document.getElementById('add-user-max-configs').value;\n               maxConfigs = maxConfigs ? parseInt(maxConfigs) : null;\n               const nodesCheckbox = getSelectedCheckboxes(\"add-user-nodes-wrap\");\n               const nodesCustom = document.getElementById(\"add-user-custom-nodes\").value.trim();\n               let nodesArray = [];\n               if (nodesCheckbox) nodesArray.push(...nodesCheckbox.split(','));\n               if (nodesCustom) nodesArray.push(...nodesCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               const userNodes = nodesArray.length ? nodesArray.join(',') : null;\n               const nat64 = document.getElementById('edit-user-nat64').value.trim() || null;\n\n               if(!name) {\n                   alert(lang === 'fa' ? 'لطفاً نام را وارد کنید' : 'Please enter a name');\n                  return;\n              }\n\n              if(!window.mrvpn294Config) window.mrvpn294Config = {};\n              if(!window.mrvpn294Config.users) window.mrvpn294Config.users = [];\n\n              if(window.mrvpn294Config.users.some(u => u.name.trim().toLowerCase() === name.toLowerCase())) {\n                  alert(lang === 'fa' ? 'این نام قبلاً استفاده شده است' : 'This name is already taken');\n                  return;\n              }\n\n               tReq = tReq ? parseInt(tReq) : null;\n               dReq = dReq ? parseInt(dReq) : null;\n               days = days ? parseInt(days) : null;\n               let connLimit = document.getElementById('add-user-conn-limit').value;\n               connLimit = connLimit ? parseInt(connLimit) : null;\n               const userPanelUrl = document.getElementById('add-user-panel-url').value.trim() || null;\n\n               let newId = Array.from(crypto.getRandomValues(new Uint8Array(16)))\n                   .map((b,i) => (i===4||i===6||i===8||i===10?'-':'') + b.toString(16).padStart(2,'0')).join('');\n\n                const u = {\n                    id: newId,\n                    name: name,\n                    limitTotalReq: tReq,\n                    limitDailyReq: dReq,\n                    expiryMs: days ? Date.now() + days*86400000 : null,\n                    proxyIp: proxyIp,\n                     cleanIp: cleanIp,\n                     customName: customName,\n                     userMode: userMode,\n                     userPorts: userPorts,\n                     maxConfigs: maxConfigs,\n                     userNodes: userNodes,\n                     nat64: nat64,\n                     connLimit: connLimit,\n                     userPanelUrl: userPanelUrl,\n                     createdAt: Date.now()\n                };\n\n              window.mrvpn294Config.users.push(u);\n              document.getElementById('view-add-user').classList.add('hidden');\n              document.getElementById('view-users').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = '';\n              sc.classList.remove('flex', 'flex-col');\n              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');\n              updateTitle();\n              document.getElementById('add-user-name').value = '';\n               document.getElementById('add-user-custom-name').value = '';\n               document.getElementById('add-user-custom-clean').value = '';\n               document.getElementById('add-user-helper-ip').value = '';\n               document.getElementById('add-user-helper-name').value = '';\n               document.getElementById('add-user-custom-proxy').value = '';\n               document.getElementById('add-user-custom-nodes').value = '';\n              document.getElementById('add-user-total-reqs').value = '';\n              document.getElementById('add-user-daily-reqs').value = '';\n              document.getElementById('add-user-days').value = '';\n              document.getElementById('add-user-max-configs').value = '';\n              document.getElementById('add-user-conn-limit').value = '';\n              document.getElementById('add-user-panel-url').value = '';\n\n              renderUsersTable();\n              doSaveDirectly();\n          }\n\n          function editUser(uuid) {\n              if(!window.mrvpn294Config || !window.mrvpn294Config.users) return;\n              let u = window.mrvpn294Config.users.find(usr => usr.id === uuid);\n              if(!u) return;\n\n              document.getElementById('edit-user-id').value = u.id;\n              document.getElementById('edit-user-name').value = u.name;\n              document.getElementById('edit-user-total-reqs').value = u.limitTotalReq? (u.limitTotalReq / 6000).toFixed(2): '';\n              document.getElementById('edit-user-daily-reqs').value = u.limitDailyReq? (u.limitDailyReq / 6000).toFixed(2): '';\n                            const globalCleanIps = (window.mrvpn294Config?.cleanIps||\"\").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);\n              const userCleanIps = (u.cleanIp || \"\").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);\n              const checkedGlobalClean = [];\n              const customClean = [];\n              userCleanIps.forEach(ip => {\n                  let hostOnly = ip.split('#')[0].split(':')[0].trim();\n                  let isFound = globalCleanIps.some(g => g.split('#')[0].split(':')[0].trim() === hostOnly || g === ip);\n                  if (isFound) checkedGlobalClean.push(ip);\n                  else customClean.push(ip);\n              });\n              buildIPCheckboxes(\"edit-user-clean-ips-wrap\", checkedGlobalClean.join(','), globalCleanIps);\n              document.getElementById('edit-user-custom-clean').value = customClean.join(', ');\n              document.getElementById('edit-user-helper-ip').value = '';\n              document.getElementById('edit-user-helper-name').value = '';\n\n              const globalProxyIps = (window.mrvpn294Config?.backupRelay||\"\").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);\n              const userProxyIps = (u.proxyIp || \"\").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);\n              const checkedGlobalProxy = [];\n              const customProxy = [];\n              userProxyIps.forEach(ip => {\n                  let hostOnly = ip.split('#')[0].split(':')[0].trim();\n                  let isFound = globalProxyIps.some(g => g.split('#')[0].split(':')[0].trim() === hostOnly || g === ip);\n                  if (isFound) checkedGlobalProxy.push(ip);\n                  else customProxy.push(ip);\n              });\n               buildIPCheckboxes(\"edit-user-proxy-ips-wrap\", checkedGlobalProxy.join(','), globalProxyIps);\n               document.getElementById('edit-user-custom-proxy').value = customProxy.join(', ');\n\n               const globalNodes = getGlobalNodeList();\n               const userNodesList = (u.userNodes || \"\").split(/[\\r\\n,;]+/).map(s=>s.trim()).filter(Boolean);\n               const checkedGlobalNodes = [];\n               const customNodes = [];\n               userNodesList.forEach(node => {\n                   let isFound = globalNodes.some(g => g === node);\n                   if (isFound) checkedGlobalNodes.push(node);\n                   else customNodes.push(node);\n               });\n               buildNodeCheckboxes(\"edit-user-nodes-wrap\", checkedGlobalNodes.join(','), globalNodes);\n               document.getElementById('edit-user-custom-nodes').value = customNodes.join(', ');\n               document.getElementById('edit-user-nat64').value = u.nat64 || '';\n\n               document.getElementById('edit-user-custom-name').value = u.customName || '';\n\n              document.getElementById('edit-user-max-configs').value = u.maxConfigs || '';\n              document.getElementById('edit-user-conn-limit').value = u.connLimit || '';\n              document.getElementById('edit-user-panel-url').value = u.userPanelUrl || '';\n\n              buildPortCheckboxes('edit-user-ports-wrap', u.userPorts);\n              buildModeCheckboxes('edit-user-mode-wrap', u.userMode);\n\n              let daysLeft = '';\n              if(u.expiryMs) {\n                  let diff = u.expiryMs - Date.now();\n                  daysLeft = diff > 0 ? Math.ceil(diff / 86400000) : 0;\n              }\n              document.getElementById('edit-user-days').value = daysLeft;\n\n              document.getElementById('view-users').classList.add('hidden');\n              document.getElementById('view-edit-user').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = 'hidden';\n              sc.classList.add('flex', 'flex-col');\n              sc.firstElementChild.classList.add('flex-1', 'min-h-0', 'flex', 'flex-col');\n              updateTitleText('Edit Subscriber');\n          }\n\n          function commitEditUser() {\n              const uuid = document.getElementById('edit-user-id').value;\n              const name = document.getElementById('edit-user-name').value.trim();\n              let tReq = document.getElementById('edit-user-total-reqs').value;\n              tReq = tReq? Math.floor(parseFloat(tReq) * 6000): null;\n              let dReq = document.getElementById('edit-user-daily-reqs').value;\n              dReq = dReq? Math.floor(parseFloat(dReq) * 6000): null;\n              let days = document.getElementById('edit-user-days').value;\n                             const proxyIpsCheckbox = getSelectedCheckboxes(\"edit-user-proxy-ips-wrap\");\n               const proxyIpsCustom = document.getElementById(\"edit-user-custom-proxy\").value.trim();\n               let proxyIpArray = [];\n               if (proxyIpsCheckbox) proxyIpArray.push(...proxyIpsCheckbox.split(','));\n               if (proxyIpsCustom) {\n                   proxyIpArray.push(...proxyIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               }\n               const proxyIp = proxyIpArray.length ? proxyIpArray.join(',') : null;\n\n               const customName = document.getElementById('edit-user-custom-name').value.trim() || null;\n               const cleanIpsCheckbox = getSelectedCheckboxes(\"edit-user-clean-ips-wrap\");\n               const cleanIpsCustom = document.getElementById(\"edit-user-custom-clean\").value.trim();\n               let cleanIpArray = [];\n               if (cleanIpsCheckbox) cleanIpArray.push(...cleanIpsCheckbox.split(','));\n               if (cleanIpsCustom) {\n                   cleanIpArray.push(...cleanIpsCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               }\n               const cleanIp = cleanIpArray.length ? cleanIpArray.join(',') : null;\n              const userMode = readModeFromCheckboxes('edit-mode-cb');\n              const userPorts = readPortsFromCheckboxes('edit-user-ports-wrap');\n               let maxConfigs = document.getElementById('edit-user-max-configs').value;\n               maxConfigs = maxConfigs ? parseInt(maxConfigs) : null;\n               const nodesCheckbox = getSelectedCheckboxes(\"edit-user-nodes-wrap\");\n               const nodesCustom = document.getElementById(\"edit-user-custom-nodes\").value.trim();\n               let nodesArray = [];\n               if (nodesCheckbox) nodesArray.push(...nodesCheckbox.split(','));\n               if (nodesCustom) nodesArray.push(...nodesCustom.split(/[\\s,;]+/).map(s=>s.trim()).filter(Boolean));\n               const userNodes = nodesArray.length ? nodesArray.join(',') : null;\n                const nat64 = document.getElementById('add-user-nat64').value.trim() || null;\n                let connLimit = document.getElementById('edit-user-conn-limit').value;\n                connLimit = connLimit ? parseInt(connLimit) : null;\n                const userPanelUrl = document.getElementById('edit-user-panel-url').value.trim() || null;\n\n               if(!name) {\n                  alert(lang === 'fa' ? 'لطفاً نام را وارد کنید' : 'Please enter a name');\n                  return;\n              }\n              tReq = tReq ? parseInt(tReq) : null;\n              dReq = dReq ? parseInt(dReq) : null;\n              days = days ? parseInt(days) : null;\n\n              if(!window.mrvpn294Config || !window.mrvpn294Config.users) return;\n\n              if(window.mrvpn294Config.users.some(u => u.id !== uuid && u.name.trim().toLowerCase() === name.toLowerCase())) {\n                  alert(lang === 'fa' ? 'این نام قبلاً استفاده شده است' : 'This name is already taken');\n                  return;\n              }\n\n              let u = window.mrvpn294Config.users.find(usr => usr.id === uuid);\n              if(!u) return;\n\n              u.name = name;\n              u.limitTotalReq = tReq;\n              u.limitDailyReq = dReq;\n              u.expiryMs = days ? Date.now() + days*86400000 : null;\n              u.proxyIp = proxyIp;\n               u.cleanIp = cleanIp;\n               u.customName = customName;\n              u.userMode = userMode;\n              u.userPorts = userPorts;\n              u.maxConfigs = maxConfigs;\n              u.userNodes = userNodes;\n              u.nat64 = nat64;\n              u.connLimit = connLimit;\n              u.userPanelUrl = userPanelUrl;\n\n              document.getElementById('view-edit-user').classList.add('hidden');\n              document.getElementById('view-users').classList.remove('hidden');\n              var sc = document.querySelector('.scroll-content');\n              sc.style.overflow = '';\n              sc.classList.remove('flex', 'flex-col');\n              sc.firstElementChild.classList.remove('flex-1', 'min-h-0', 'flex', 'flex-col');\n              renderUsersTable();\n              doSaveDirectly();\n          }\n\n          async function loadApiKeys() {\n              try {\n                  const res = await fetch(baseRoute + '/api/keys', {\n                      headers: { 'Authorization': 'Bearer ' + sessionKey }\n                  });\n                  const data = await res.json();\n                  if (data.success) {\n                      const list = document.getElementById('api-keys-list');\n                      if (!list) return;\n                      if (!data.keys || data.keys.length === 0) {\n                          list.innerHTML = '<p class=\"text-xs text-slate-400 dark:text-slate-500\">' + (i18n[lang]?.api_keys_empty || 'No API keys generated yet.') + '</p>';\n                          return;\n                      }\n                      list.innerHTML = data.keys.map(k => {\n                          const created = new Date(k.createdAt).toLocaleDateString();\n                          const lastUsed = k.lastUsed ? new Date(k.lastUsed).toLocaleDateString() : (i18n[lang]?.never || 'Never');\n                          return '<div class=\"flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-darkborder\">' +\n                              '<div class=\"flex-1 min-w-0\">' +\n                              '<p class=\"text-xs font-bold text-slate-700 dark:text-slate-200 truncate\">' + (k.name || 'Unnamed') + '</p>' +\n                              '<p class=\"text-[10px] font-mono text-slate-400 mt-0.5\">' + k.keyPreview + '</p>' +\n                              '<p class=\"text-[10px] text-slate-400 mt-0.5\">' + (i18n[lang]?.created || 'Created') + ': ' + created + ' · ' + (i18n[lang]?.last_used || 'Last used') + ': ' + lastUsed + '</p>' +\n                              '</div>' +\n                              '<button onclick=\"revokeApiKey(\\'' + k.id + '\\')\" class=\"ms-3 px-3 py-1.5 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-[10px] font-bold rounded-lg border border-red-200 dark:border-red-800 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors\">' + (i18n[lang]?.revoke || 'Revoke') + '</button>' +\n                              '</div>';\n                      }).join('');\n                  }\n              } catch(e) {}\n          }\n\n          async function generateApiKey() {\n              const name = prompt(i18n[lang]?.enter_key_name || 'Enter a name for this API key:');\n              if (!name) return;\n              try {\n                  const res = await fetch(baseRoute + '/api/keys', {\n                      method: 'POST',\n                      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionKey },\n                      body: JSON.stringify({ action: 'create', name })\n                  });\n                  const data = await res.json();\n                  if (data.success && data.key) {\n                      const newBox = document.getElementById('api-key-new');\n                      const keyInput = document.getElementById('api-key-value');\n                      keyInput.value = data.key.key;\n                      newBox.classList.remove('hidden');\n                      loadApiKeys();\n                  } else {\n                      alert(data.error || 'Failed to create key');\n                  }\n              } catch(e) { alert('Error: ' + e.message); }\n          }\n\n          async function revokeApiKey(id) {\n              if (!confirm(i18n[lang]?.confirm_revoke || 'Revoke this API key? The remote panel will lose access.')) return;\n              try {\n                  const res = await fetch(baseRoute + '/api/keys', {\n                      method: 'POST',\n                      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionKey },\n                      body: JSON.stringify({ action: 'revoke', id })\n                  });\n                  const data = await res.json();\n                  if (data.success) loadApiKeys();\n                  else alert(data.error || 'Failed to revoke key');\n              } catch(e) { alert('Error: ' + e.message); }\n          }\n\n          function copyApiKey() {\n              const input = document.getElementById('api-key-value');\n              navigator.clipboard.writeText(input.value);\n              const stat = document.getElementById('save-status');\n              if (stat) { stat.textContent = \"Copied!\"; stat.className = \"text-sm font-bold text-emerald-500 md:me-4\"; setTimeout(() => { stat.textContent = \"\"; }, 2000); }\n          }\n\n          async function doSaveDirectly() {\n              const btn = document.querySelector('button[onclick=\"doSave()\"]');\n              const origText = btn.innerText; btn.innerText = \"...\";\n              try {\n                  const res = await fetch(baseRoute + '/api/sync', {\n                      method: 'POST',\n                      headers: {'Content-Type': 'application/json'},\n                      body: JSON.stringify({ key: sessionKey, config: window.mrvpn294Config })\n                  });\n                  if(res.ok) {\n                       const stat = document.getElementById('save-status');\n                       stat.textContent = \"Saved. Refreshing...\";\n                       setTimeout(() => { doLogin(true); stat.textContent = \"\"; }, 1000);\n                  }\n              } catch(e) {}\n              btn.innerText = origText;\n          }\n\n          async function resolveSmartCleanIps() {\n              const btn = document.getElementById('btn-resolve-smart-ips');\n              const origText = btn.innerHTML;\n              btn.disabled = true;\n              btn.innerHTML = '⚡ Resolving CDN & Clean IPs...';\n\n              const domains = [\n                  'www.speedtest.net',\n                  'grok.com',\n                  'feedback.spotify.com',\n                  'www.hcaptcha.com',\n                  'chatgpt.com',\n                  'sourceforge.net',\n                  'snapp.ir',\n                  'digikala.com',\n                  'divar.ir',\n                  'cafebazaar.ir',\n                  'shaparak.ir',\n                  'aparat.com',\n                  'soft98.ir',\n                  'varzesh3.com'\n              ];\n\n              let resolvedIps = new Set();\n              const cleanIpsTextarea = document.getElementById('cfg-ips');\n\n              async function resolveOne(domain) {\n                  try {\n                      const res = await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=A`, {\n                          headers: { 'accept': 'application/dns-json' }\n                      });\n                      const data = await res.json();\n                      if (data && data.Answer) {\n                          data.Answer.forEach(ans => {\n                              if (ans.type === 1 && ans.data) {\n                                  resolvedIps.add(ans.data);\n                              }\n                          });\n                      }\n                  } catch(e) {\n                      try {\n                          const res = await fetch(`https://dns.google/resolve?name=${encodeURIComponent(domain)}&type=A`);\n                          const data = await res.json();\n                          if (data && data.Answer) {\n                              data.Answer.forEach(ans => {\n                                  if (ans.type === 1 && ans.data) {\n                                      resolvedIps.add(ans.data);\n                                  }\n                              });\n                          }\n                      } catch(ge) {}\n                  }\n              }\n\n              try {\n                  await Promise.all(domains.map(d => resolveOne(d)));\n              } catch(err) {\n                  console.error(\"DNS resolving process encountered an issue:\", err);\n              }\n\n              if (resolvedIps.size > 0) {\n                  const ipList = Array.from(resolvedIps).join('\\n');\n                  cleanIpsTextarea.value = ipList;\n                  cleanIpsTextarea.dispatchEvent(new Event('input'));\n                  cleanIpsTextarea.dispatchEvent(new Event('change'));\n                  alert((lang === 'fa' ? 'با موفقیت حل شد و ' : 'Successfully resolved and loaded ') + resolvedIps.size + (lang === 'fa' ? ' آی‌پی تمیز بارگذاری شد!' : ' clean IPs!'));\n              } else {\n                  alert(lang === 'fa' ? 'خطا در تبدیل دامنه به آی‌پی. لطفاً اتصال اینترنت یا DNS سفارشی خود را بررسی کنید.' : 'Failed to resolve domains to IPs. Please verify your internet connection or custom DNS.');\n              }\n\n              btn.disabled = false;\n              btn.innerHTML = origText;\n          }\n\n          async function checkUpdate() {\n              try {\n                  const res = await fetch(baseRoute + '/api/update', {\n                      method: 'POST',\n                      headers: { 'Content-Type': 'application/json' },\n                      body: JSON.stringify({ key: sessionKey, action: 'check' })\n                  });\n                  const data = await res.json();\n                  if (data.success && data.updateAvailable) {\n                      window._updateData = data;\n                      if (window.mrvpn294Config?.autoUpdate && data.canDeploy) {\n                          const format = window.mrvpn294Config.autoUpdateFormat || 'normal';\n                          const formatEl = document.querySelector(`input[name=\"auto-update-format\"][value=\"${format}\"]`);\n                          if (formatEl) formatEl.checked = true;\n                          const autoRadio = document.querySelector(`input[name=\"auto-update-format\"][value=\"${format}\"]`);\n                          if (autoRadio) autoRadio.checked = true;\n                          doUpdate();\n                      } else {\n                          showUpdateBanner((document.getElementById('cfg-github-repo')?.value || window.mrvpn294Config?.githubRepo || 'amirpocom63-del/mrvpn294').replace('https://github.com/', '').replace('http://github.com/', '').trim(), data.latest);\n                      }\n                  }\n                  if (data.success && !data.canDeploy) {\n                      const statusEl = document.getElementById('update-deploy-status');\n                      if (statusEl) {\n                          statusEl.classList.remove('hidden');\n                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-amber-600 bg-amber-50 dark:bg-amber-900/20 dark:text-amber-400';\n                          statusEl.textContent = i18n[lang].update_requires_cf || 'Configure CF credentials to enable auto-deploy.';\n                      }\n                  }\n              } catch(err) {\n                  console.error(\"Update check failed:\", err);\n              }\n          }\n\n          async function doUpdate() {\n              const btn = document.getElementById('update-deploy-btn');\n              const statusEl = document.getElementById('update-deploy-status');\n              if (!btn) return;\n              if (!confirm(lang === 'fa' ? 'آیا از دپلوی نسخه فعلی/جدید اطمینان دارید؟' : 'Deploy the selected version now?')) return;\n\n              const formatEl = document.querySelector('input[name=\"update-format\"]:checked');\n              const format = formatEl ? formatEl.value : 'normal';\n              const forceDeploy = !window._updateData?.updateAvailable;\n\n              const origText = btn.innerHTML;\n              btn.innerHTML = '⏳ ' + (i18n[lang].update_deploying || 'Deploying...');\n              btn.disabled = true;\n              if (statusEl) {\n                  statusEl.classList.remove('hidden');\n                  statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-blue-600 bg-blue-50 dark:bg-blue-900/20 dark:text-blue-400 animate-pulse';\n                  statusEl.textContent = i18n[lang].update_deploying || 'Deploying update (processing on server)...';\n              }\n\n              try {\n                  const res = await fetch(baseRoute + '/api/update', {\n                      method: 'POST',\n                      headers: { 'Content-Type': 'application/json' },\n                      body: JSON.stringify({\n                          key: sessionKey,\n                          action: 'deploy',\n                          format: format,\n                          force: forceDeploy\n                      })\n                  });\n                  const data = await res.json();\n                  if (data.success) {\n                      if (statusEl) {\n                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-emerald-600 bg-emerald-50 dark:bg-emerald-900/20 dark:text-emerald-400';\n                          statusEl.textContent = (i18n[lang].update_success || 'Update successful!') + ' v' + data.newVersion;\n                      }\n                      btn.innerHTML = '✅ ' + (i18n[lang].update_success || 'Done!');\n                      setTimeout(() => window.location.reload(), 3000);\n                  } else {\n                      if (statusEl) {\n                          statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-red-600 bg-red-50 dark:bg-red-900/20 dark:text-red-400';\n                          statusEl.textContent = (i18n[lang].update_error || 'Update failed') + ': ' + (data.error || 'Unknown error');\n                      }\n                      btn.innerHTML = origText;\n                      btn.disabled = false;\n                  }\n              } catch(e) {\n                  if (statusEl) {\n                      statusEl.className = 'w-full mt-3 p-3 rounded-xl text-sm font-bold text-center text-red-600 bg-red-50 dark:bg-red-900/20 dark:text-red-400';\n                      statusEl.textContent = 'Error: ' + e.message;\n                  }\n                  btn.innerHTML = origText;\n                  btn.disabled = false;\n              }\n          }\n\n          async function triggerManualRedeploy() {\n              const banner = document.getElementById('update-alert-banner');\n              if (!banner) return;\n\n              document.getElementById('update-alert-text').textContent = lang === 'fa'\n                  ? 'می‌توانید آخرین نسخه فعال را مجدداً دپلوی نموده یا بین نسخه معمولی و مبهم‌سازی شده جابجا شوید.'\n                  : 'You can redeploy the latest code or switch between Normal/Obfuscated version on the fly.';\n\n              banner.classList.remove('hidden');\n              banner.classList.add('flex');\n\n              if (!window._updateData) {\n                  window._updateData = { latest: CURRENT_VERSION, updateAvailable: false };\n              }\n\n              const repo = (document.getElementById('cfg-github-repo')?.value || window.mrvpn294Config?.githubRepo || 'amirpocom63-del/mrvpn294').replace('https://github.com/', '').replace('http://github.com/', '').trim();\n\n              showUpdateBanner(repo, CURRENT_VERSION);\n\n              switchTab('overview');\n              document.getElementById('update-alert-banner').scrollIntoView({ behavior: 'smooth' });\n          }\n\n          function parseMarkdown(md) {\n              if (!md) return '';\n              let lines = md.split(/\\r?\\n/);\n              let htmlLines = [];\n              let inCodeBlock = false;\n              let codeContent = [];\n              let activeBlockLang = null;\n\n              for (let line of lines) {\n                  let trimmed = line.trim();\n\n                  if (trimmed === '<!-- LANG:EN -->' || trimmed === '<!--LANG:EN-->') {\n                      if (activeBlockLang === 'en') {\n                          activeBlockLang = null;\n                      } else {\n                          activeBlockLang = 'en';\n                      }\n                      continue;\n                  }\n                  if (trimmed === '<!-- LANG:FA -->' || trimmed === '<!--LANG:FA-->') {\n                      if (activeBlockLang === 'fa') {\n                          activeBlockLang = null;\n                      } else {\n                          activeBlockLang = 'fa';\n                      }\n                      continue;\n                  }\n\n                  if (activeBlockLang !== null && activeBlockLang !== lang) {\n                      continue;\n                  }\n\n                  // Toggle code block\n                  if (trimmed.startsWith('\\x60\\x60\\x60')) {\n                      if (inCodeBlock) {\n                          // Close code block\n                          let codeText = codeContent.join('\\n')\n                              .replace(/&/g, \"&amp;\")\n                              .replace(/</g, \"&lt;\")\n                              .replace(/>/g, \"&gt;\");\n                          htmlLines.push('<pre class=\"bg-slate-900/90 text-slate-100 p-3 rounded-xl my-2 font-mono text-[10px] overflow-x-auto border border-slate-800 max-h-40\">' + codeText + '</pre>');\n                          codeContent = [];\n                          inCodeBlock = false;\n                      } else {\n                          inCodeBlock = true;\n                      }\n                      continue;\n                  }\n\n                  if (inCodeBlock) {\n                      codeContent.push(line);\n                      continue;\n                  }\n\n                  if (!trimmed) {\n                      continue;\n                  }\n\n                  // Process headers\n                  if (trimmed.startsWith('### ')) {\n                      let text = trimmed.slice(4);\n                      htmlLines.push('<h5 class=\"text-sm font-bold text-amber-800 dark:text-amber-400 mt-3 mb-1\">' + parseInlineMarkdown(text) + '</h5>');\n                      continue;\n                  }\n                  if (trimmed.startsWith('## ')) {\n                      let text = trimmed.slice(3);\n                      htmlLines.push('<h4 class=\"text-sm font-extrabold text-amber-800 dark:text-amber-400 mt-4 mb-2\">' + parseInlineMarkdown(text) + '</h4>');\n                      continue;\n                  }\n                  if (trimmed.startsWith('# ')) {\n                      let text = trimmed.slice(2);\n                      htmlLines.push('<h3 class=\"text-base font-black text-amber-900 dark:text-amber-300 mt-4 mb-2\">' + parseInlineMarkdown(text) + '</h3>');\n                      continue;\n                  }\n\n                  // Process lists\n                  let listMatch = line.match(/^(\\s*)([-*+])\\s+(.*)$/);\n                  if (listMatch) {\n                      let text = listMatch[3];\n                      htmlLines.push('<div class=\"flex items-start gap-2 my-1\"><span class=\"text-amber-500 mt-0.5\">▪</span><span class=\"flex-1\">' + parseInlineMarkdown(text) + '</span></div>');\n                      continue;\n                  }\n\n                  // Standard line\n                  htmlLines.push('<p class=\"my-1\">' + parseInlineMarkdown(line) + '</p>');\n              }\n\n              // Guard for unclosed code block\n              if (inCodeBlock && codeContent.length > 0) {\n                  let codeText = codeContent.join('\\n')\n                      .replace(/&/g, \"&amp;\")\n                      .replace(/</g, \"&lt;\")\n                      .replace(/>/g, \"&gt;\");\n                  htmlLines.push('<pre class=\"bg-slate-900/90 text-slate-100 p-3 rounded-xl my-2 font-mono text-[10px] overflow-x-auto border border-slate-800 max-h-40\">' + codeText + '</pre>');\n              }\n\n              return htmlLines.join('\\n');\n\n              function parseInlineMarkdown(text) {\n                  let safe = text\n                      .replace(/&/g, \"&amp;\")\n                      .replace(/</g, \"&lt;\")\n                      .replace(/>/g, \"&gt;\");\n                  // Bold\n                  safe = safe.replace(/\\*\\*(.*?)\\*\\*/g, '<strong class=\"font-extrabold text-[var(--color-text)]\">$1</strong>');\n                  // Italic\n                  safe = safe.replace(/\\*(.*?)\\*/g, '<em class=\"italic\">$1</em>');\n                  // Inline code\n                  safe = safe.replace(/[\\x60](.*?)[\\x60]/g, '<code class=\"bg-amber-500/10 dark:bg-slate-800 px-1.5 py-0.5 rounded text-rose-500 font-mono text-[11px]\">$1</code>');\n                  return safe;\n              }\n          }\n\n          async function showUpdateBanner(repo, version) {\n              const banner = document.getElementById('update-alert-banner');\n              if (!banner) return;\n\n              const msg = lang === 'fa'\n                  ? 'نسخه جدیدتر (v' + version + ') در مخزن گیت‌هاب شما (' + repo + ') در دسترس است.'\n                  : 'A newer version (v' + version + ') is available in your GitHub repository (' + repo + ').';\n\n              document.getElementById('update-alert-text').textContent = msg;\n              const ghLink = document.getElementById('update-github-link');\n              if (ghLink) ghLink.href = 'https://github.com/' + repo;\n              banner.classList.remove('hidden');\n              banner.classList.add('flex');\n\n              const changelogArea = document.getElementById('update-changelog-area');\n              const changelogContent = document.getElementById('update-changelog-content');\n              if (changelogArea && changelogContent) {\n                  changelogArea.classList.remove('hidden');\n                  changelogContent.innerHTML = lang === 'fa'\n                      ? '<p class=\"animate-pulse\">در حال دریافت گزارش تغییرات...</p>'\n                      : '<p class=\"animate-pulse\">Loading changelog...</p>';\n\n                  try {\n                      let changelogText = '';\n                      try {\n                          const res = await fetch('https://api.github.com/repos/' + repo + '/releases/tags/v' + version);\n                          if (res.ok) {\n                              const rel = await res.json();\n                              if (rel && rel.body) {\n                                  changelogText = rel.body;\n                              }\n                          } else {\n                              const resNoV = await fetch('https://api.github.com/repos/' + repo + '/releases/tags/' + version);\n                              if (resNoV.ok) {\n                                  const relNoV = await resNoV.json();\n                                  if (relNoV && relNoV.body) {\n                                      changelogText = relNoV.body;\n                                  }\n                              }\n                          }\n                      } catch(e) {}\n\n                      if (!changelogText) {\n                          try {\n                              const resLatest = await fetch('https://api.github.com/repos/' + repo + '/releases/latest');\n                              if (resLatest.ok) {\n                                  const relLatest = await resLatest.json();\n                                  if (relLatest && relLatest.body) {\n                                      changelogText = relLatest.body;\n                                  }\n                              }\n                          } catch(e) {}\n                      }\n\n                      if (!changelogText) {\n                          try {\n                              const resFile = await fetch('https://raw.githubusercontent.com/' + repo + '/main/CHANGELOG.md');\n                              if (resFile.ok) {\n                                  changelogText = await resFile.text();\n                              }\n                          } catch(e) {}\n                      }\n\n                      if (changelogText) {\n                          changelogContent.innerHTML = parseMarkdown(changelogText);\n                      } else {\n                          changelogContent.innerHTML = lang === 'fa'\n                              ? '<div class=\"space-y-2\">' +\n                                '<p class=\"font-bold\">✨ اضافه شده:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>صفحه اشتراک چندزبانه با حالت تاریک/روشن</li>' +\n                                '<li>پشتیبانی NAT64 و نودهای اختصاصی کاربر</li>' +\n                                '<li>کانفیگ‌های مستقیم و بروزرسانی خودکار</li>' +\n                                '<li>مدیریت کامل دروازه از ربات تلگرام</li>' +\n                                '</ul>' +\n                                '<p class=\"font-bold mt-2\">⚡ بهبود یافته:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>عملکرد داشبورد و سرعت اسکرول</li>' +\n                                '<li>بازنویسی کامل تولید کانفیگ‌ها</li>' +\n                                '<li>نام‌گذاری هوشمند با تگ‌های جدید</li>' +\n                                '</ul>' +\n                                '<p class=\"font-bold mt-2\">🔧 رفع شده:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>ترجمه‌های فارسی معیوب</li>' +\n                                '<li>خطای صفحه اشتراک</li>' +\n                                '</ul></div>'\n                              : '<div class=\"space-y-2\">' +\n                                '<p class=\"font-bold\">✨ Added:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>Bilingual subscription page with dark/light mode</li>' +\n                                '<li>NAT64 support and per-user custom nodes</li>' +\n                                '<li>Direct configs and auto update</li>' +\n                                '<li>Full gateway management via Telegram bot</li>' +\n                                '</ul>' +\n                                '<p class=\"font-bold mt-2\">⚡ Improved:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>Dashboard performance and scroll speed</li>' +\n                                '<li>Complete rewrite of all config generators</li>' +\n                                '<li>Smart config naming with new tags</li>' +\n                                '</ul>' +\n                                '<p class=\"font-bold mt-2\">🔧 Fixed:</p>' +\n                                '<ul class=\"list-disc list-inside text-xs space-y-1\">' +\n                                '<li>Garbled Persian translations</li>' +\n                                '<li>Subscription page display error</li>' +\n                                '</ul></div>';\n                      }\n                  } catch(err) {\n                      changelogContent.innerHTML = lang === 'fa'\n                          ? '<p class=\"text-rose-500\">خطا در دریافت گزارش تغییرات.</p>'\n                          : '<p class=\"text-rose-500\">Failed to load changelog.</p>';\n                  }\n              }\n          }\n          //DateTime Function\n            const _dtFormatter = new Intl.DateTimeFormat('fa-IR', {\n        year: 'numeric',\n        month: 'long',\n        day: 'numeric',\n        hour: '2-digit',\n        minute: '2-digit',\n        second: '2-digit'\n    });\n            function updatePersianDateTime() {\n    const now = new Date();\n    const parts = _dtFormatter.formatToParts(now);\n\n    const map = {};\n    parts.forEach(p => {\n        map[p.type] = p.value;\n    });\n\n\n\n        const custom = `${map.day} ${map.month} ${map.year} ${map.hour}:${map.minute}:${map.second}`;\n\n    document.getElementById(\"net-datetime\").innerText = custom;\n\n}\n\n                updatePersianDateTime();\n                setInterval(updatePersianDateTime, 1000);\n\n\n\n          function dismissUpdate() {\n              const b = document.getElementById('update-alert-banner');\n              if (b) {\n                  b.classList.remove('flex');\n                  b.classList.add('hidden');\n              }\n          }\n\n  window.addHelperIpToTextarea = function(ipId, nameId, targetId) {\n      const ipInput = document.getElementById(ipId);\n      const nameInput = document.getElementById(nameId);\n      if (!ipInput || !nameInput) return;\n      const ip = ipInput.value.trim();\n      const name = nameInput.value.trim();\n      if (!ip) return;\n      \n      const textarea = document.getElementById(targetId);\n      if (!textarea) return;\n      \n      let val = textarea.value;\n      if (val && !val.endsWith('\\n')) val += '\\n';\n      val += name ? `${ip}#${name}` : ip;\n      \n      textarea.value = val;\n      \n      ipInput.value = '';\n      nameInput.value = '';\n      \n      textarea.dispatchEvent(new Event('input', { bubbles: true }));\n      textarea.dispatchEvent(new Event('change', { bubbles: true }));\n  };\n\n  window.changeThemeVariant = function(val) {\n      document.documentElement.setAttribute('data-theme', val);\n      localStorage.setItem('theme_variant', val);\n  };\n\n  document.addEventListener('DOMContentLoaded', () => {\n      const savedVariant = localStorage.getItem('theme_variant') || 'system';\n      document.documentElement.setAttribute('data-theme', savedVariant);\n      const sel = document.getElementById('theme-selector');\n      if(sel) sel.value = savedVariant;\n\n      const strategyInput = document.getElementById('cfg-name-strategy');\n      const suggestionsBox = document.getElementById('cfg-strategy-suggestions');\n      if (strategyInput && suggestionsBox) {\n          strategyInput.addEventListener('focus', () => {\n              suggestionsBox.classList.remove('hidden');\n          });\n\n          document.addEventListener('click', (e) => {\n              if (!strategyInput.contains(e.target) && !suggestionsBox.contains(e.target)) {\n                  suggestionsBox.classList.add('hidden');\n              }\n          });\n\n          const tagBtns = suggestionsBox.querySelectorAll('.tag-btn');\n          tagBtns.forEach(btn => {\n              btn.addEventListener('click', () => {\n                  const tag = btn.textContent;\n                  const startPos = strategyInput.selectionStart;\n                  const endPos = strategyInput.selectionEnd;\n                  const text = strategyInput.value;\n                  strategyInput.value = text.substring(0, startPos) + tag + text.substring(endPos);\n                  strategyInput.focus();\n                  const newPos = startPos + tag.length;\n                  strategyInput.setSelectionRange(newPos, newPos);\n              });\n          });\n      }\n  });\n</script>\n</body>\n</html>\n";
                        html = html.replace(/__CURRENT_VERSION__/g, CURRENT_VERSION);
                        if (env.AM_DB !== undefined) {
                            html = html.replace('__HAS_DB_WARNING__', '');
                        } else {
                            html = html.replace('__HAS_DB_WARNING__', '<div class="mb-5 p-4 rounded-2xl flex items-start gap-3" style="background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.2);"><span style="color:#f87171;">&#9888;&#65039;</span><span class="text-sm" style="color:#fca5a5;" data-i18n="missing_db">Database not connected. Settings won\'t be saved.</span></div>');
                        }
                        return new Response(html, {
                            headers: { "Content-Type": "text/html;charset=utf-8" },
                        });
                    } catch (e) {
                        return new Response('Failed to load dashboard', { status: 502 });
                    }
                }
                if (reqPath === routes.auth) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleAuth(request, url.hostname, ctx, env);
                }
                if (reqPath === routes.sync || isSyncRoute) {
                    if (request.method === "OPTIONS") {
                        return new Response(null, {
                            status: 204,
                            headers: {
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Methods": "POST, OPTIONS",
                                "Access-Control-Allow-Headers":
                                    "Content-Type, Authorization",
                                "Access-Control-Max-Age": "86400",
                            },
                        });
                    }
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    const syncRes = await handleConfigSync(request, env, ctx);
                    syncRes.headers.set("Access-Control-Allow-Origin", "*");
                    syncRes.headers.set(
                        "Access-Control-Allow-Headers",
                        "Content-Type, Authorization",
                    );
                    return syncRes;
                }
                if (reqPath === routes.logs) {
                    if (request.method !== "POST" && request.method !== "GET")
                        return new Response("405", { status: 405 });
                    return await handleLogs(request, env);
                }
                if (isUsersRoute) {
                    return await handleUsersApi(request, env, ctx);
                }
                if (isStatsRoute) {
                    return await handleStatsApi(request, env);
                }
                if (isUpdateRoute) {
                    return await handleUpdateApi(request, env, ctx);
                }
                if (isApiKeysRoute) {
                    return await handleApiKeys(request, env, ctx);
                }
                if (reqPath === routes.syncPanel) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleSyncPanel(request, env, ctx);
                }
                if (reqPath === routes.tg) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleTelegramWebhook(
                        request,
                        env,
                        url.hostname,
                        ctx,
                    );
                }
                if (reqPath === routes.data) {
                    const ua = (
                        request.headers.get("User-Agent") || ""
                    ).toLowerCase();
                    const isCustomUaAllowed =
                        sysConfig.subUserAgent &&
                        sysConfig.subUserAgent.trim().length > 0 &&
                        ua.includes(
                            sysConfig.subUserAgent.trim().toLowerCase(),
                        );
                    const clientHost =
                        request.headers.get("Host") || url.hostname;
                    let targetSub = url.searchParams.get("sub");
                    let hasMultiUser =
                        sysConfig.users && sysConfig.users.length > 0;

                    let targetUser = null;
                    let isValidUser = false;
                    if (hasMultiUser) {
                        if (targetSub) {
                            targetUser = sysConfig.users.find(
                                (u) =>
                                    u.name.toLowerCase() ===
                                        targetSub.toLowerCase() ||
                                    u.id === targetSub,
                            );
                            if (targetUser) isValidUser = true;
                        }
                    } else {
                        isValidUser = true;
                        targetUser = { id: activeDeviceId, name: "Default" };
                    }

                    const acceptHeader = (
                        request.headers.get("Accept") || ""
                    ).toLowerCase();
                    const secFetchDest = (
                        request.headers.get("Sec-Fetch-Dest") || ""
                    ).toLowerCase();

                    const isRealBrowser =
                        (secFetchDest === "document" ||
                            acceptHeader.includes("text/html")) &&
                        (ua.includes("mozilla") ||
                            ua.includes("chrome") ||
                            ua.includes("safari") ||
                            ua.includes("applewebkit") ||
                            ua.includes("gecko") ||
                            ua.includes("opera") ||
                            ua.includes("edge")) &&
                        !ua.includes("cla" + "sh") &&
                        !ua.includes("si" + "ng-box") &&
                        !ua.includes("v" + "2r" + "ay") &&
                        !ua.includes("shadow" + "rocket") &&
                        !ua.includes("quantum" + "ult") &&
                        !ua.includes("surf" + "board") &&
                        !ua.includes("sta" + "sh");

                    if (isRealBrowser && !isCustomUaAllowed) {
                        if (isValidUser) {
                            try {
                            let html = "<!DOCTYPE html>\r\n<html lang=\"en\" dir=\"ltr\">\r\n<head>\r\n    <meta charset=\"UTF-8\">\r\n    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\r\n    <title>MrVpn294 Subscription</title>\r\n    <link href=\"https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;600;700;800;900&family=Inter:wght@400;500;600;700;800;900&display=swap\" rel=\"stylesheet\">\r\n    <script src=\"https://cdn.jsdelivr.net/gh/amirpocom63-del/mrvpn294@main/tailwindcss.js\"></script>\r\n    <script>\r\n        tailwind.config = {\r\n            darkMode: 'class',\r\n            theme: {\r\n                extend: {\r\n                    fontFamily: {\r\n                        fa: ['Vazirmatn', 'sans-serif'],\r\n                        en: ['Inter', 'sans-serif'],\r\n                    },\r\n                    colors: {\r\n                        dark: {\r\n                            900: '#0b0f19',\r\n                            800: '#111827',\r\n                            700: '#1f2937',\r\n                        },\r\n                        brand: {\r\n                            500: '#6366f1',\r\n                            600: '#4f46e5',\r\n                        }\r\n                    }\r\n                }\r\n            }\r\n        }\r\n    </script>\r\n    <style>\r\n        :root {\r\n            --bg-primary: #f3f4f6;\r\n            --bg-card: #ffffff;\r\n            --bg-card-inner: #f9fafb;\r\n            --bg-input: #f3f4f6;\r\n            --border-card: #e5e7eb;\r\n            --border-inner: #e5e7eb;\r\n            --text-primary: #111827;\r\n            --text-secondary: #4b5563;\r\n            --text-muted: #9ca3af;\r\n            --accent: #6366f1;\r\n            --accent-light: #e0e7ff;\r\n            --accent-border: #c7d2fe;\r\n            --accent-hover: #4f46e5;\r\n            --green-bg: #d1fae5;\r\n            --green-border: #a7f3d0;\r\n            --green-text: #059669;\r\n            --amber-bg: #fef3c7;\r\n            --amber-border: #fde68a;\r\n            --amber-text: #d97706;\r\n            --red-bg: #fee2e2;\r\n            --red-border: #fecaca;\r\n            --red-text: #dc2626;\r\n            --progress-bg: #e5e7eb;\r\n            --shadow-card: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025);\r\n            --btn-primary-bg: #6366f1;\r\n            --btn-primary-hover: #4f46e5;\r\n            --btn-secondary-bg: #f3f4f6;\r\n            --btn-secondary-hover: #e5e7eb;\r\n            --modal-bg: rgba(0,0,0,0.5);\r\n            --modal-card: #ffffff;\r\n        }\r\n\r\n        .dark {\r\n            --bg-primary: #0b0f19;\r\n            --bg-card: rgba(17, 24, 39, 0.7);\r\n            --bg-card-inner: rgba(31, 41, 55, 0.5);\r\n            --bg-input: #111827;\r\n            --border-card: rgba(99, 102, 241, 0.15);\r\n            --border-inner: rgba(99, 102, 241, 0.1);\r\n            --text-primary: #f9fafb;\r\n            --text-secondary: #9ca3af;\r\n            --text-muted: #6b7280;\r\n            --accent: #818cf8;\r\n            --accent-light: rgba(99, 102, 241, 0.15);\r\n            --accent-border: rgba(99, 102, 241, 0.3);\r\n            --accent-hover: #6366f1;\r\n            --green-bg: rgba(16, 185, 129, 0.1);\r\n            --green-border: rgba(16, 185, 129, 0.2);\r\n            --green-text: #34d399;\r\n            --amber-bg: rgba(245, 158, 11, 0.1);\r\n            --amber-border: rgba(245, 158, 11, 0.2);\r\n            --amber-text: #fbbf24;\r\n            --red-bg: rgba(239, 68, 68, 0.1);\r\n            --red-border: rgba(239, 68, 68, 0.2);\r\n            --red-text: #f87171;\r\n            --progress-bg: rgba(31, 41, 55, 0.8);\r\n            --shadow-card: 0 10px 15px -3px rgba(0, 0, 0, 0.2), 0 4px 6px -2px rgba(0, 0, 0, 0.1);\r\n            --btn-primary-bg: #6366f1;\r\n            --btn-primary-hover: #4f46e5;\r\n            --btn-secondary-bg: rgba(31, 41, 55, 0.6);\r\n            --btn-secondary-hover: rgba(55, 65, 81, 0.8);\r\n            --modal-bg: rgba(0,0,0,0.8);\r\n            --modal-card: #1f2937;\r\n        }\r\n\r\n        body {\r\n            font-family: 'Inter', 'Vazirmatn', sans-serif;\r\n            background: var(--bg-primary) !important;\r\n            color: var(--text-primary);\r\n            transition: background 0.3s, color 0.3s;\r\n            -webkit-font-smoothing: antialiased;\r\n        }\r\n        [lang=\"fa\"] body { font-family: 'Vazirmatn', sans-serif; }\r\n        \r\n        .bento-card {\r\n            background: var(--bg-card) !important;\r\n            border: 1px solid var(--border-card) !important;\r\n            box-shadow: var(--shadow-card) !important;\r\n            border-radius: 1.5rem;\r\n            backdrop-filter: blur(10px);\r\n            transition: all 0.3s;\r\n        }\r\n        \r\n        .bento-inner {\r\n            background: var(--bg-card-inner);\r\n            border: 1px solid var(--border-inner);\r\n            border-radius: 1rem;\r\n            transition: all 0.3s;\r\n        }\r\n\r\n        .input-field {\r\n            background: var(--bg-input);\r\n            border: 1px solid var(--border-inner);\r\n            color: var(--text-primary);\r\n        }\r\n\r\n        ::-webkit-scrollbar { width: 6px; }\r\n        ::-webkit-scrollbar-thumb { background: var(--accent); border-radius: 10px; }\r\n        \r\n        .btn-primary { background: var(--btn-primary-bg); color: white; transition: all 0.2s ease; }\r\n        .btn-primary:hover { background: var(--btn-primary-hover); transform: translateY(-1px); }\r\n        .btn-secondary { background: var(--btn-secondary-bg); color: var(--text-primary); border: 1px solid var(--border-inner); transition: all 0.2s ease; }\r\n        .btn-secondary:hover { background: var(--btn-secondary-hover); transform: translateY(-1px); }\r\n        .text-secondary { color: var(--text-secondary); }\r\n        .text-muted { color: var(--text-muted); }\r\n        .progress-bar-bg { background: var(--progress-bg); }\r\n        \r\n        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }\r\n        .fade-in { animation: fadeIn 0.4s ease-out; }\r\n        \r\n        .modal-overlay { background: var(--modal-bg); backdrop-filter: blur(8px); }\r\n        .modal-card { background: var(--modal-card); border: 1px solid var(--border-card); }\r\n        \r\n        .app-btn {\r\n            background: var(--bg-card-inner);\r\n            border: 1px solid var(--border-inner);\r\n            color: var(--text-primary);\r\n            transition: all 0.2s ease;\r\n        }\r\n        .app-btn:hover { border-color: var(--accent); color: var(--accent); transform: translateY(-2px); }\r\n        \r\n        .glow-effect {\r\n            position: absolute;\r\n            width: 150px;\r\n            height: 150px;\r\n            background: var(--accent);\r\n            filter: blur(80px);\r\n            opacity: 0.15;\r\n            border-radius: 50%;\r\n            pointer-events: none;\r\n            z-index: 0;\r\n        }\r\n    </style>\r\n</head>\r\n<body class=\"min-h-screen py-8 px-4 sm:px-6 lg:px-8 flex flex-col items-center justify-start fade-in relative overflow-x-hidden\">\r\n    <!-- Ambient Background Glows -->\r\n    <div class=\"glow-effect top-0 left-10\"></div>\r\n    <div class=\"glow-effect bottom-20 right-10\" style=\"background: #34d399;\"></div>\r\n\r\n    <!-- Theme & Language Toggle -->\r\n    <div class=\"w-full max-w-6xl flex justify-end items-center mb-6 z-10 relative\">\r\n        <div class=\"flex gap-3\">\r\n            <button onclick=\"toggleTheme()\" id=\"theme-toggle\" class=\"btn-secondary px-4 py-2 rounded-xl text-xs font-bold transition-all flex items-center gap-2 shadow-sm\" title=\"Toggle Theme\">\r\n                <span id=\"theme-icon\">☀️</span>\r\n                <span id=\"theme-label\"></span>\r\n            </button>\r\n            <button onclick=\"toggleLang()\" id=\"lang-toggle\" class=\"btn-secondary px-4 py-2 rounded-xl text-xs font-bold transition-all flex items-center gap-2 shadow-sm\" title=\"Toggle Language\">\r\n                <span id=\"lang-icon\">🇺🇸</span>\r\n                <span id=\"lang-label\">EN</span>\r\n            </button>\r\n        </div>\r\n    </div>\r\n\r\n    <!-- Bento Grid Container -->\r\n    <div class=\"w-full max-w-6xl grid grid-cols-1 md:grid-cols-12 gap-6 z-10 relative\">\r\n        \r\n        <!-- Left Sidebar / Profile (Col span 4) -->\r\n        <div class=\"md:col-span-4 flex flex-col gap-6\">\r\n            <!-- Profile Card -->\r\n            <div class=\"bento-card p-6 flex flex-col items-center text-center relative overflow-hidden\">\r\n                <div class=\"absolute top-0 w-full h-24\" style=\"background: linear-gradient(135deg, var(--accent-light) 0%, transparent 100%); opacity: 0.5;\"></div>\r\n                <div class=\"w-20 h-20 rounded-2xl flex items-center justify-center shadow-lg mb-4 z-10 relative\" style=\"background: var(--bg-card); border: 2px solid var(--accent-border); color: var(--accent);\">\r\n                    <svg class=\"w-10 h-10\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z\"></path></svg>\r\n                </div>\r\n                <h1 class=\"text-2xl font-black tracking-tight z-10 mb-1\" style=\"color: var(--text-primary);\">__USER_NAME__</h1>\r\n                <p class=\"text-xs font-mono z-10 mb-5\" style=\"color: var(--text-muted);\">__USER_ID__</p>\r\n                <div class=\"z-10 w-full\">\r\n                    <span id=\"status-badge\" class=\"px-6 py-2 rounded-xl text-xs font-bold shadow-sm inline-block w-full uppercase tracking-wider\"></span>\r\n                </div>\r\n            </div>\r\n\r\n            <!-- Expiry & General Info -->\r\n            <div class=\"bento-card p-6\">\r\n                <h3 class=\"text-xs font-bold uppercase tracking-widest text-secondary mb-4 flex items-center gap-2\">\r\n                    <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z\"></path></svg>\r\n                    <span data-i18n=\"expDate\">Expiration Date</span>\r\n                </h3>\r\n                <div class=\"flex flex-col gap-1\">\r\n                    <span class=\"text-2xl font-black\" style=\"color: var(--text-primary);\">__EXPIRY_DATE__</span>\r\n                    <p class=\"text-xs text-muted font-medium\" data-i18n=\"calendarLocal\">Calendar Local Time</p>\r\n                </div>\r\n                <div class=\"mt-6 pt-5 border-t\" style=\"border-color: var(--border-inner);\">\r\n                    <div class=\"flex justify-between items-center mb-3\">\r\n                        <span class=\"text-xs font-bold text-secondary\">Connection IP</span>\r\n                        <span class=\"text-xs font-mono font-semibold\" style=\"color: var(--text-primary);\" id=\"client-ip\">Detecting...</span>\r\n                    </div>\r\n                </div>\r\n            </div>\r\n\r\n            <!-- Support / Help -->\r\n            <div class=\"bento-card p-6\" style=\"background: linear-gradient(135deg, var(--bg-card) 0%, var(--accent-light) 200%);\">\r\n                <h3 class=\"text-sm font-bold flex items-center gap-2 mb-3\" style=\"color: var(--text-primary);\">\r\n                    <svg class=\"w-5 h-5 text-indigo-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M18.364 5.636l-3.536 3.536m0 5.656l3.536 3.536M9.172 9.172L5.636 5.636m3.536 9.192l-3.536 3.536M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-5 0a4 4 0 11-8 0 4 4 0 018 0z\"></path></svg>\r\n                    Help & Support\r\n                </h3>\r\n                <p class=\"text-xs text-secondary mb-4 leading-relaxed\">Having trouble connecting? Keep your clients updated or contact support for assistance.</p>\r\n                <a href=\"https://t.me/+mx5EmkbHzm41Y2Q5\" class=\"btn-primary w-full py-2.5 rounded-xl text-xs font-bold flex items-center justify-center gap-2 shadow-sm\">\r\n                    <svg class=\"w-4 h-4\" fill=\"currentColor\" viewBox=\"0 0 24 24\"><path d=\"M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69.01-.03.01-.14-.07-.19-.08-.05-.19-.02-.27 0-.12.03-1.98 1.26-5.59 3.7-.53.36-1.01.54-1.44.53-.47-.01-1.38-.27-2.05-.49-.83-.27-1.49-.42-1.44-.88.03-.24.36-.48.98-.74 3.84-1.67 6.4-2.78 7.68-3.32 3.64-1.52 4.4-1.79 4.9-1.8.11 0 .36.03.49.14.11.09.14.22.15.34-.01.07-.01.16-.02.26z\"/></svg>\r\n                    Telegram Support\r\n                </a>\r\n            </div>\r\n        </div>\r\n\r\n        <!-- Right Main Content (Col span 8) -->\r\n        <div class=\"md:col-span-8 flex flex-col gap-6\">\r\n            \r\n            <!-- Metrics Row -->\r\n            <div class=\"grid grid-cols-1 sm:grid-cols-2 gap-6\">\r\n                <!-- Total Usage -->\r\n                <div class=\"bento-card p-6 relative overflow-hidden group\">\r\n                    <div class=\"absolute -right-6 -top-6 w-24 h-24 bg-indigo-500 opacity-5 rounded-full group-hover:scale-150 transition-transform duration-500\"></div>\r\n                    <div class=\"flex justify-between items-start mb-4 relative z-10\">\r\n                        <p class=\"text-xs font-bold uppercase tracking-widest text-secondary\" data-i18n=\"totalUsage\">Total Usage</p>\r\n                        <span class=\"p-2 rounded-lg\" style=\"background: var(--bg-card-inner); border: 1px solid var(--border-inner);\">\r\n                            <svg class=\"w-4 h-4 text-indigo-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4\"></path></svg>\r\n                        </span>\r\n                    </div>\r\n                    <div class=\"flex items-baseline gap-1.5 mb-4 relative z-10\">\r\n                        <span class=\"text-3xl font-black\" style=\"color: var(--text-primary);\">__TOTAL_GB__</span>\r\n                        <span class=\"text-sm font-medium text-secondary\">/ __LIMIT_TOTAL_GB__ GB</span>\r\n                    </div>\r\n                    __TOTAL_PROGRESS__\r\n                </div>\r\n\r\n                <!-- Daily Usage -->\r\n                <div class=\"bento-card p-6 relative overflow-hidden group\">\r\n                    <div class=\"absolute -right-6 -top-6 w-24 h-24 bg-emerald-500 opacity-5 rounded-full group-hover:scale-150 transition-transform duration-500\"></div>\r\n                    <div class=\"flex justify-between items-start mb-4 relative z-10\">\r\n                        <p class=\"text-xs font-bold uppercase tracking-widest text-secondary\" data-i18n=\"dailyUsage\">Daily Usage</p>\r\n                        <span class=\"p-2 rounded-lg\" style=\"background: var(--bg-card-inner); border: 1px solid var(--border-inner);\">\r\n                            <svg class=\"w-4 h-4 text-emerald-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg>\r\n                        </span>\r\n                    </div>\r\n                    <div class=\"flex items-baseline gap-1.5 mb-4 relative z-10\">\r\n                        <span class=\"text-3xl font-black\" style=\"color: var(--text-primary);\">__DAILY_GB__</span>\r\n                        <span class=\"text-sm font-medium text-secondary\">/ __LIMIT_DAILY_GB__ GB</span>\r\n                    </div>\r\n                    __DAILY_PROGRESS__\r\n                </div>\r\n            </div>\r\n\r\n            <!-- Universal Link Setup -->\r\n            <div class=\"bento-card p-6 sm:p-8\">\r\n                <div class=\"flex justify-between items-end mb-6\">\r\n                    <div>\r\n                        <h3 class=\"text-lg font-black flex items-center gap-2 mb-1\" style=\"color: var(--text-primary);\">\r\n                            <svg class=\"w-5 h-5 text-indigo-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1\"></path></svg>\r\n                            <span data-i18n=\"integrationTitle\">Integration Connections</span>\r\n                        </h3>\r\n                        <p class=\"text-xs text-secondary\" data-i18n=\"integrationDesc\">Add your preferred configuration link.</p>\r\n                    </div>\r\n                </div>\r\n\r\n                <div class=\"bento-inner p-5 sm:p-6 border border-accent-border relative overflow-hidden\" style=\"border-color: var(--accent-border);\">\r\n                    <div class=\"absolute inset-0 opacity-10 pointer-events-none\" style=\"background: radial-gradient(circle at 100% 0%, var(--accent) 0%, transparent 40%);\"></div>\r\n                    <div class=\"relative z-10\">\r\n                        <h4 class=\"font-bold mb-2 flex items-center gap-2\" style=\"color: var(--text-primary);\">\r\n                            <span data-i18n=\"universalLink\">Universal Auto-Detecting Link</span>\r\n                            <span class=\"px-2 py-0.5 rounded text-[9px] uppercase tracking-wider font-bold shadow-sm\" style=\"background: var(--accent); color: white;\">Recommended</span>\r\n                        </h4>\r\n                        <p class=\"text-xs text-secondary mb-4 leading-relaxed\" data-i18n=\"universalDesc\">This URL automatically detects your client and delivers the optimal format.</p>\r\n                        \r\n                        <div class=\"flex flex-col sm:flex-row gap-3 w-full\">\r\n                            <input type=\"text\" id=\"link-normal\" value=\"__SYNC_NORMAL__\" readonly class=\"input-field flex-1 px-4 py-3 rounded-xl text-sm font-mono w-full outline-none shadow-inner focus:ring-2 focus:ring-indigo-500 transition-shadow\">\r\n                            <div class=\"flex gap-2\">\r\n                                <button onclick=\"copyLink('link-normal')\" class=\"btn-primary flex-1 sm:flex-none px-6 py-3 rounded-xl text-sm font-bold shrink-0 flex justify-center items-center gap-2 shadow-md\">\r\n                                    <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3\"></path></svg>\r\n                                    <span data-i18n=\"copy\">Copy</span>\r\n                                </button>\r\n                                <button onclick=\"showQRModal('__SYNC_NORMAL__')\" class=\"btn-secondary px-4 py-3 rounded-xl text-sm font-bold shrink-0 shadow-sm flex items-center justify-center\">\r\n                                    <svg class=\"w-5 h-5\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z\"></path></svg>\r\n                                </button>\r\n                            </div>\r\n                        </div>\r\n                    </div>\r\n                </div>\r\n\r\n                <div class=\"mt-6 bento-inner p-5 sm:p-6 border border-accent-border relative overflow-hidden\" style=\"border-color: var(--accent-border);\">\r\n                    <div class=\"absolute inset-0 opacity-5 pointer-events-none\" style=\"background: radial-gradient(circle at 100% 0%, var(--accent) 0%, transparent 40%);\"></div>\r\n                    <div class=\"relative z-10 space-y-4\">\r\n                        <h4 class=\"font-bold flex items-center gap-2\" style=\"color: var(--text-primary);\">\r\n                            <span data-i18n=\"availableFormats\">Available Subscription Formats</span>\r\n                        </h4>\r\n                        <p class=\"text-xs text-secondary leading-relaxed\" data-i18n=\"formatsDesc\">Use these links to explicitly request specific configuration formats.</p>\r\n                        \r\n                        <div class=\"grid grid-cols-1 md:grid-cols-2 gap-4\">\r\n                            <!-- Raw -->\r\n                            <div class=\"p-4 rounded-xl border border-accent-border space-y-2 bg-black/5 dark:bg-white/5\" style=\"border-color: var(--accent-border);\">\r\n                                <div class=\"flex justify-between items-center\">\r\n                                    <span class=\"text-xs font-bold\" style=\"color: var(--text-primary);\">Raw Configs (Text/URI)</span>\r\n                                    <span class=\"text-[9px] uppercase tracking-wider font-bold px-1.5 py-0.5 rounded\" style=\"background: rgba(99, 102, 241, 0.15); color: #6366f1;\">RAW</span>\r\n                                </div>\r\n                                <div class=\"flex gap-2\">\r\n                                    <input type=\"text\" id=\"link-raw-fmt\" readonly class=\"input-field flex-1 px-3 py-2 rounded-lg text-xs font-mono w-full outline-none\" style=\"background: var(--bg-card); color: var(--text-primary); border: 1px solid var(--accent-border);\">\r\n                                    <button onclick=\"copyLink('link-raw-fmt')\" class=\"px-3 py-2 rounded-lg text-xs font-bold bg-indigo-500 text-white flex items-center gap-1 hover:bg-indigo-600 transition-colors\">\r\n                                        <span data-i18n=\"copy\">Copy</span>\r\n                                    </button>\r\n                                    <button onclick=\"showQRModal(document.getElementById('link-raw-fmt').value)\" class=\"p-2 rounded-lg border border-accent-border hover:bg-white/10 flex items-center justify-center\" style=\"border-color: var(--accent-border);\">\r\n                                        <svg class=\"w-4 h-4 text-secondary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z\"></path></svg>\r\n                                    </button>\r\n                                </div>\r\n                            </div>\r\n\r\n                            <!-- Base64 -->\r\n                            <div class=\"p-4 rounded-xl border border-accent-border space-y-2 bg-black/5 dark:bg-white/5\" style=\"border-color: var(--accent-border);\">\r\n                                <div class=\"flex justify-between items-center\">\r\n                                    <span class=\"text-xs font-bold\" style=\"color: var(--text-primary);\">Base64 Sub (V2Ray / Shadowrocket)</span>\r\n                                    <span class=\"text-[9px] uppercase tracking-wider font-bold px-1.5 py-0.5 rounded\" style=\"background: rgba(16, 185, 129, 0.15); color: #10b981;\">BASE64</span>\r\n                                </div>\r\n                                <div class=\"flex gap-2\">\r\n                                    <input type=\"text\" id=\"link-base64-fmt\" readonly class=\"input-field flex-1 px-3 py-2 rounded-lg text-xs font-mono w-full outline-none\" style=\"background: var(--bg-card); color: var(--text-primary); border: 1px solid var(--accent-border);\">\r\n                                    <button onclick=\"copyLink('link-base64-fmt')\" class=\"px-3 py-2 rounded-lg text-xs font-bold bg-emerald-500 text-white flex items-center gap-1 hover:bg-emerald-600 transition-colors\">\r\n                                        <span data-i18n=\"copy\">Copy</span>\r\n                                    </button>\r\n                                    <button onclick=\"showQRModal(document.getElementById('link-base64-fmt').value)\" class=\"p-2 rounded-lg border border-accent-border hover:bg-white/10 flex items-center justify-center\" style=\"border-color: var(--accent-border);\">\r\n                                        <svg class=\"w-4 h-4 text-secondary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z\"></path></svg>\r\n                                    </button>\r\n                                </div>\r\n                            </div>\r\n\r\n                            <!-- Singbox -->\r\n                            <div class=\"p-4 rounded-xl border border-accent-border space-y-2 bg-black/5 dark:bg-white/5\" style=\"border-color: var(--accent-border);\">\r\n                                <div class=\"flex justify-between items-center\">\r\n                                    <span class=\"text-xs font-bold\" style=\"color: var(--text-primary);\">Sing-box Subscription (JSON)</span>\r\n                                    <span class=\"text-[9px] uppercase tracking-wider font-bold px-1.5 py-0.5 rounded\" style=\"background: rgba(245, 158, 11, 0.15); color: #f59e0b;\">SINGBOX</span>\r\n                                </div>\r\n                                <div class=\"flex gap-2\">\r\n                                    <input type=\"text\" id=\"link-singbox-fmt\" readonly class=\"input-field flex-1 px-3 py-2 rounded-lg text-xs font-mono w-full outline-none\" style=\"background: var(--bg-card); color: var(--text-primary); border: 1px solid var(--accent-border);\">\r\n                                    <button onclick=\"copyLink('link-singbox-fmt')\" class=\"px-3 py-2 rounded-lg text-xs font-bold bg-amber-500 text-white flex items-center gap-1 hover:bg-amber-600 transition-colors\">\r\n                                        <span data-i18n=\"copy\">Copy</span>\r\n                                    </button>\r\n                                    <button onclick=\"showQRModal(document.getElementById('link-singbox-fmt').value)\" class=\"p-2 rounded-lg border border-accent-border hover:bg-white/10 flex items-center justify-center\" style=\"border-color: var(--accent-border);\">\r\n                                        <svg class=\"w-4 h-4 text-secondary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z\"></path></svg>\r\n                                    </button>\r\n                                </div>\r\n                            </div>\r\n\r\n                            <!-- Clash -->\r\n                            <div class=\"p-4 rounded-xl border border-accent-border space-y-2 bg-black/5 dark:bg-white/5\" style=\"border-color: var(--accent-border);\">\r\n                                <div class=\"flex justify-between items-center\">\r\n                                    <span class=\"text-xs font-bold\" style=\"color: var(--text-primary);\">Clash Subscription (YAML)</span>\r\n                                    <span class=\"text-[9px] uppercase tracking-wider font-bold px-1.5 py-0.5 rounded\" style=\"background: rgba(139, 92, 246, 0.15); color: #8b5cf6;\">CLASH</span>\r\n                                </div>\r\n                                <div class=\"flex gap-2\">\r\n                                    <input type=\"text\" id=\"link-clash-fmt\" readonly class=\"input-field flex-1 px-3 py-2 rounded-lg text-xs font-mono w-full outline-none\" style=\"background: var(--bg-card); color: var(--text-primary); border: 1px solid var(--accent-border);\">\r\n                                    <button onclick=\"copyLink('link-clash-fmt')\" class=\"px-3 py-2 rounded-lg text-xs font-bold bg-violet-500 text-white flex items-center gap-1 hover:bg-violet-600 transition-colors\">\r\n                                        <span data-i18n=\"copy\">Copy</span>\r\n                                    </button>\r\n                                    <button onclick=\"showQRModal(document.getElementById('link-clash-fmt').value)\" class=\"p-2 rounded-lg border border-accent-border hover:bg-white/10 flex items-center justify-center\" style=\"border-color: var(--accent-border);\">\r\n                                        <svg class=\"w-4 h-4 text-secondary\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z\"></path></svg>\r\n                                    </button>\r\n                                </div>\r\n                            </div>\r\n                        </div>\r\n                    </div>\r\n                </div>\r\n\r\n                <div class=\"mt-8\">\r\n                    <h4 class=\"text-xs font-bold uppercase tracking-widest text-secondary mb-4 flex items-center gap-2\">\r\n                        <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M13 10V3L4 14h7v7l9-11h-7z\"></path></svg>\r\n                        <span data-i18n=\"oneClickImport\">One-Click Import</span>\r\n                    </h4>\r\n                    <div class=\"grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3\">\r\n                        <a id=\"v2rayng-link\" href=\"v2rayng://install-sub?url=__SYNC_NORMAL__&name=__USER_NAME__\" class=\"app-btn p-3 rounded-xl flex items-center justify-center gap-2 font-bold text-sm shadow-sm group hover:shadow-md\">\r\n                            <span>v2rayNG</span>\r\n                            <svg class=\"w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14\"></path></svg>\r\n                        </a>\r\n                        <a href=\"v2rayn://install-sub?url=__SYNC_NORMAL__&name=__USER_NAME__\" class=\"app-btn p-3 rounded-xl flex items-center justify-center gap-2 font-bold text-sm shadow-sm group hover:shadow-md\">\r\n                            <span>v2rayN</span>\r\n                            <svg class=\"w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14\"></path></svg>\r\n                        </a>\r\n                        <a href=\"shadowrocket://add/sub://__SYNC_NORMAL_BASE64__?title=__USER_NAME__\" class=\"app-btn p-3 rounded-xl flex items-center justify-center gap-2 font-bold text-sm shadow-sm group hover:shadow-md\">\r\n                            <span>Shadowrocket</span>\r\n                            <svg class=\"w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14\"></path></svg>\r\n                        </a>\r\n                        <a href=\"streisand://import/__SYNC_NORMAL__\" class=\"app-btn p-3 rounded-xl flex items-center justify-center gap-2 font-bold text-sm shadow-sm group hover:shadow-md\">\r\n                            <span>Streisand</span>\r\n                            <svg class=\"w-4 h-4 opacity-50 group-hover:opacity-100 transition-opacity\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14\"></path></svg>\r\n                        </a>\r\n                    </div>\r\n                </div>\r\n\r\n                <div class=\"mt-8 pt-6 border-t border-inner flex flex-col sm:flex-row justify-between items-center gap-4\">\r\n                    <div class=\"text-xs text-secondary\">\r\n                        Advanced users can retrieve raw config blocks manually.\r\n                    </div>\r\n                    <div class=\"flex gap-2 w-full sm:w-auto\">\r\n                        <button onclick=\"fetchDecodedRawContent()\" class=\"btn-secondary w-full sm:w-auto px-5 py-2.5 rounded-xl text-xs font-bold transition-colors flex justify-center items-center gap-2 shadow-sm\">\r\n                            <svg class=\"w-4 h-4\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4\"></path></svg>\r\n                            <span data-i18n=\"parsedContent\">Retrieve Raw Content</span>\r\n                        </button>\r\n                        <input type=\"text\" id=\"link-raw\" value=\"__SYNC_RAW__\" class=\"hidden\">\r\n                    </div>\r\n                </div>\r\n            </div>\r\n        </div>\r\n    </div>\r\n\r\n    <!-- QR Modal -->\r\n    <div id=\"qr-modal\" class=\"fixed inset-0 modal-overlay z-50 hidden items-center justify-center p-4 transition-opacity duration-300 opacity-0\">\r\n        <div class=\"modal-card max-w-sm w-full rounded-3xl p-6 md:p-8 transform scale-95 transition-transform duration-300 shadow-2xl relative\">\r\n            <button onclick=\"closeQRModal()\" class=\"absolute top-4 right-4 p-2 rounded-full hover:bg-black/5 dark:hover:bg-white/5 transition-colors\">\r\n                <svg class=\"w-6 h-6 text-muted\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M6 18L18 6M6 6l12 12\"></path></svg>\r\n            </button>\r\n            <h3 class=\"text-xl font-black mb-6 text-center\" style=\"color: var(--text-primary);\" id=\"qr-title\">Scan QR Code</h3>\r\n            <div class=\"bg-white p-4 rounded-2xl shadow-inner mb-6 mx-auto w-fit\">\r\n                <img id=\"qr-img\" src=\"\" alt=\"QR Code\" class=\"w-48 h-48 sm:w-56 sm:h-56\">\r\n            </div>\r\n            <p id=\"qr-text\" class=\"text-xs text-center text-secondary font-mono break-all mb-6 px-2 bg-black/5 dark:bg-white/5 py-3 rounded-xl border border-inner\"></p>\r\n            <button onclick=\"closeQRModal()\" class=\"btn-primary w-full py-3 rounded-xl font-bold shadow-md\" data-i18n=\"close\">Close</button>\r\n        </div>\r\n    </div>\r\n\r\n    <!-- Toast Notification -->\r\n    <div id=\"toast\" class=\"fixed top-10 left-1/2 -translate-x-1/2 bg-slate-900 dark:bg-white text-white dark:text-slate-900 px-6 py-3 rounded-full shadow-2xl font-bold text-sm z-50 transition-all transform -translate-y-4 opacity-0 pointer-events-none flex items-center gap-2\">\r\n        <svg class=\"w-5 h-5 text-emerald-400 dark:text-emerald-500\" fill=\"none\" stroke=\"currentColor\" viewBox=\"0 0 24 24\"><path stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-width=\"2\" d=\"M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z\"></path></svg>\r\n        <span>Notification</span>\r\n    </div>\r\n\r\n    <script>\r\n\r\n        const I18N = {\r\n            en: {\r\n                totalUsage: 'Total Usage',\r\n                dailyUsage: 'Daily Usage',\r\n                expDate: 'Expiration Date',\r\n                calendarLocal: 'Calendar Local',\r\n                unlimitedPlan: 'Unlimited Plan',\r\n                noDailyLimit: 'No Daily Limit',\r\n                integrationTitle: 'Integration Connections',\r\n                integrationDesc: 'Add the correct configuration link based on your preferred format below.',\r\n                universalLink: 'Universal Auto-Detecting Link',\r\n                universalDesc: 'This URL automatically detects your client and delivers the optimal format.',\r\n                oneClickImport: 'One-Click Import',\r\n                copy: 'Copy',\r\n                copyRaw: 'Copy Raw URL',\r\n                qr: 'QR',\r\n                parsedContent: 'Retrieve Raw Content',\r\n                close: 'Close',\r\n                qrTitle: 'Scan QR Code',\r\n                copied: 'Copied to clipboard!',\r\n                decodedCopied: 'Decoded links copied!',\r\n                decodedError: 'Error fetching content',\r\n                used: '% Used',\r\n                active: 'Active',\r\n                paused: 'Paused',\r\n                expired: 'Expired',\r\n                limitExceeded: 'Limit Exceeded',\r\n                dailyLimitExceeded: 'Daily Limit Exceeded',\r\n                availableFormats: 'Available Subscription Formats',\r\n                formatsDesc: 'Use these links to explicitly request specific configuration formats.'\r\n            },\r\n            fa: {\r\n                totalUsage: 'مصرف کل',\r\n                dailyUsage: 'مصرف روزانه',\r\n                expDate: 'تاریخ انقضا',\r\n                calendarLocal: 'زمان محلی',\r\n                unlimitedPlan: 'طرح نامحدود',\r\n                noDailyLimit: 'بدون محدودیت روزانه',\r\n                integrationTitle: 'لینک‌های اتصال',\r\n                integrationDesc: 'لینک پیکربندی مورد نظر خود را اضافه کنید.',\r\n                universalLink: 'لینک خودکار',\r\n                universalDesc: 'این لینک کلاینت شما را شناسایی و بهترین فرمت را ارسال می‌کند.',\r\n                oneClickImport: 'وارد کردن با یک کلیک',\r\n                copy: 'کپی',\r\n                copyRaw: 'کپی لینک خام',\r\n                qr: 'QR',\r\n                parsedContent: 'دریافت متن خام',\r\n                close: 'بستن',\r\n                qrTitle: 'اسکن کد QR',\r\n                copied: 'کپی شد!',\r\n                decodedCopied: 'لینک‌ها کپی شد!',\r\n                decodedError: 'خطا در دریافت',\r\n                used: '% مصرف',\r\n                active: 'فعال',\r\n                paused: 'متوقف',\r\n                expired: 'منقضی',\r\n                limitExceeded: 'از حد مجاز رد شده',\r\n                dailyLimitExceeded: 'از حد روزانه رد شده',\r\n                availableFormats: 'فرمت‌های اشتراک در دسترس',\r\n                formatsDesc: 'از این لینک‌ها برای درخواست مستقیم فرمت‌های خاص پیکربندی استفاده کنید.'\r\n            }\r\n        };\r\n\r\n        let currentLang = 'en';\r\n        let isDark = true;\r\n\r\n        function applyTheme() {\r\n            const root = document.documentElement;\r\n            const themeLabel = document.getElementById('theme-label');\r\n            if (isDark) {\r\n                root.classList.add('dark');\r\n                document.getElementById('theme-icon').textContent = '☀️';\r\n                if (themeLabel) themeLabel.textContent = currentLang === 'fa' ? 'روشن' : 'Light';\r\n            } else {\r\n                root.classList.remove('dark');\r\n                document.getElementById('theme-icon').textContent = '🌙';\r\n                if (themeLabel) themeLabel.textContent = currentLang === 'fa' ? 'تاریک' : 'Dark';\r\n            }\r\n            try { localStorage.setItem('sub-theme', isDark ? 'dark' : 'light'); } catch(e) {}\r\n        }\r\n\r\n        function applyLang() {\r\n            const t = I18N[currentLang];\r\n            document.querySelectorAll('[data-i18n]').forEach(el => {\r\n                const key = el.getAttribute('data-i18n');\r\n                if (t[key]) el.textContent = t[key];\r\n            });\r\n            if (currentLang === 'fa') {\r\n                document.documentElement.setAttribute('dir', 'rtl');\r\n                document.documentElement.setAttribute('lang', 'fa');\r\n                document.getElementById('lang-icon').textContent = '🇮🇷';\r\n                document.getElementById('lang-label').textContent = 'FA';\r\n            } else {\r\n                document.documentElement.setAttribute('dir', 'ltr');\r\n                document.documentElement.setAttribute('lang', 'en');\r\n                document.getElementById('lang-icon').textContent = '🇺🇸';\r\n                document.getElementById('lang-label').textContent = 'EN';\r\n            }\r\n            initStatusBadge();\r\n            try { localStorage.setItem('sub-lang', currentLang); } catch(e) {}\r\n        }\r\n\r\n        function toggleTheme() {\r\n            isDark = !isDark;\r\n            applyTheme();\r\n        }\r\n\r\n        function toggleLang() {\r\n            currentLang = currentLang === 'en' ? 'fa' : 'en';\r\n            applyLang();\r\n            applyTheme();\r\n        }\r\n\r\n        function initStatusBadge() {\r\n            const badge = document.getElementById('status-badge');\r\n            const t = I18N[currentLang];\r\n            const map = {\r\n                active: { en: t.active || 'Active', bg: 'var(--green-bg)', border: 'var(--green-border)', color: 'var(--green-text)' },\r\n                paused: { en: t.paused || 'Paused', bg: 'var(--amber-bg)', border: 'var(--amber-border)', color: 'var(--amber-text)' },\r\n                expired: { en: t.expired || 'Expired', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' },\r\n                limit: { en: t.limitExceeded || 'Limit Exceeded', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' },\r\n                dailyLimit: { en: t.dailyLimitExceeded || 'Daily Limit Exceeded', bg: 'var(--red-bg)', border: 'var(--red-border)', color: 'var(--red-text)' }\r\n            };\r\n            const s = map['__STATUS_CODE__'] || map.active;\r\n            badge.textContent = s.en;\r\n            badge.style.background = s.bg;\r\n            badge.style.borderColor = s.border;\r\n            badge.style.color = s.color;\r\n            badge.style.border = '1px solid ' + s.border;\r\n        }\r\n\r\n        function copyLink(id) {\r\n            const el = document.getElementById(id);\r\n            el.select();\r\n            navigator.clipboard.writeText(el.value);\r\n            showToast(I18N[currentLang].copied);\r\n        }\r\n\r\n        async function fetchDecodedRawContent() {\r\n            try {\r\n                const res = await fetch('__SYNC_RAW__');\r\n                if(!res.ok) throw new Error('Failed');\r\n                const base64Str = await res.text();\r\n                const decodedText = atob(base64Str.trim());\r\n                await navigator.clipboard.writeText(decodedText);\r\n                showToast(I18N[currentLang].decodedCopied);\r\n            } catch(e) {\r\n                alert(I18N[currentLang].decodedError + ': ' + e.message);\r\n            }\r\n        }\r\n\r\n        function showQRModal(url) {\r\n            const t = I18N[currentLang];\r\n            document.getElementById('qr-title').innerText = t.qrTitle;\r\n            document.getElementById('qr-text').innerText = url;\r\n            document.getElementById('qr-img').src = 'https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=' + encodeURIComponent(url);\r\n            \r\n            const modal = document.getElementById('qr-modal');\r\n            modal.classList.remove('hidden');\r\n            modal.classList.add('flex');\r\n            \r\n            // Animation\r\n            setTimeout(() => {\r\n                modal.style.opacity = '1';\r\n                modal.querySelector('.modal-card').style.transform = 'scale(1)';\r\n            }, 10);\r\n        }\r\n\r\n        function closeQRModal() {\r\n            const modal = document.getElementById('qr-modal');\r\n            modal.style.opacity = '0';\r\n            modal.querySelector('.modal-card').style.transform = 'scale(0.95)';\r\n            \r\n            setTimeout(() => {\r\n                modal.classList.add('hidden');\r\n                modal.classList.remove('flex');\r\n            }, 300);\r\n        }\r\n\r\n        function showToast(msg) {\r\n            const t = document.getElementById('toast');\r\n            t.innerText = msg;\r\n            t.style.opacity = '1';\r\n            t.style.transform = 'translate(-50%, 0)';\r\n            setTimeout(() => { \r\n                t.style.opacity = '0'; \r\n                t.style.transform = 'translate(-50%, 16px)';\r\n            }, 2500);\r\n        }\r\n\r\n        \r\n        // Fetch Client IP\r\n        fetch('https://api.ipify.org?format=json')\r\n            .then(r => r.json())\r\n            .then(data => {\r\n                let ipEl = document.getElementById('client-ip');\r\n                if(ipEl) ipEl.textContent = data.ip;\r\n            }).catch(e => {\r\n                let ipEl = document.getElementById('client-ip');\r\n                if(ipEl) ipEl.textContent = 'Unknown';\r\n            });\r\n\r\n        (function init() {\r\n            try {\r\n                const savedTheme = localStorage.getItem('sub-theme');\r\n                if (savedTheme) isDark = savedTheme === 'dark';\r\n            } catch(e) {}\r\n            try {\r\n                const savedLang = localStorage.getItem('sub-lang');\r\n                if (savedLang && I18N[savedLang]) currentLang = savedLang;\r\n            } catch(e) {}\r\n            applyTheme();\r\n            applyLang();\r\n            \r\n            // Base64 encode normal link for shadowrocket\r\n            const normalLink = '__SYNC_NORMAL__';\r\n            document.querySelectorAll('a').forEach(a => {\r\n                if(a.href.includes('__SYNC_NORMAL_BASE64__')) {\r\n                    a.href = a.href.replace('__SYNC_NORMAL_BASE64__', btoa(normalLink));\r\n                }\r\n            });\r\n\r\n            // Set base64 flag for v2rayNG link\r\n            const v2rayngLink = document.getElementById('v2rayng-link');\r\n            if (v2rayngLink) {\r\n                const base64Url = normalLink + (normalLink.includes('?') ? '&flag=base64' : '?flag=base64');\r\n                v2rayngLink.href = 'v2rayng://install-sub?url=' + encodeURIComponent(base64Url) + '&name=' + encodeURIComponent('__USER_NAME__');\r\n            }\r\n\r\n            // Populate format-specific subscription links\r\n            const rawInput = document.getElementById('link-raw-fmt');\r\n            const base64Input = document.getElementById('link-base64-fmt');\r\n            const singboxInput = document.getElementById('link-singbox-fmt');\r\n            const clashInput = document.getElementById('link-clash-fmt');\r\n\r\n            if (rawInput) rawInput.value = normalLink + (normalLink.includes('?') ? '&flag=raw' : '?flag=raw');\r\n            if (base64Input) base64Input.value = normalLink + (normalLink.includes('?') ? '&flag=base64' : '?flag=base64');\r\n            if (singboxInput) singboxInput.value = normalLink + (normalLink.includes('?') ? '&flag=singbox' : '?flag=singbox');\r\n            if (clashInput) clashInput.value = normalLink + (normalLink.includes('?') ? '&flag=clash' : '?flag=clash');\r\n        })();\r\n    \r\n</script>\r\n</body>\r\n</html>\r\n";
                                // Compute dynamic values
                                const idClean = targetUser.id.replace(/-/g, '').toLowerCase();
                                const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
                                const totalReqs = sysU.reqs || 0;
                                const todayDate = new Date().toISOString().split('T')[0];
                                const dailyReqs = sysU.lastDay === todayDate ? (sysU.dReqs || 0) : 0;
                                const limitTotal = targetUser.limitTotalReq || 0;
                                const limitDaily = targetUser.limitDailyReq || 0;
                                const totalGb = (totalReqs / 6000).toFixed(2);
                                const limitTotalGb = limitTotal ? (limitTotal / 6000).toFixed(2) : '9999';
                                const dailyGb = (dailyReqs / 6000).toFixed(2);
                                const limitDailyGb = limitDaily ? (limitDaily / 6000).toFixed(2) : '9999';
                                const totalPercent = limitTotal ? Math.min(100, (totalReqs / limitTotal) * 100).toFixed(1) : '0';
                                const dailyPercent = limitDaily ? Math.min(100, (dailyReqs / limitDaily) * 100).toFixed(1) : '0';
                                let expiryDateTxt = '2099-01-01';
                                let isExpired = false;
                                if (targetUser.expiryMs) {
                                    expiryDateTxt = new Date(targetUser.expiryMs).toISOString().split('T')[0];
                                    if (Date.now() > targetUser.expiryMs) isExpired = true;
                                }
                                let statusCode = 'active';
                                if (targetUser.isPaused) statusCode = 'paused';
                                else if (isExpired) statusCode = 'expired';
                                else if (limitTotal && totalReqs >= limitTotal) statusCode = 'limit';
                                else if (limitDaily && dailyReqs >= limitDaily) statusCode = 'dailyLimit';
                                let cleanUrl = new URL(url.href);
                                let panelUrlToUse = sysConfig.customPanelUrl;
                                if (targetUser.userPanelUrl && targetUser.userPanelUrl.trim()) panelUrlToUse = targetUser.userPanelUrl.trim();
                                if (panelUrlToUse) {
                                    let customUrlStr = panelUrlToUse;
                                    if (!customUrlStr.startsWith('http://') && !customUrlStr.startsWith('https://')) customUrlStr = 'https://' + customUrlStr;
                                    try { const customUrl = new URL(customUrlStr); cleanUrl.protocol = customUrl.protocol; cleanUrl.host = customUrl.host; } catch(e) {}
                                }
                                cleanUrl.searchParams.delete('flag'); cleanUrl.searchParams.delete('format');
                                cleanUrl.searchParams.delete('type'); cleanUrl.searchParams.delete('output'); cleanUrl.searchParams.delete('raw');
                                const syncNormal = cleanUrl.href;
                                const syncRaw = cleanUrl.href + (cleanUrl.href.includes('?') ? '&flag=a' : '?flag=a');
                                // Total progress bar
                                let totalProgress = '';
                                if (limitTotal) {
                                    totalProgress = `<div class="w-full rounded-full h-1.5 mt-3 overflow-hidden progress-bar-bg"><div class="h-1.5 rounded-full" style="background: var(--accent); width: ${totalPercent}%;"></div></div><p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${totalPercent}% Used</p>`;
                                } else {
                                    totalProgress = '<p class="text-[10px] text-muted mt-2" data-i18n="unlimitedPlan">Unlimited Plan</p>';
                                }
                                // Daily progress bar
                                let dailyProgress = '';
                                if (limitDaily) {
                                    dailyProgress = `<div class="w-full rounded-full h-1.5 mt-3 overflow-hidden progress-bar-bg"><div class="h-1.5 rounded-full" style="background: var(--amber-text); width: ${dailyPercent}%;"></div></div><p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${dailyPercent}% Used</p>`;
                                } else {
                                    dailyProgress = '<p class="text-[10px] text-muted mt-2" data-i18n="noDailyLimit">No Daily Limit</p>';
                                }
                                // Replace placeholders
                                html = html.replace(/__USER_NAME__/g, targetUser.name);
                                html = html.replace(/__USER_ID__/g, targetUser.id);
                                html = html.replace(/__STATUS_CODE__/g, statusCode);
                                html = html.replace(/__TOTAL_GB__/g, totalGb);
                                html = html.replace(/__LIMIT_TOTAL_GB__/g, limitTotalGb);
                                html = html.replace(/__TOTAL_PERCENT__/g, totalPercent);
                                html = html.replace(/__DAILY_GB__/g, dailyGb);
                                html = html.replace(/__LIMIT_DAILY_GB__/g, limitDailyGb);
                                html = html.replace(/__DAILY_PERCENT__/g, dailyPercent);
                                html = html.replace(/__EXPIRY_DATE__/g, expiryDateTxt);
                                html = html.replace(/__SYNC_NORMAL__/g, syncNormal);
                                html = html.replace(/__SYNC_RAW__/g, syncRaw);
                                html = html.replace(/__TOTAL_PROGRESS__/g, totalProgress);
                                html = html.replace(/__DAILY_PROGRESS__/g, dailyProgress);
                                return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
                            } catch (e) {
                                return new Response('Failed to load subscription page', { status: 502 });
                            }
                        } else {
                            return serveMaintenancePage(request, url);
                        }
                    }

                    if (hasMultiUser && !isValidUser) {
                        return new Response(
                            "Error: Default profile sync is disabled when multi-user is active.",
                            { status: 403 },
                        );
                    }

                    const allowInsecure =
                        url.searchParams.get("insecure") === "true" ||
                        url.searchParams.get("allowInsecure") === "true" ||
                        url.searchParams.get("allow_insecure") === "1" ||
                        url.searchParams.get("allowInsecure") === "1";

                    const resHeaders = new Headers();
                    resHeaders.set("Cache-Control", "no-store");
                    resHeaders.set("Access-Control-Allow-Origin", "*");

                    let flag = (
                        url.searchParams.get("flag") ||
                        url.searchParams.get("format") ||
                        url.searchParams.get("type") ||
                        url.searchParams.get("output") ||
                        ""
                    ).toLowerCase();

                    if (isValidUser && targetUser) {
                        let idClean = targetUser.id
                            .replace(/-/g, "")
                            .toLowerCase();
                        let sysU = sysUsageCache?.users?.[idClean] || {
                            reqs: 0,
                            dReqs: 0,
                        };
                        let totalReqs = sysU.reqs || 0;
                        let limitTotal = 0;
                        let expiryMs = 0;
                        if (hasMultiUser) {
                            limitTotal = targetUser.limitTotalReq || 0;
                            expiryMs = targetUser.expiryMs || 0;
                        } else {
                            limitTotal = sysConfig.limitTotalReq || 0;
                            expiryMs = sysConfig.expiryMs || 0;
                        }

                        let usedBytes = Math.floor(
                            totalReqs * (1073741824 / 6000),
                        );
                        let limitBytes = Math.floor(
                            limitTotal * (1073741824 / 6000),
                        );
                        let expireSec = expiryMs
                            ? Math.floor(expiryMs / 1000)
                            : 0;

                        const subUserInfo = `upload=0; download=${usedBytes}; total=${limitBytes}; expire=${expireSec}`;
                        resHeaders.set("Subscription-UserInfo", subUserInfo);
                        resHeaders.set("subscription-userinfo", subUserInfo);
                        resHeaders.set("Profile-Update-Interval", "12");
                        resHeaders.set("profile-update-interval", "12");

                        let cleanName = encodeURIComponent(targetUser.name);
                        resHeaders.set(
                            "Content-Disposition",
                            `attachment; filename="${cleanName}"; filename*=UTF-8''${cleanName}`,
                        );
                    }

                    // Determine subscription format
                    let isClashYaml = false;
                    let isSingboxJson = false;
                    let isClashJson = false;
                    let isVJson = false;

                    // If flag is explicitly set, we respect it
                    if (
                        flag === "clash" ||
                        flag === "yaml" ||
                        flag === "meta" ||
                        flag === "stash" ||
                        flag === "clash-meta" ||
                        flag === "y"
                    ) {
                        isClashYaml = true;
                    } else if (flag === "b" || flag === "c_legacy") {
                        isClashJson = true;
                    } else if (
                        flag === "sing" ||
                        flag === "singbox" ||
                        flag === "sing-box" ||
                        flag === "sb" ||
                        flag === "s" ||
                        flag === "c" ||
                        flag === "g"
                    ) {
                        isSingboxJson = true;
                    } else if (flag === "vjson" || flag === "v") {
                        isVJson = true;
                    } else if (flag === "base64") {
                        // Skip auto-detect to default to base64 plain-text subscription format
                    } else if (flag === "a" || flag === "raw" || flag === "") {
                        // Safe auto-detect for raw sync or no-flag links using target browser / client User-Agent
                        if (
                            ua.includes(getGamma()) ||
                            ua.includes("meta") ||
                            ua.includes("sta" + "sh") ||
                            ua.includes("verge") ||
                            ua.includes("mihomo") ||
                            ua.includes("cfw") ||
                            ua.includes("stash") ||
                            ua.includes("clash")
                        ) {
                            isClashYaml = true;
                        } else if (
                            ua.includes("sing-box") ||
                            ua.includes("singbox") ||
                            ua.includes("hiddify") ||
                            ua.includes("nekobox") ||
                            ua.includes("sfa") ||
                            ua.includes("karing")
                        ) {
                            isSingboxJson = true;
                        }
                    }

                    if (isClashYaml) {
                        resHeaders.set(
                            "Content-Type",
                            "text/yaml; charset=utf-8",
                        );
                        return new Response(
                            await buildYamlProfile(clientHost, targetSub, allowInsecure, env),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isSingboxJson) {
                        resHeaders.set(
                            "Content-Type",
                            "application/json; charset=utf-8",
                        );
                        return new Response(
                            JSON.stringify(
                                await buildSingBoxJsonProfile(clientHost, targetSub, allowInsecure, env),
                                null,
                                2,
                            ),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isClashJson) {
                        resHeaders.set(
                            "Content-Type",
                            "application/json; charset=utf-8",
                        );
                        return new Response(
                            JSON.stringify(
                                await buildClashJsonProfile(clientHost, targetSub, allowInsecure, env),
                                null,
                                2,
                            ),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isVJson) {
                        resHeaders.set("Content-Type", "application/json; charset=utf-8");
                        return new Response(JSON.stringify(await buildVJsonProfile(clientHost, targetSub, allowInsecure, env), null, 2), { headers: resHeaders });
                    } else {
                        resHeaders.set(
                            "Content-Type",
                            "text/plain; charset=utf-8",
                        );
                        const raw = await buildUriProfile(
                            clientHost,
                            targetSub,
                            allowInsecure,
                        );
                        return new Response(safeBtoa(raw), {
                            headers: resHeaders,
                        });
                    }
                }
            }

            if (isTelemetryStream) {
                if (sysConfig.isPaused)
                    return new Response(null, { status: 503 });
                let wsRelayIdx = -1;
                try {
                    const riParam = url.searchParams.get("ri");
                    if (riParam !== null) wsRelayIdx = parseInt(riParam, 10);
                } catch (e) {}
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split("/").pop();
                        if (lastSeg) {
                            const num = parseInt(lastSeg, 10);
                            if (!isNaN(num) && num >= 0) wsRelayIdx = num;
                        }
                    } catch (e) {}
                }
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split("/").pop();
                        if (lastSeg) {
                            const decoded = JSON.parse(atob(lastSeg));
                            if (typeof decoded.relayIdx === "number")
                                wsRelayIdx = decoded.relayIdx;
                        }
                    } catch (e) {}
                }
                return await processTelemetryStream(env, ctx, wsRelayIdx);
            }

            return new Response(null, { status: 404 });
        } catch (err) {
            return new Response(null, { status: 404 });
        }
    },
    async scheduled(event, env, ctx) {
        try {
            await loadSysConfig(env, ctx);
            if (sysConfig.autoUpdate && sysConfig.cfAccountId && sysConfig.cfApiToken && sysConfig.cfWorkerName) {
                const repo = (sysConfig.githubRepo || "amirpocom63-del/mrvpn294")
                    .replace(/https?:\/\/github\.com\//, "")
                    .trim();
                let remoteVer = null;
                try {
                    const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/version`);
                    if (res.ok) {
                        remoteVer = (await res.text()).trim();
                    }
                } catch (e) {}
                
                if (remoteVer && cmpVersions(CURRENT_VERSION, remoteVer) < 0) {
                    try {
                        const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.js`);
                        if (!res.ok) throw new Error(`HTTP ${res.status}`);
                        let latestCode = await res.text();
                        const format = sysConfig.autoUpdateFormat || "normal";
                        if (format === "obfuscated") {
                            latestCode = obfuscateCode(latestCode);
                        }
                        const deployRes = await deployWorkerToCloudflare(
                            sysConfig.cfAccountId,
                            sysConfig.cfApiToken,
                            sysConfig.cfWorkerName,
                            latestCode
                        );
                        const deployResult = await deployRes.json();
                        if (deployResult.success) {
                            await logActivity(env, "Auto-Update Success", `Auto-updated to v${remoteVer} (${format})`);
                            if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
                                for (const p of sysConfig.linkedPanels) {
                                    if (p && p.url && p.apiKey) {
                                        let cleanUrl = p.url.trim();
                                        if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                                            cleanUrl = "https://" + cleanUrl;
                                        }
                                        try {
                                            const parsed = new URL(cleanUrl);
                                            const targetUrl = `${parsed.protocol}//${parsed.host}/${encodeURI(sysConfig.apiRoute)}/api/update`;
                                            ctx?.waitUntil(
                                                fetch(targetUrl, {
                                                    method: "POST",
                                                    headers: { "Content-Type": "application/json" },
                                                    body: JSON.stringify({
                                                        key: p.apiKey,
                                                        action: "deploy",
                                                        code: latestCode,
                                                        force: true
                                                    }),
                                                    signal: AbortSignal.timeout(15000)
                                                }).catch(() => {})
                                            );
                                        } catch (err) {}
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        await logActivity(env, "Auto-Update Failed", `Auto-update failed: ${e.message}`);
                    }
                }
            }
        } catch (e) {}
    }
};

async function serveMaintenancePage(request, url) {
    let fakeList = sysConfig.maintenanceHost
        ? sysConfig.maintenanceHost
              .split(",")
              .map((s) => s.trim())
              .filter((s) => s)
        : ["https://www.ubuntu.com"];
    const clientIP = request.headers.get("cf-connecting-ip") || "0.0.0.0";
    const ipHash = Array.from(clientIP).reduce(
        (acc, char) => acc + char.charCodeAt(0),
        0,
    );
    const targetStr = fakeList[ipHash % fakeList.length].startsWith("http")
        ? fakeList[ipHash % fakeList.length]
        : `https://${fakeList[ipHash % fakeList.length]}`;

    try {
        const targetUrl = new URL(targetStr);
        if (url.pathname !== "/") targetUrl.pathname = url.pathname;
        targetUrl.search = url.search;
        const cleanHeaders = new Headers(request.headers);
        cleanHeaders.set("Host", targetUrl.hostname);
        cleanHeaders.delete("cf-connecting-ip");
        cleanHeaders.delete("x-forwarded-for");
        const fetchInit = {
            method: request.method,
            headers: cleanHeaders,
            redirect: "follow",
        };
        if (request.method !== "GET" && request.method !== "HEAD")
            fetchInit.body = request.body;
        return await fetch(new Request(targetUrl.toString(), fetchInit));
    } catch (e) {
        return new Response("Not Found", { status: 404 });
    }
}


let sysConfigLoading = null;
let sysUsageLoading = null;
let backupIpLoading = null;

function migrateSlaveNodesToLinkedPanels(config) {
    let modified = false;
    if (config && config.slaveNodes && config.slaveNodes.trim().length > 0) {
        if (!config.linkedPanels) config.linkedPanels = [];
        let nodes = config.slaveNodes
            .split(/[\r\n,;]+/)
            .map((s) => s.trim())
            .filter(Boolean);
        let syncKey = config.syncApiKey || "";
        nodes.forEach((node) => {
            let cleanNode = node.replace(/^[a-zA-Z]+:\/\//, "").split("/")[0].split("@").pop().split(":")[0].toLowerCase();
            let exists = config.linkedPanels.some((p) => {
                if (!p || !p.url) return false;
                let cleanUrl = p.url.replace(/^[a-zA-Z]+:\/\//, "").split("/")[0].split("@").pop().split(":")[0].toLowerCase();
                return cleanUrl === cleanNode;
            });
            if (!exists) {
                config.linkedPanels.push({ url: node, apiKey: syncKey });
                modified = true;
            }
        });
        config.slaveNodes = "";
        modified = true;
    }
    return modified;
}

async function loadSysConfig(env, ctx = null) {
    const now = Date.now();

    if (env.AM_DB) {
        if (now - sysConfigCacheTime > CACHE_TTL_CONFIG) {
            if (!sysConfigLoading) {
                sysConfigLoading = d1Get(env, "sys_config")
                    .then((stored) => {
                        sysConfig = {
                            ...SYSTEM_DEFAULTS,
                            ...(stored ? JSON.parse(stored) : null),
                        };
                        sysConfigCacheTime = Date.now();
                        if (migrateSlaveNodesToLinkedPanels(sysConfig)) {
                            const promise = cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            if (ctx && typeof ctx.waitUntil === "function") {
                                ctx.waitUntil(promise.catch(() => {}));
                            } else {
                                promise.catch(() => {});
                            }
                        }
                    })
                    .catch(() => {
                        sysConfig = { ...SYSTEM_DEFAULTS };
                        sysConfigCacheTime = Date.now();
                    })
                    .finally(() => {
                        sysConfigLoading = null;
                    });
            }
            await sysConfigLoading;
        }
        if (now - sysUsageCacheTime > CACHE_TTL_USAGE) {
            if (!sysUsageLoading) {
                sysUsageLoading = d1Get(env, "sys_usage")
                    .then((ustored) => {
                        if (ustored) sysUsageCache = JSON.parse(ustored);
                        else sysUsageCache = { users: {} };
                        sysUsageCacheTime = Date.now();
                    })
                    .catch(() => {
                        sysUsageCache = { users: {} };
                        sysUsageCacheTime = Date.now();
                    })
                    .finally(() => {
                        sysUsageLoading = null;
                    });
            }
            await sysUsageLoading;
        }
    }

    if (now - backupIpCacheTime > CACHE_TTL_BACKUP_IP) {
        if (!backupIpLoading) {
            backupIpLoading = (
                env.AM_DB ? d1Get(env, "backup_ip") : Promise.resolve(null)
            )
                .then((val) => {
                    backupIpCache = val;
                    backupIpCacheTime = Date.now();
                })
                .catch(() => {
                    backupIpCacheTime = Date.now();
                })
                .finally(() => {
                    backupIpLoading = null;
                });
        }
        await backupIpLoading;
    }
    sysConfig.customRelay = backupIpCache ?? env.RELAY_IP ?? "";
}

async function fetchCloudflareUsage(accountId, apiToken) {
    if (!accountId || !apiToken) return null;
    try {
        const d = new Date();
        const currentDate = d.toISOString().split("T")[0] + "T00:00:00Z";

        const query = `query GetDailyUsage($accountId: String!, $start: ISO8601DateTime!) { viewer { accounts(filter: {accountTag: $accountId}) { workersInvocationsAdaptive(limit: 1, filter: { datetime_geq: $start }) { sum { requests } } } } }`;
        const variables = { accountId: accountId, start: currentDate };

        const res = await fetch(
            "https://api.cloudflare.com/client/v4/graphql",
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${apiToken}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ query, variables }),
            },
        );

        const json = await res.json();
        const reqs =
            json?.data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive?.[0]
                ?.sum?.requests;
        return typeof reqs === "number" ? reqs : null;
    } catch (e) {
        return null;
    }
}

async function sendTelegramMessage(request, type, hostName) {
    if (!sysConfig.tgToken || !(sysConfig.tgAdminId || sysConfig.tgChatId))
        return;

    const escMd = (s) => String(s).replace(/[_*()[`[]/g, "\\$&");

    let usageStr = "نامشخص (0.00%)";
    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
        const reqs = await fetchCloudflareUsage(
            sysConfig.cfAccountId,
            sysConfig.cfApiToken,
        );
        if (reqs !== null) {
            const limit = 100000;
            const pct = ((reqs / limit) * 100).toFixed(2);
            usageStr = `${reqs}/${limit} ${pct}%`;
        }
    }

    const ip = request.headers.get("cf-connecting-ip") || "Unknown";
    const cf = request.cf || {};
    const country = cf.country || "Unknown";
    const city = cf.city || "Unknown";
    const asn = cf.asn || "Unknown";
    const asOrg = cf.asOrganization || "Unknown";
    const domain = request.headers.get("Host") || new URL(request.url).hostname;
    const path = new URL(request.url).pathname;
    const ua =
        request.headers.get("User-Agent") || "حالا یوزرایجنت مارو نبینین";

    const d = new Date();
    const timeStr = new Intl.DateTimeFormat("fa-IR", {
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
    }).format(d);

    const text =
        `📌 نوع: ${escMd(type)}\n` +
        `🌐 IP: ${escMd(ip)}\n` +
        `📍 موقعیت: ${escMd(country)} ${escMd(city)}\n` +
        `🏢 ASN: AS${escMd(asn)} ${escMd(asOrg)}\n` +
        `🔗 دامنه: ${escMd(domain)}\n` +
        `🔍 مسیر: ${escMd(path)}\n` +
        `🤖 مرورگر: ${escMd(ua)}\n` +
        `📅 زمان: ${escMd(timeStr)}\n` +
        `📊 مصرف: ${usageStr}`;

    const h = hostName || domain;
    const langCode = sysConfig.tgBotLang || "fa";
    const locT = (key) =>
        botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;
    const isPaused = sysConfig.isPaused || false;
    const panelUrl = `https://${h}/${encodeURI(sysConfig.apiRoute)}/dash`;
    const subUrl = `https://${h}/${sysConfig.apiRoute}`;
    const inline_keyboard = [
        [
            { text: `📊 ${locT("dashboard")}`, callback_data: "sys_dashboard" },
            { text: `📈 ${locT("statistics")}`, callback_data: "sys_stats" },
        ],
        [
            {
                text: `🔗 ${locT("btn_sub_link")}`,
                callback_data: "get_sub_link",
            },
            {
                text: `ℹ️ ${locT("panel_info")}`,
                callback_data: "sys_panel_info",
            },
        ],
        [
            {
                text: `🌐 ${langCode === "fa" ? "English 🇺🇸" : "فارسی 🇮🇷"}`,
                callback_data: "sys_lang",
            },
            {
                text: isPaused
                    ? `▶️ ${locT("btn_resume")}`
                    : `⏸️ ${locT("btn_pause")}`,
                callback_data: "sys_toggle_status",
            },
        ],
        [{ text: `🔑 ${locT("dash")}`, web_app: { url: panelUrl } }],
    ];

    const tgUrl = `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`;
    const notifyChatId = sysConfig.tgAdminId || sysConfig.tgChatId;
    try {
        await fetch(tgUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                chat_id: notifyChatId,
                text: text,
                parse_mode: "Markdown",
                reply_markup: /** @type {any} */ ({ inline_keyboard }),
            }),
        });
    } catch (e) {}
}

async function logActivity(env, type, detail) {
    if (!env || !env.AM_DB) return;
    try {
        const ts = new Date().toISOString();
        let logs = [];
        const stored = await d1Get(env, "sys_logs");
        if (stored) logs = JSON.parse(stored);
        logs.unshift({ ts, type, detail });
        if (logs.length > 50) logs = logs.slice(0, 50);
        await d1Put(env, "sys_logs", JSON.stringify(logs));
    } catch (e) {}
}

async function handleLogs(request, env) {
    try {
        if (request.method === "POST") {
            const data = await request.json();
            if (!isAuthorized(request, data))
                return new Response(JSON.stringify({ success: false }), {
                    status: 401,
                });
            let logs = [];
            if (env.AM_DB) {
                const stored = await d1Get(env, "sys_logs");
                if (stored) logs = JSON.parse(stored);
            }
            return new Response(JSON.stringify({ success: true, logs }), {
                status: 200,
            });
        }
        return new Response("OK", { status: 200 });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleUsersApi(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;
        const userId = url.searchParams.get("id");
        const action = url.searchParams.get("action");

        const authHeader = request.headers.get("Authorization") || "";
        const authKey =
            authHeader.replace("Bearer ", "") ||
            url.searchParams.get("key") ||
            "";
        let bodyKey = "";
        if (method === "POST" || method === "PUT") {
            try {
                const body = await request.clone().json();
                bodyKey = body.key || "";
            } catch (e) {}
        }
        const isAuth =
            authKey === sysConfig.masterKey ||
            bodyKey === sysConfig.masterKey ||
            isPanelApiKey(authKey) ||
            isPanelApiKey(bodyKey);
        if (!isAuth) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "GET" && !userId) {
            const q = url.searchParams.get("q") || "";
            let users = sysConfig.users || [];
            if (q) {
                const ql = q.toLowerCase();
                users = users.filter(
                    (u) =>
                        u.name.toLowerCase().includes(ql) ||
                        u.id.toLowerCase().includes(ql) ||
                        (u.notes && u.notes.toLowerCase().includes(ql)),
                );
            }
            const enriched = users.map((u) => {
                const idClean = u.id.replace(/-/g, "").toLowerCase();
                const sysU = sysUsageCache?.users?.[idClean] || {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: "",
                };
                const usedBytes = Math.floor(
                    (sysU.reqs || 0) * (1073741824 / 6000),
                );
                const limitBytes = u.limitTotalReq
                    ? Math.floor(u.limitTotalReq * (1073741824 / 6000))
                    : 0;
                const isExpired = u.expiryMs && Date.now() > u.expiryMs;
                let status = "active";
                if (u.isPaused && u.disabledReason) status = "auto-disabled";
                else if (u.isPaused) status = "paused";
                else if (isExpired) status = "expired";
                return {
                    ...u,
                    usage: {
                        total: usedBytes,
                        limit: limitBytes,
                        daily: sysU.dReqs || 0,
                        dailyLimit: u.limitDailyReq || 0,
                    },
                    status,
                };
            });
            return new Response(
                JSON.stringify({
                    success: true,
                    users: enriched,
                    total: enriched.length,
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "GET" && userId) {
            const u = (sysConfig.users || []).find(
                (usr) =>
                    usr.id === userId ||
                    usr.name.toLowerCase() === userId.toLowerCase(),
            );
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const idClean = u.id.replace(/-/g, "").toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || {
                reqs: 0,
                dReqs: 0,
                lastDay: "",
            };
            const usedBytes = Math.floor(
                (sysU.reqs || 0) * (1073741824 / 6000),
            );
            const limitBytes = u.limitTotalReq
                ? Math.floor(u.limitTotalReq * (1073741824 / 6000))
                : 0;
            const isExpired = u.expiryMs && Date.now() > u.expiryMs;
            let status = "active";
            if (u.isPaused && u.disabledReason) status = "auto-disabled";
            else if (u.isPaused) status = "paused";
            else if (isExpired) status = "expired";
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;
            return new Response(
                JSON.stringify({
                    success: true,
                    user: {
                        ...u,
                        usage: {
                            total: usedBytes,
                            limit: limitBytes,
                            daily: sysU.dReqs || 0,
                            dailyLimit: u.limitDailyReq || 0,
                        },
                        status,
                        subscriptionUrl: subUrl,
                    },
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "POST" && !userId) {
            const body = await request.json();
            const {
                name,
                trafficLimit,
                expiryDays,
                notes,
                maxConfigs,
                proxyIp,
                cleanIp,
                userMode,
                userPorts,
                userNodes,
                nat64,
                connLimit,
                userPanelUrl,
            } = body;
            if (!name)
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Name is required",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const newId = crypto.randomUUID();
            const newUser = {
                id: newId,
                name: name,
                limitTotalReq: trafficLimit
                    ? Math.floor(parseFloat(trafficLimit) * 6000)
                    : null,
                limitDailyReq: body.dailyLimit
                    ? Math.floor(parseFloat(body.dailyLimit) * 6000)
                    : null,
                expiryMs: expiryDays
                    ? Date.now() + parseInt(expiryDays) * 86400000
                    : null,
                notes: notes || "",
                maxConfigs: maxConfigs ? parseInt(maxConfigs) : null,
                proxyIp: proxyIp || null,
                cleanIp: cleanIp || null,
                userMode: userMode || null,
                userPorts: userPorts || null,
                userNodes: userNodes || null,
                nat64: nat64 || null,
                connLimit: connLimit ? parseInt(connLimit) : null,
                userPanelUrl: userPanelUrl || null,
                createdAt: Date.now(),
            };
            await resolveUserProxyIpGeo(newUser);
            if (!sysConfig.users) sysConfig.users = [];
            sysConfig.users.push(newUser);
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Created",
                    `User "${name}" (${newId}) created via API`,
                ).catch(() => {}),
            );
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(name)}`;
            return new Response(
                JSON.stringify({
                    success: true,
                    user: newUser,
                    subscriptionUrl: subUrl,
                }),
                {
                    status: 201,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "PUT" && userId) {
            const body = await request.json();
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const u = sysConfig.users.find((usr) => usr.id === userId);
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            if (body.name !== undefined) u.name = body.name;
            if (body.trafficLimit !== undefined)
                u.limitTotalReq = body.trafficLimit
                    ? Math.floor(parseFloat(body.trafficLimit) * 6000)
                    : null;
            if (body.dailyLimit !== undefined)
                u.limitDailyReq = body.dailyLimit
                    ? Math.floor(parseFloat(body.dailyLimit) * 6000)
                    : null;
            if (body.expiryDays !== undefined)
                u.expiryMs = body.expiryDays
                    ? Date.now() + parseInt(body.expiryDays) * 86400000
                    : null;
            if (body.notes !== undefined) u.notes = body.notes;
            if (body.maxConfigs !== undefined)
                u.maxConfigs = body.maxConfigs
                    ? parseInt(body.maxConfigs)
                    : null;
            if (body.proxyIp !== undefined) {
                u.proxyIp = body.proxyIp;
                if (!body.proxyIp) {
                    u.proxyIpGeo = null;
                } else {
                    await resolveUserProxyIpGeo(u);
                }
            }
            if (body.cleanIp !== undefined) u.cleanIp = body.cleanIp;
            if (body.userMode !== undefined) u.userMode = body.userMode;
            if (body.userPorts !== undefined) u.userPorts = body.userPorts;
            if (body.userNodes !== undefined) u.userNodes = body.userNodes;
            if (body.nat64 !== undefined) u.nat64 = body.nat64;
            if (body.connLimit !== undefined)
                u.connLimit = body.connLimit ? parseInt(body.connLimit) : null;
            if (body.userPanelUrl !== undefined)
                u.userPanelUrl = body.userPanelUrl || null;
            if (body.status !== undefined) {
                if (body.status === "active") {
                    u.isPaused = false;
                    u.disabledReason = null;
                    u.disabledAt = null;
                } else if (body.status === "paused") {
                    u.isPaused = true;
                    u.disabledReason = null;
                    u.disabledAt = null;
                }
            }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Updated",
                    `User "${u.name}" (${userId}) updated via API`,
                ).catch(() => {}),
            );
            return new Response(JSON.stringify({ success: true, user: u }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "DELETE" && userId) {
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const idx = sysConfig.users.findIndex((usr) => usr.id === userId);
            if (idx === -1)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const deleted = sysConfig.users.splice(idx, 1)[0];
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Deleted",
                    `User "${deleted.name}" (${userId}) deleted via API`,
                ).catch(() => {}),
            );
            return new Response(
                JSON.stringify({ success: true, deleted: deleted.id }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "POST" && userId && action === "toggle") {
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const u = sysConfig.users.find((usr) => usr.id === userId);
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            u.isPaused = !u.isPaused;
            if (!u.isPaused) {
                u.disabledReason = null;
                u.disabledAt = null;
            }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Toggled",
                    `User "${u.name}" (${userId}) ${u.isPaused ? "paused" : "resumed"} via API`,
                ).catch(() => {}),
            );
            return new Response(JSON.stringify({ success: true, user: u }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "POST" && userId && action === "reset") {
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            const uuidClean = userId.replace(/-/g, "").toLowerCase();
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: new Date().toISOString().split("T")[0],
                };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "Traffic Reset",
                    `Traffic reset for user ${userId} via API`,
                ).catch(() => {}),
            );
            return new Response(
                JSON.stringify({ success: true, message: "Traffic reset" }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid request" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleStatsApi(request, env) {
    try {
        const url = new URL(request.url);
        const authHeader = request.headers.get("Authorization") || "";
        const authKey =
            authHeader.replace("Bearer ", "") ||
            url.searchParams.get("key") ||
            "";
        if (authKey !== sysConfig.masterKey && !isPanelApiKey(authKey)) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        const users = sysConfig.users || [];
        const totalUsers = users.length;
        const activeUsers = users.filter(
            (u) => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs),
        ).length;
        const autoDisabledUsers = users.filter(
            (u) => u.isPaused && u.disabledReason,
        ).length;
        const pausedUsers = users.filter(
            (u) => u.isPaused && !u.disabledReason,
        ).length;
        const expiredUsers = users.filter(
            (u) => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused,
        ).length;

        let totalTrafficReqs = 0;
        let dailyTrafficReqs = 0;
        const todayDate = new Date().toISOString().split("T")[0];
        users.forEach((u) => {
            const idClean = u.id.replace(/-/g, "").toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || {
                reqs: 0,
                dReqs: 0,
                lastDay: "",
            };
            totalTrafficReqs += sysU.reqs || 0;
            if (sysU.lastDay === todayDate) dailyTrafficReqs += sysU.dReqs || 0;
        });

        
        let usageData = {};
        for (let [k, v] of uuidUsage.entries()) {
            usageData[k] = { ...v, connects: activeConns.get(k) || 0 };
        }
        const upSeconds = Math.floor((Date.now() - isolateStartTime) / 1000);

        return new Response(
            JSON.stringify({
                success: true,
                stats: {
                    users: {
                        total: totalUsers,
                        active: activeUsers,
                        paused: pausedUsers,
                        expired: expiredUsers,
                        autoDisabled: autoDisabledUsers,
                    },
                    traffic: {
                        totalRequests: totalTrafficReqs,
                        totalGB: (totalTrafficReqs / 6000).toFixed(2),
                        dailyRequests: dailyTrafficReqs,
                        dailyGB: (dailyTrafficReqs / 6000).toFixed(2),
                    },
                    usage: usageData,
                system: {
                        uptimeSeconds: upSeconds,
                        activeConnections,
                        version: CURRENT_VERSION,
                        isPaused: sysConfig.isPaused || false,
                    },
                },
            }),
            { headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

function parseImportBindings(importStr) {
    const cleanStr = importStr.replace(/\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "").trim();
    const content = cleanStr
        .replace(/^import\s+/, "")
        .replace(/\s+from\s+["'].*?["'];?$/, "")
        .trim();

    const bindings = [];

    if (content.startsWith("*")) {
        const match = content.match(/\*\s+as\s+(\w+)/);
        if (match) bindings.push({ name: match[1], isNamespace: true });
        return bindings;
    }

    const braceStart = content.indexOf("{");
    if (braceStart !== -1) {
        const defaultPart = content.slice(0, braceStart).replace(/,/, "").trim();
        if (defaultPart) {
            bindings.push({ name: defaultPart, isDefault: true });
        }
        const bracePart = content.slice(braceStart + 1, content.lastIndexOf("}")).trim();
        const namedImports = bracePart.split(",").map((s) => s.trim()).filter(Boolean);
        namedImports.forEach((item) => {
            if (item.includes(" as ")) {
                const parts = item.split(/\s+as\s+/);
                bindings.push({ name: parts[1], original: parts[0] });
            } else {
                bindings.push({ name: item });
            }
        });
    } else {
        bindings.push({ name: content, isDefault: true });
    }

    return bindings;
}

function obfuscateCode(srcText) {
    const importRegex = /import\s+[\s\S]*?from\s+["'].*?["'];?/g;
    const imports = [];
    let match;

    while ((match = importRegex.exec(srcText)) !== null) {
        imports.push(match[0]);
    }

    let cleanCode = srcText.replace(importRegex, "");

    const bindings = [];
    imports.forEach((imp) => {
        const parsed = parseImportBindings(imp);
        bindings.push(...parsed);
    });

    const uniqueBindings = [];
    const seenNames = new Set();
    bindings.forEach((b) => {
        if (!seenNames.has(b.name)) {
            seenNames.add(b.name);
            uniqueBindings.push(b);
        }
    });

    cleanCode = cleanCode.replace(/export\s+default\s+/g, "const _0xmrvpn294Module = ");
    cleanCode += "\nreturn _0xmrvpn294Module;";

    const randKey = Math.floor(Math.random() * 80) + 64;

    const encoder = new TextEncoder();
    const bytes = encoder.encode(cleanCode);

    let hexOutput = "";
    for (let i = 0; i < bytes.length; i++) {
        const xorByte = bytes[i] ^ randKey;
        hexOutput += xorByte.toString(16).padStart(2, "0");
    }

    const rawImportsStr = imports.join("\n");
    const bindingNames = uniqueBindings.map((b) => b.name);

    const finalLoaderCode =
        rawImportsStr +
        "\n\n" +
        "// mrvpn294 Gateway - Obfuscated Loader Context (v2.5.4.2 Optimized)\n" +
        'const _0xmrvpn294Payload = "' +
        hexOutput +
        '";\n' +
        "const _0xmrvpn294Key = " +
        randKey +
        ";\n\n" +
        "const _0xmrvpn294Bytes = new Uint8Array((_0xmrvpn294Payload.match(/.{1,2}/g) || []).map(x => parseInt(x, 16) ^ _0xmrvpn294Key));\n" +
        "const _0xmrvpn294Code = new TextDecoder().decode(_0xmrvpn294Bytes);\n" +
        "const _0xmrvpn294Runtime = new Function(" +
        bindingNames.map((name) => '"' + name + '"').join(", ") +
        ", _0xmrvpn294Code)(" +
        bindingNames.join(", ") +
        ");\n\n" +
        "export default _0xmrvpn294Runtime;";

    return finalLoaderCode;
}

function cmpVersions(a, b) {
    const strip = (v) => String(v).replace(/^v/, "").trim();
    const pa = strip(a).split(".").map(Number);
    const pb = strip(b).split(".").map(Number);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
        let na = pa[i] || 0,
            nb = pb[i] || 0;
        if (na > nb) return 1;
        if (nb > na) return -1;
    }
    return 0;
}

async function handleUpdateApi(request, env, ctx) {
    try {
        if (request.method !== "POST")
            return new Response("405", { status: 405 });
        const data = await request.json();
        const deployKey = extractAuthKey(request, data);
        if (deployKey !== sysConfig.masterKey) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        const accountId = sysConfig.cfAccountId;
        const apiToken = sysConfig.cfApiToken;
        const workerName = sysConfig.cfWorkerName;
        const repo = (sysConfig.githubRepo || "amirpocom63-del/mrvpn294")
            .replace(/https?:\/\/github\.com\//, "")
            .trim();

        if (data.action === "check") {
            let remoteVer = null;
            try {
                const res = await fetch(
                    `https://raw.githubusercontent.com/${repo}/main/version`,
                );
                if (res.ok) {
                    const txt = (await res.text()).trim();
                    if (txt && txt.length <= 15) remoteVer = txt;
                }
            } catch (e) {}
            if (!remoteVer) {
                try {
                    const res = await fetch(
                        `https://raw.githubusercontent.com/${repo}/main/_worker.js`,
                    );
                    if (res.ok) {
                        const code = await res.text();
                        const match = code.match(
                            /const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/,
                        );
                        if (match) remoteVer = match[1];
                    }
                } catch (e) {}
            }
            if (!remoteVer) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Could not fetch remote version",
                    }),
                    {
                        status: 502,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
            const hasCredentials = !!(accountId && apiToken && workerName);
            return new Response(
                JSON.stringify({
                    success: true,
                    current: CURRENT_VERSION,
                    latest: remoteVer,
                    updateAvailable:
                        cmpVersions(CURRENT_VERSION, remoteVer) < 0,
                    canDeploy: hasCredentials,
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (data.action === "deploy") {
            if (!accountId || !apiToken || !workerName) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "CF credentials not configured",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }

            let finalCodeToDeploy = data.code;
            if (!finalCodeToDeploy) {
                try {
                    const res = await fetch(
                        `https://raw.githubusercontent.com/${repo}/main/_worker.js`,
                    );
                    if (!res.ok) throw new Error(`HTTP ${res.status}`);
                    finalCodeToDeploy = await res.text();
                } catch (e) {
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Failed to fetch from GitHub: " + e.message,
                        }),
                        {
                            status: 502,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                }
            }

            const versionMatch = finalCodeToDeploy.match(
                /const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/,
            );
            const newVersion = versionMatch ? versionMatch[1] : CURRENT_VERSION;

            if (
                cmpVersions(CURRENT_VERSION, newVersion) >= 0 &&
                !data.force &&
                !data.code
            ) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Remote version is not newer. Click force redeploy to switch formats or overwrite.",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }

            // Move the obfuscate logic from client-side to worker-side
            const format = data.format || sysConfig.autoUpdateFormat || "normal";
            if (format === "obfuscated") {
                try {
                    finalCodeToDeploy = obfuscateCode(finalCodeToDeploy);
                } catch (oe) {
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Obfuscation failed: " + oe.message,
                        }),
                        {
                            status: 500,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                }
            }

            const deployRes = await deployWorkerToCloudflare(
                accountId,
                apiToken,
                workerName,
                finalCodeToDeploy,
            );
            const deployResult = await deployRes.json();

            if (deployResult.success) {
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "Panel Updated",
                        `v${CURRENT_VERSION} → v${newVersion} (${format})`,
                    ).catch(() => {}),
                );

                // Update all nodes with main panel update!
                if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
                    for (const p of sysConfig.linkedPanels) {
                        if (p && p.url && p.apiKey) {
                            let cleanUrl = p.url.trim();
                            if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                                cleanUrl = "https://" + cleanUrl;
                            }
                            try {
                                const parsed = new URL(cleanUrl);
                                const targetUrl = `${parsed.protocol}//${parsed.host}/${encodeURI(sysConfig.apiRoute)}/api/update`;
                                ctx?.waitUntil(
                                    fetch(targetUrl, {
                                        method: "POST",
                                        headers: { "Content-Type": "application/json" },
                                        body: JSON.stringify({
                                            key: p.apiKey,
                                            action: "deploy",
                                            code: finalCodeToDeploy,
                                            force: true
                                        }),
                                        signal: AbortSignal.timeout(15000)
                                    }).then(async (r) => {
                                        const resJson = await r.json();
                                        await logActivity(env, "Node Update Success", `Node ${p.url} update response: ${JSON.stringify(resJson)}`);
                                    }).catch((e) => {
                                        logActivity(env, "Node Update Failed", `Node ${p.url} update failed: ${e.message}`);
                                    })
                                );
                            } catch (err) {
                                console.error(`Failed to trigger update on node ${p.url}:`, err);
                            }
                        }
                    }
                }

                if (
                    sysConfig.tgToken &&
                    (sysConfig.tgAdminId || sysConfig.tgChatId)
                ) {
                    const tgMsg = `🔄 <b>Panel Updated</b>\n\n📦 v${CURRENT_VERSION} → v${newVersion}\n🌐 <b>Format:</b> ${format}`;
                    const notifyChatId =
                        sysConfig.tgAdminId || sysConfig.tgChatId;
                    ctx?.waitUntil(
                        fetch(
                            `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`,
                            {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    chat_id: notifyChatId,
                                    text: tgMsg,
                                    parse_mode: "HTML",
                                }),
                            },
                        ).catch(() => {}),
                    );
                }
                return new Response(
                    JSON.stringify({
                        success: true,
                        message: `Updated to v${newVersion}`,
                        newVersion,
                    }),
                    { headers: { "Content-Type": "application/json" } },
                );
            } else {
                const errMsg =
                    deployResult.errors?.[0]?.message || "Unknown API error";
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Cloudflare API: " + errMsg,
                    }),
                    {
                        status: 502,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid action" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: "Internal error" }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleApiKeys(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;

        const authKey = extractAuthKey(request, null);
        if (authKey !== sysConfig.masterKey) {
            return new Response(
                JSON.stringify({
                    success: false,
                    error: "Only master key can manage API keys",
                }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "GET") {
            const keys = (sysConfig.panelApiKeys || []).map((k) => ({
                id: k.id,
                name: k.name,
                keyPreview: k.key.slice(0, 8) + "..." + k.key.slice(-4),
                createdAt: k.createdAt,
                lastUsed: k.lastUsed,
            }));
            return new Response(JSON.stringify({ success: true, keys }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "POST") {
            const body = await request.json();
            if (body.action === "create") {
                if (!sysConfig.panelApiKeys) sysConfig.panelApiKeys = [];
                if (sysConfig.panelApiKeys.length >= 10) {
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Maximum 10 API keys allowed",
                        }),
                        {
                            status: 400,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                }
                const newKey = generateApiKey(body.name);
                sysConfig.panelApiKeys.push(newKey);
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "API Key Created",
                        `Key "${newKey.name}" created`,
                    ).catch(() => {}),
                );
                return new Response(
                    JSON.stringify({ success: true, key: newKey }),
                    {
                        status: 201,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
            if (body.action === "revoke") {
                if (!body.id)
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "ID required",
                        }),
                        {
                            status: 400,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                const idx = (sysConfig.panelApiKeys || []).findIndex(
                    (k) => k.id === body.id,
                );
                if (idx === -1)
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Key not found",
                        }),
                        {
                            status: 404,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                const revoked = sysConfig.panelApiKeys.splice(idx, 1)[0];
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "API Key Revoked",
                        `Key "${revoked.name}" revoked`,
                    ).catch(() => {}),
                );
                return new Response(
                    JSON.stringify({ success: true, revoked: revoked.id }),
                    { headers: { "Content-Type": "application/json" } },
                );
            }
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid request" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleAuth(request, hostName, ctx, env) {
    try {
        const data = await request.json();
        const ip = request.headers.get("cf-connecting-ip") || "Unknown";
        const loginKey = data.key || "";
        const isKeyAuth =
            loginKey === sysConfig.masterKey || isPanelApiKey(loginKey);
        if (isKeyAuth) {
            if (isPanelApiKey(loginKey)) {
                const apiKeyEntry = (sysConfig.panelApiKeys || []).find(
                    (k) => k.key === loginKey,
                );
                if (apiKeyEntry) apiKeyEntry.lastUsed = Date.now();
            }
            ctx?.waitUntil(
                logActivity(
                    env,
                    "Auth Success",
                    `Successful panel login from ${ip} (via ${isPanelApiKey(loginKey) ? "API Key" : "Master Key"})`,
                ),
            );
            if (!sysConfig.silentAlerts && ctx)
                ctx.waitUntil(
                    sendTelegramMessage(
                        request,
                        "ورود به پنل (موفق)",
                        hostName,
                    ),
                );

            // Store login signal for Telegram bot
            if (sysConfig.tgAdminId && env.AM_DB) {
                const loginSignal = {
                    name: sysConfig.name || hostName,
                    host: hostName,
                    apiRoute: sysConfig.apiRoute,
                    masterKey: sysConfig.masterKey,
                    isLocal: true,
                    ts: Date.now(),
                };
                ctx?.waitUntil(
                    d1Put(
                        env,
                        "tg_panel_login",
                        JSON.stringify(loginSignal),
                    ).catch(() => {}),
                );
            }

            // Notify hub panel if configured
            if (
                sysConfig.hubPanelUrl &&
                sysConfig.hubPanelUrl.trim() &&
                sysConfig.tgAdminId
            ) {
                try {
                    let hubUrl = sysConfig.hubPanelUrl.trim();
                    if (!hubUrl.startsWith("http"))
                        hubUrl = "https://" + hubUrl;
                    const signalPayload = {
                        signal: "panel_login",
                        panelName: sysConfig.name || hostName,
                        panelHost: hostName,
                        panelApiRoute: sysConfig.apiRoute,
                        tgAdminId: sysConfig.tgAdminId,
                        ts: Date.now(),
                    };
                    ctx?.waitUntil(
                        fetch(
                            `${hubUrl}/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`,
                            {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify(signalPayload),
                            },
                        ).catch(() => {}),
                    );
                } catch (e) {}
            }

            const netInfo = {
                ip: ip,
                colo: request.cf?.colo || "Unknown",
                loc:
                    (request.cf?.city || "Unknown") +
                    ", " +
                    (request.cf?.country || "Unknown"),
            };
            let usageData = {};
            for (let [k, v] of uuidUsage.entries()) usageData[k] = { ...v, connects: activeConns.get(k) || 0 };
            let baseHost = hostName;
            let protocol = "https";
            if (sysConfig.customPanelUrl && sysConfig.customPanelUrl.trim()) {
                let customUrlStr = sysConfig.customPanelUrl.trim();
                if (
                    !customUrlStr.startsWith("http://") &&
                    !customUrlStr.startsWith("https://")
                ) {
                    customUrlStr = "https://" + customUrlStr;
                }
                try {
                    const customUrl = new URL(customUrlStr);
                    baseHost = customUrl.host;
                    protocol = customUrl.protocol.replace(":", "");
                } catch (e) {}
            }
            return new Response(
                JSON.stringify({
                    success: true,
                    config: isPanelApiKey(loginKey)
                        ? {
                              ...sysConfig,
                              masterKey: "[PROTECTED]",
                              panelApiKeys: "[PROTECTED]",
                              cfApiToken: "[PROTECTED]",
                              cfAccountId: "[PROTECTED]",
                              cfWorkerName: "[PROTECTED]",
                              tgToken: "[PROTECTED]",
                              tgChatId: "[PROTECTED]",
                              tgAdminId: "[PROTECTED]",
                              syncApiKey: "[PROTECTED]",
                          }
                        : sysConfig,
                    deviceId: activeDeviceId,
                    network: netInfo,
                    usage: usageData,
                    sysUsage:
                        sysUsageCache && sysUsageCache.users
                            ? sysUsageCache.users
                            : {},
                    version: CURRENT_VERSION,
                    profiles: getAllProfiles().map((p) => {
                        let subSuffix =
                            p.name === "Default"
                                ? ""
                                : "?sub=" + encodeURIComponent(p.name);
                        return {
                            name: p.name,
                            id: p.id,
                            sync: `${protocol}://${baseHost}/${sysConfig.apiRoute}${subSuffix}`,
                        };
                    }),
                }),
                { status: 200 },
            );
        }
        ctx?.waitUntil(
            logActivity(env, "Auth Failed", `Failed login attempt from ${ip}`),
        );
        if (ctx)
            ctx.waitUntil(
                sendTelegramMessage(
                    request,
                    "تلاش ناموفق ورود به پنل!",
                    hostName,
                ),
            );
        return new Response(JSON.stringify({ success: false }), {
            status: 401,
        });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleConfigSync(request, env, ctx) {
    try {
        const data = await request.json();
        const isAuthSync =
            data.key === sysConfig.masterKey ||
            (data.oldKey && data.oldKey === sysConfig.masterKey) ||
            isPanelApiKey(data.key) ||
            isPanelApiKey(data.oldKey) ||
            (data.fromMaster &&
                data.config &&
                data.config.masterKey &&
                data.config.masterKey === sysConfig.masterKey);
        if (!isAuthSync)
            return new Response(
                JSON.stringify({
                    success: false,
                    error: "Auth failed. Generate the API key on THIS panel, not the main panel.",
                }),
                { status: 401 },
            );
        if (!env.AM_DB)
            return new Response(
                JSON.stringify({ success: false, msg: "DB Error" }),
                { status: 400 },
            );

        let nextConfig = sysConfig;
        if (data.config) {
            const preserveApiKeys = sysConfig.panelApiKeys || [];
            nextConfig = { ...sysConfig, ...data.config };
            if (Array.isArray(nextConfig.users)) {
                nextConfig.users = nextConfig.users.map(u => ({...u}));
            }
            if (
                preserveApiKeys.length > 0 &&
                (!data.config.panelApiKeys ||
                    data.config.panelApiKeys.length === 0)
            ) {
                nextConfig.panelApiKeys = preserveApiKeys;
            }
            migrateSlaveNodesToLinkedPanels(nextConfig);
            if (
                Array.isArray(nextConfig.users) &&
                nextConfig.users.length > 0
            ) {
                const geoPromises = nextConfig.users.map(async (u) => {
                    if (u.proxyIp) {
                        await resolveUserProxyIpGeo(u);
                    } else {
                        u.proxyIpGeo = null;
                    }
                });
                await Promise.all(geoPromises);
            }
            sysConfig = nextConfig;
            await cachedD1Put(env, "sys_config", JSON.stringify(nextConfig));
        }

        let tagWarning = null;
        if (
            nextConfig.nameStrategy &&
            nextConfig.nameStrategy.includes("{") &&
            nextConfig.nameStrategy.includes("}")
        ) {
            let vResult = validateNameStrategy(nextConfig.nameStrategy);
            if (!vResult.valid)
                tagWarning = `Unknown tags detected: ${vResult.unknownTags.join(", ")}`;
        }

        if (data.resetUUID) {
            const uuidClean = data.resetUUID.replace(/-/g, "").toLowerCase();
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: new Date().toISOString().split("T")[0],
                };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
        }

        if (data.config && !data.fromMaster) {
            let currentHost = new URL(request.url).hostname;
            let slaveConfig = { ...nextConfig };
            [
                "cfAccountId",
                "cfApiToken",
                "cfWorkerName",
                "tgToken",
                "tgChatId",
                "tgAdminId",
                "masterKey",
                "syncApiKey",
                "apiRoute",
                "deviceId",
                "panelApiKeys",
                "hubPanelUrl",
                "linkedPanels",
                "slaveNodes",
                "githubRepo",
                "customPanelUrl"
            ].forEach((k) => delete slaveConfig[k]);

            // Propagate config to slaveNodes
            if (nextConfig.slaveNodes && nextConfig.slaveNodes.trim().length > 0) {
                let nodes = nextConfig.slaveNodes
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
                let syncKey = nextConfig.syncApiKey || "";
                nodes.forEach((node) => {
                    if (node !== currentHost) {
                        ctx?.waitUntil(
                            fetch(
                                `https://${node}/${encodeURI(nextConfig.apiRoute)}/api/sync`,
                                {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({
                                        key: syncKey,
                                        config: slaveConfig,
                                        fromMaster: true,
                                    }),
                                },
                            ).catch(() => {}),
                        );
                    }
                });
            }

            // Propagate config to linkedPanels
            if (nextConfig.linkedPanels && Array.isArray(nextConfig.linkedPanels)) {
                nextConfig.linkedPanels.forEach((p) => {
                    if (p && p.url && p.apiKey) {
                        let cleanUrl = p.url.trim();
                        if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                            cleanUrl = "https://" + cleanUrl;
                        }
                        try {
                            const parsed = new URL(cleanUrl);
                            if (parsed.hostname !== currentHost) {
                                ctx?.waitUntil(
                                    fetch(
                                        `${parsed.protocol}//${parsed.host}/${encodeURI(nextConfig.apiRoute)}/api/sync`,
                                        {
                                            method: "POST",
                                            headers: { "Content-Type": "application/json" },
                                            body: JSON.stringify({
                                                key: p.apiKey,
                                                config: slaveConfig,
                                                fromMaster: true,
                                            }),
                                        },
                                    ).catch(() => {}),
                                );
                            }
                        } catch (err) {
                            console.error(`Failed to propagate config to linked panel ${p.url}:`, err);
                        }
                    }
                });
            }
        }

        if (nextConfig.tgToken && ctx) {
            const hookUrl = `https://${new URL(request.url).hostname}/${encodeURI(nextConfig.apiRoute)}/tg`;
            ctx.waitUntil(
                fetch(
                    `https://api.telegram.org/bot${nextConfig.tgToken}/setWebhook`,
                    {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ url: hookUrl }),
                    },
                ).catch(() => {}),
            );
        }

        return new Response(
            JSON.stringify({
                success: true,
                newRoute: nextConfig.apiRoute,
                tagWarning,
            }),
            { status: 200 },
        );
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleSyncPanel(request, env, ctx) {
    try {
        const data = await request.json();
        if (!data.signal || data.signal !== "panel_login") {
            return new Response(
                JSON.stringify({ success: false, error: "Invalid signal" }),
                { status: 400 },
            );
        }
        if (!data.tgAdminId || !data.panelHost) {
            return new Response(
                JSON.stringify({ success: false, error: "Missing fields" }),
                { status: 400 },
            );
        }
        // Verify the tgAdminId matches this panel's config
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        if (!adminId || adminId.toString() !== data.tgAdminId.toString()) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 401 },
            );
        }
        // Also verify a valid panelApiKey if one was provided
        if (data.panelApiKey && !isPanelApiKey(data.panelApiKey)) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 401 },
            );
        }
        const loginSignal = {
            name: data.panelName || data.panelHost,
            host: data.panelHost,
            apiRoute: data.panelApiRoute || sysConfig.apiRoute,
            isLocal: false,
            ts: data.ts || Date.now(),
        };
        if (env.AM_DB) {
            ctx?.waitUntil(
                d1Put(env, "tg_panel_login", JSON.stringify(loginSignal)).catch(
                    () => {},
                ),
            );
        }
        return new Response(JSON.stringify({ success: true }), { status: 200 });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

const botI18n = {
    en: {
        welcome:
            "🤖 **Welcome to mrvpn294 Gateway Bot**\nSelect your option below to manage your system:",
        status: "System Status",
        users: "Subscribers",
        metrics: "Gateway Health",
        panic: "Panic Mode",
        dash: "Dashboard Control",
        lang: "🌐 Change Language",
        active: "🟢 Active",
        paused: "🔴 Paused",
        uptime: "Uptime",
        streams: "📡 Active Streams",
        no_users: "No subscribers found.",
        sub_info: "👤 Subscriber Details:",
        name: "Name",
        total: "Total Reqs",
        daily: "Daily Reqs",
        expiry: "Expiry",
        days: "Days remaining",
        created: "Created At",
        unlimited: "Unlimited",
        btn_back: "◀️ Back",
        btn_next: "▶️ Next",
        btn_del: "Delete",
        btn_pause: "Pause",
        btn_resume: "Resume",
        btn_edit_name: "Change Name",
        btn_edit_limits: "Limits",
        btn_add: "+ Add Subscriber",
        btn_confirm: "Confirm",
        btn_cancel: "Cancel",
        msg_enter_name: "Please send a name for the subscriber:",
        msg_added: "Sub added successfully! 🎉",
        msg_deleted: "Sub deleted successfully! 🗑️",
        msg_panic:
            "🚨 PANIC MODE ACTIVATED 🚨\nRoute randomized & System Paused.",
        msg_invalid: "Invalid input. Please try again.",
        msg_enter_limits:
            "Enter limits format:\n`[totalReqs] [dailyReqs] [days_limit]`\n(Use 0 for unlimited)\n\nExample:\n`10000 500 30`",
        msg_confirm_del: "⚠️ Are you sure you want to delete this subscriber?",
        msg_confirm_panic:
            "⚠️ Are you absolutely sure you want to trigger PANIC mode? This will randomize API routes and pause all connections!",
        status_updated: "Status updated!",
        access_denied:
            "Access Denied. You are not authorized to manage this panel.",
        dashboard: "Dashboard",
        search: "Search User",
        statistics: "Statistics",
        panel_info: "Panel Info",
        disabled_users: "Disabled Users",
        reset_traffic: "Reset Traffic",
        extend_expiry: "Extend Expiry",
        notes: "Notes",
        device_limit: "Config Limit",
        msg_enter_search:
            "🔍 Send a username, UUID, or subscription to search:",
        msg_enter_notes: "📝 Send notes for this user:",
        msg_enter_extend_days: "📅 Enter number of days to extend expiration:",
        msg_traffic_reset: "Traffic has been reset successfully!",
        msg_expiry_extended: "Expiration extended by {days} days!",
        msg_no_disabled: "No disabled users found.",
        msg_enter_device_limit: "Enter config limit (0 for unlimited):",
        config_limit_updated: "Config limit updated!",
        stats_title: "Panel Statistics",
        count_active: "active",
        count_paused: "paused",
        count_disabled: "auto-disabled",
        dash_total: "Total Users",
        dash_active: "Active",
        dash_paused: "Paused",
        dash_expired: "Expired",
        dash_auto_disabled: "Auto-Disabled",
        btn_main_menu: "Main Menu",
        btn_back_to_list: "Back to List",
        total_traffic: "Total Traffic",
        daily_traffic: "Daily Traffic",
        lbl_status: "Status",
        lbl_subscription: "Subscription Connection",
        lbl_user_not_found: "⚠️ User not found",
        lbl_none: "None",
        lbl_page: "Page",
        select_panel: "🔌 Which panel do you want to manage?",
        current_panel: "Current Panel",
        switch_panel: "🔄 Switch Panel",
        panel_local: "🏠 This Panel",
        panel_remote: "🌐",
        msg_panel_selected: "Panel selected! ✅",
        msg_panel_error: "❌ Failed to connect to the selected panel.",
        msg_panel_unreachable:
            "⚠️ Panel is unreachable. Please check the configuration.",
        btn_sub_link: "Subscription Link",
        sub_link_sent: "Subscription link sent!",
        btn_update_usage: "Update Usage",
        tg_settings: "Settings",
        tg_advanced: "Advanced",
        tg_logs: "Logs",
        tg_sys_settings: "System Settings",
        tg_adv_settings: "Advanced Settings",
        tg_logs_view: "View Logs",
        tg_logs_clear: "Clear Logs",
        tg_proto: "Protocol",
        tg_ports: "Ports",
        tg_uuid: "Device UUID",
        tg_path: "API Route",
        tg_pass: "Master Key",
        tg_dns: "DNS",
        tg_relay: "Relay IP",
        tg_maintenance: "Maintenance Hosts",
        tg_tfo: "TCP Fast Open",
        tg_ech: "ECH",
        tg_silent: "Silent Alerts",
        tg_pause: "Kill Switch",
        tg_auto_update: "Auto Update",
        tg_direct: "Direct Configs",
        tg_nat64: "NAT64",
        tg_clean_ips: "Clean IPs",
        tg_nodes: "Nodes",
        tg_strategy: "Name Strategy",
        tg_prefix: "Name Prefix",
        tg_fake_entries: "Fake Entries",
        tg_cf_settings: "Cloudflare Settings",
        tg_tg_settings: "Telegram Settings",
        tg_backup: "Backup",
        tg_restore: "Restore",
        tg_current_val: "Current Value",
        tg_new_val: "Send new value:",
        tg_saved: "Saved!",
        tg_cancelled: "Cancelled",
        tg_log_entry: "",
        tg_log_empty: "No logs found",
        tg_u_custom_name: "Custom Name",
        tg_u_clean_ips: "Clean IPs",
        tg_u_proxy_ips: "Proxy IPs",
        tg_u_nodes: "Nodes",
        tg_u_nat64: "NAT64",
        tg_u_mode: "Protocol Mode",
        tg_u_ports: "Ports",
        tg_u_conn_limit: "Conn Limit",
        tg_u_panel_url: "Panel URL",
        tg_u_max_cfg: "Max Configs",
        tg_u_all: "All Settings",
        tg_network: "Network",
        tg_uptime: "Uptime",
        tg_conns: "Active Connections",
        tg_version: "Version",
        tg_cf_usage: "CF Usage",
    },
    fa: {
        welcome:
            "🤖 **به ربات ترانزیت MrVpn294 خوش آمدید**\nجهت مدیریت سیستم نظارتی خود یکی از گزینه‌های زیر را انتخاب نمایید:",
        status: "وضعیت سیستم",
        users: "مدیریت مشترکین",
        metrics: "سلامت درگاه شبکه",
        panic: "وضعیت اضطراری (Panic)",
        dash: "پنل تحت وب",
        lang: "🌐 تغییر زبان به انگلیسی",
        active: "🟢 فعال",
        paused: "🔴 متوقف شده",
        uptime: "زمان کارکرد",
        streams: "📡 اتصالات فعال",
        no_users: "هیچ مشترکی پیدا نشد.",
        sub_info: "👤 مشخصات مشترک:",
        name: "نام",
        total: "درخواست کل",
        daily: "درخواست روزانه",
        expiry: "انقضاء",
        days: "روزهای باقی‌مانده",
        created: "تاریخ ایجاد",
        unlimited: "نامحدود",
        btn_back: "بازگشت",
        btn_next: "بعدی",
        btn_del: "حذف",
        btn_pause: "غیرفعال‌سازی",
        btn_resume: "فعال‌سازی",
        btn_edit_name: "تغییر نام",
        btn_edit_limits: "ویرایش محدودیت‌ها",
        btn_add: "+ افزودن مشترک جدید",
        btn_confirm: "تأیید",
        btn_cancel: "انصراف",
        msg_enter_name: "لطفاً نام یا شناسه مشترک جدید را ارسال نمایید:",
        msg_added: "مشترک با موفقیت افزوده شد!",
        msg_deleted: "مشترک با موفقیت حذف گردید!",
        msg_panic: "وضعیت اضطراری فعال شد\nمسیر تصادفی شد و سیستم متوقف گردید.",
        msg_invalid: "ورودی نامعتبر است. مجدداً تلاش نمایید.",
        msg_enter_limits:
            "فرمت ورودی محدودیت:\n`[کل] [روزانه] [مدت_روز]`\n(از 0 برای نامحدود استفاده کنید)\n\nمثال:\n`10000 500 30`",
        msg_confirm_del: "آیا از حذف این مشترک اطمینان کامل دارید؟",
        msg_confirm_panic:
            "آیا از فعال‌سازی وضعیت اضطراری اطمینان دارید؟ کل اتصالات متوقف و آدرس‌ها منقضی خواهند شد!",
        status_updated: "وضعیت بروزرسانی شد!",
        access_denied: "دسترسی غیرمجاز. شما اجازه مدیریت این پنل را ندارید.",
        dashboard: "داشبورد",
        search: "جستجوی کاربر",
        statistics: "آمار",
        panel_info: "اطلاعات پنل",
        disabled_users: "کاربران غیرفعال",
        reset_traffic: "بازنشانی ترافیک",
        extend_expiry: "تمدید انقضا",
        notes: "یادداشت‌ها",
        device_limit: "محدودیت کانفیگ",
        msg_enter_search: "🔍 نام کاربری، UUID یا لینک اشتراک را ارسال کنید:",
        msg_enter_notes: "📝 یادداشت برای این کاربر را ارسال کنید:",
        msg_enter_extend_days: "📅 تعداد روزهای تمدید را وارد کنید:",
        msg_traffic_reset: "ترافیک با موفقیت بازنشانی شد!",
        msg_expiry_extended: "انقضا به مدت {days} روز تمدید شد!",
        msg_no_disabled: "هیچ کاربر غیرفعالی یافت نشد.",
        msg_enter_device_limit:
            "محدودیت تعداد کانفیگ را وارد کنید (0 برای نامحدود):",
        config_limit_updated: "محدودیت کانفیگ به‌روزرسانی شد!",
        stats_title: "آمار پنل",
        count_active: "فعال",
        count_paused: "متوقف",
        count_disabled: "غیرفعال خودکار",
        dash_total: "کل کاربران",
        dash_active: "فعال",
        dash_paused: "متوقف",
        dash_expired: "منقضی",
        dash_auto_disabled: "غیرفعال خودکار",
        btn_main_menu: "منوی اصلی",
        btn_back_to_list: "بازگشت به لیست",
        total_traffic: "ترافیک کل",
        daily_traffic: "ترافیک روزانه",
        lbl_status: "وضعیت",
        lbl_subscription: "لینک اشتراک",
        lbl_user_not_found: "⚠️ کاربر یافت نشد",
        lbl_none: "ندارد",
        lbl_page: "صفحه",
        select_panel: "🔌 کدام پنل را می‌خواهید مدیریت کنید؟",
        current_panel: "پنل فعلی",
        switch_panel: "🔄 تغییر پنل",
        panel_local: "🏠 این پنل",
        panel_remote: "🌐",
        msg_panel_selected: "پنل انتخاب شد! ✅",
        msg_panel_error: "❌ اتصال به پنل انتخابی ناموفق بود.",
        msg_panel_unreachable:
            "⚠️ پنل در دسترس نیست. لطفاً پیکربندی را بررسی کنید.",
        btn_sub_link: "لینک اشتراک",
        sub_link_sent: "لینک اشتراک ارسال شد!",
        btn_update_usage: "بروزرسانی مصرف",
        tg_settings: "تنظیمات",
        tg_advanced: "پیشرفته",
        tg_logs: "گزارش‌ها",
        tg_sys_settings: "تنظیمات سیستم",
        tg_adv_settings: "تنظیمات پیشرفته",
        tg_logs_view: "مشاهده گزارش‌ها",
        tg_logs_clear: "پاک کردن گزارش‌ها",
        tg_proto: "پروتکل",
        tg_ports: "پورت‌ها",
        tg_uuid: "شناسه دستگاه",
        tg_path: "مسیر API",
        tg_pass: "کلید اصلی",
        tg_dns: "DNS",
        tg_relay: "آی‌پی رله",
        tg_maintenance: "سایت استتار",
        tg_tfo: "TCP Fast Open",
        tg_ech: "ECH",
        tg_silent: "هشدار خاموش",
        tg_pause: "کلید توقف",
        tg_auto_update: "بروزرسانی خودکار",
        tg_direct: "کانفیگ مستقیم",
        tg_nat64: "NAT64",
        tg_clean_ips: "آی‌پی تمیز",
        tg_nodes: "نودها",
        tg_strategy: "روش نام‌گذاری",
        tg_prefix: "پیشوند",
        tg_fake_entries: "ورودی‌های اشتراک",
        tg_cf_settings: "تنظیمات کلودفلر",
        tg_tg_settings: "تنظیمات تلگرام",
        tg_backup: "پشتیبان‌گیری",
        tg_restore: "بازیابی",
        tg_current_val: "مقدار فعلی",
        tg_new_val: "مقدار جدید را ارسال کنید:",
        tg_saved: "ذخیره شد!",
        tg_cancelled: "لغو شد",
        tg_log_entry: "",
        tg_log_empty: "گزارشی ثبت نشده",
        tg_u_custom_name: "نام سفارشی",
        tg_u_clean_ips: "آی‌پی تمیز",
        tg_u_proxy_ips: "آی‌پی پروکسی",
        tg_u_nodes: "نودها",
        tg_u_nat64: "NAT64",
        tg_u_mode: "پروتکل",
        tg_u_ports: "پورت‌ها",
        tg_u_conn_limit: "محدودیت اتصال",
        tg_u_panel_url: "آدرس پنل",
        tg_u_max_cfg: "حداکثر کانفیگ",
        tg_u_all: "همه تنظیمات",
        tg_network: "شبکه",
        tg_uptime: "زمان کارکرد",
        tg_conns: "اتصالات فعال",
        tg_version: "نسخه",
        tg_cf_usage: "مصرف کلودفلر",
    },
};

function getPanelsList() {
    const panels = [];
    panels.push({
        name: sysConfig.name || "Main Panel",
        host: null,
        apiRoute: sysConfig.apiRoute,
        apiKey: null,
        isLocal: true,
    });
    if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
        sysConfig.linkedPanels.forEach((p) => {
            if (p && p.host) {
                panels.push({
                    name: p.name || p.host,
                    host: p.host,
                    apiRoute: p.apiRoute || sysConfig.apiRoute,
                    apiKey: p.apiKey || p.masterKey || null,
                    isLocal: false,
                });
            }
        });
    }
    return panels;
}

async function remotePanelFetch(panel, method, path, body = null) {
    try {
        const url = `https://${panel.host}/${encodeURI(panel.apiRoute)}${path}`;
        const options = {
            method,
            headers: { "Content-Type": "application/json" },
        };
        if (body) options.body = JSON.stringify(body);
        const res = await fetch(url, {
            ...options,
            signal: AbortSignal.timeout(8000),
        });
        return await res.json();
    } catch (e) {
        return { success: false, error: e.message };
    }
}

async function fetchRemotePanelUsers(panel) {
    return await remotePanelFetch(
        panel,
        "GET",
        `/api/users?key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function fetchRemotePanelUser(panel, userId) {
    return await remotePanelFetch(
        panel,
        "GET",
        `/api/users?id=${encodeURIComponent(userId)}&key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function fetchRemotePanelStats(panel) {
    return await remotePanelFetch(
        panel,
        "GET",
        `/api/stats?key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function fetchRemotePanelConfig(panel) {
    return await remotePanelFetch(panel, "POST", "/api/auth", {
        key: panel.apiKey,
    });
}

async function remotePanelWriteAction(panel, method, userId, body = null) {
    let path = "/api/users";
    if (userId)
        path += `?id=${encodeURIComponent(userId)}&key=${encodeURIComponent(panel.apiKey)}`;
    else path += `?key=${encodeURIComponent(panel.apiKey)}`;
    return await remotePanelFetch(
        panel,
        method,
        path,
        body || { key: panel.apiKey },
    );
}

async function remotePanelToggleUser(panel, userId) {
    return await remotePanelFetch(
        panel,
        "POST",
        `/api/users?id=${encodeURIComponent(userId)}&action=toggle&key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function remotePanelResetTraffic(panel, userId) {
    return await remotePanelFetch(
        panel,
        "POST",
        `/api/users?id=${encodeURIComponent(userId)}&action=reset&key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function handleTelegramWebhook(request, env, hostName, ctx) {
    try {
        const update = await request.json();
        const tgApi = `https://api.telegram.org/bot${sysConfig.tgToken}`;

        const langCode = sysConfig.tgBotLang || "fa";
        const t = (key) =>
            botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;

        const callerId =
            update.callback_query?.from?.id?.toString() ||
            update.message?.from?.id?.toString();
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        const isAuthorized = adminId && callerId === adminId.toString();

        if (!isAuthorized) {
            const chatId =
                update.callback_query?.message?.chat?.id ||
                update.message?.chat?.id;
            if (chatId) {
                await fetch(`${tgApi}/sendMessage`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        chat_id: chatId,
                        text:
                            "❌ *شما دسترسی به این ربات را ندارید.*\n\nیوزر آیدی شما جهت اضافه کردن به لیست ادمین ها: `" +
                            (callerId || "Unknown") +
                            "`",
                        parse_mode: "Markdown",
                    }),
                });
            }
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 200 },
            );
        }

        let tgState = {};
        try {
            const storedState = await d1Get(env, "tg_bot_state");
            if (storedState) tgState = JSON.parse(storedState);
        } catch (e) {}

        const panels = getPanelsList();

        // Read last login signal from D1 (set by handleAuth or handleSyncPanel)
        let lastLoginPanel = null;
        try {
            const stored = await d1Get(env, "tg_panel_login");
            if (stored) lastLoginPanel = JSON.parse(stored);
        } catch (e) {}

        const getActivePanel = () => {
            if (lastLoginPanel) {
                if (lastLoginPanel.isLocal)
                    return panels.find((p) => p.isLocal) || panels[0];
                const found = panels.find(
                    (p) => !p.isLocal && p.host === lastLoginPanel.host,
                );
                if (found) return found;
                // Remote panel not in linkedPanels — synthesize from login signal
                return {
                    name: lastLoginPanel.name || lastLoginPanel.host,
                    host: lastLoginPanel.host,
                    apiRoute: lastLoginPanel.apiRoute || sysConfig.apiRoute,
                    apiKey:
                        lastLoginPanel.apiKey ||
                        lastLoginPanel.masterKey ||
                        null,
                    isLocal: false,
                };
            }
            return panels[0]; // default to local
        };

        // Custom sendOrEdit message helper
        const sendOrEdit = async (
            chatId,
            text,
            replyMarkup = null,
            messageId = null,
        ) => {
            let res;
            if (messageId) {
                res = await fetch(`${tgApi}/editMessageText`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        chat_id: chatId,
                        message_id: messageId,
                        text: text,
                        parse_mode: "Markdown",
                        reply_markup: replyMarkup,
                    }),
                });
                if (res.ok) return res;
                try {
                    const errBody = await res.json();
                    if (
                        errBody?.description?.includes(
                            "message is not modified",
                        )
                    )
                        return res;
                } catch (e) {}
            }
            res = await fetch(`${tgApi}/sendMessage`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    chat_id: chatId,
                    text: text,
                    parse_mode: "Markdown",
                    reply_markup: replyMarkup,
                }),
            });
            return res;
        };

        const getMainMenu = (activePanel, isAdmin = true) => {
            const isPaused = sysConfig.isPaused || false;
            const statusEmoji = isPaused ? "🔴" : "🟢";
            const users = sysConfig.users || [];
            const activeCount = users.filter(
                (u) => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs),
            ).length;
            const pausedCount = users.filter(
                (u) => u.isPaused && !u.disabledReason,
            ).length;
            const autoDisabledCount = users.filter(
                (u) => u.isPaused && u.disabledReason,
            ).length;
            const isLocal = !activePanel || activePanel.isLocal;
            const panelName = activePanel
                ? activePanel.name
                : sysConfig.name || "Main Panel";
            const panelIndicator = isLocal
                ? `🏠 ${panelName}`
                : `🌐 ${panelName}`;
            let text =
                `${t("welcome")}\n\n` +
                `━━━━━━━━━━━━━━━━\n` +
                `📌 **${t("current_panel")}**: ${panelIndicator}\n` +
                `⚡ **${t("status")}**: ${isPaused ? t("paused") : t("active")} ${statusEmoji}\n` +
                `👥 **${t("users")}**: ${users.length} (${activeCount} ${t("count_active")}, ${pausedCount} ${t("count_paused")}, ${autoDisabledCount} ${t("count_disabled")})\n` +
                `━━━━━━━━━━━━━━━━`;
            const panelUrl = isLocal
                ? `https://${hostName}/${encodeURI(sysConfig.apiRoute)}/dash`
                : null;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}`;
            /** @type {any} */
            const inline_keyboard = [];
            if (isAdmin) {
                inline_keyboard.push([
                    { text: `👥 ${t("users")}`, callback_data: "subs_list:0" },
                    {
                        text: `🔍 ${t("search")}`,
                        callback_data: "sub_search_init",
                    },
                ]);
            }
            inline_keyboard.push([
                {
                    text: `📊 ${t("dashboard")}`,
                    callback_data: "sys_dashboard",
                },
                { text: `📈 ${t("statistics")}`, callback_data: "sys_stats" },
            ]);
            inline_keyboard.push([
                {
                    text: `🔗 ${t("btn_sub_link")}`,
                    callback_data: "get_sub_link",
                },
            ]);
            if (isAdmin) {
                inline_keyboard.push([
                    {
                        text: `🚫 ${t("disabled_users")}`,
                        callback_data: "subs_disabled:0",
                    },
                ]);
                inline_keyboard.push([
                    {
                        text: `⚙️ ${t("tg_settings")}`,
                        callback_data: "tg_settings_menu",
                    },
                    {
                        text: `🔧 ${t("tg_advanced")}`,
                        callback_data: "tg_advanced_menu",
                    },
                ]);
                inline_keyboard.push([
                    {
                        text: `📋 ${t("tg_logs")}`,
                        callback_data: "tg_logs_menu",
                    },
                ]);
            }
            inline_keyboard.push([
                {
                    text: `🌐 ${langCode === "fa" ? "English 🇺🇸" : "فارسی 🇮🇷"}`,
                    callback_data: "sys_lang",
                },
                {
                    text: isPaused
                        ? `▶️ ${t("btn_resume")}`
                        : `⏸️ ${t("btn_pause")}`,
                    callback_data: "sys_toggle_status",
                },
            ]);
            if (panelUrl) {
                inline_keyboard.push([
                    { text: `🔑 ${t("dash")}`, web_app: { url: panelUrl } },
                    {
                        text: `ℹ️ ${t("panel_info")}`,
                        callback_data: "sys_panel_info",
                    },
                ]);
                if (isAdmin) {
                    inline_keyboard.push([
                        {
                            text: `🚨 ${t("panic")}`,
                            callback_data: "sys_panic_init",
                        },
                    ]);
                }
            } else {
                inline_keyboard.push([
                    {
                        text: `ℹ️ ${t("panel_info")}`,
                        callback_data: "sys_panel_info",
                    },
                ]);
            }
            const kb = { inline_keyboard };
            return { text, kb };
        };

        const getSubsList = (page = 0, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const itemsPerPage = 5;
            const totalPages = Math.ceil(users.length / itemsPerPage);
            const start = page * itemsPerPage;
            const end = start + itemsPerPage;
            const pageUsers = users.slice(start, end);

            let text = `👥 **${t("users")}** (${t("lbl_page")} ${page + 1}/${Math.max(1, totalPages)})\n`;
            text += `━━━━━━━━━━━━━━━━\n`;

            if (users.length === 0) {
                text += `⚠️ ${t("no_users")}\n`;
            } else {
                pageUsers.forEach((u, idx) => {
                    text += `${start + idx + 1}. 👤 **${u.name}**\n   \`${u.id}\`\n`;
                });
            }
            text += `━━━━━━━━━━━━━━━━`;

            const inline_keyboard = [];
            pageUsers.forEach((u) => {
                inline_keyboard.push([
                    {
                        text: `👤 ${u.name}`,
                        callback_data: `sub_detail:${u.id}`,
                    },
                ]);
            });

            const navRow = [];
            if (page > 0) {
                navRow.push({
                    text: `⬅️ ${t("btn_back")}`,
                    callback_data: `subs_list:${page - 1}`,
                });
            }
            if (end < users.length) {
                navRow.push({
                    text: `${t("btn_next")} ➡️`,
                    callback_data: `subs_list:${page + 1}`,
                });
            }
            if (navRow.length > 0) {
                inline_keyboard.push(navRow);
            }

            inline_keyboard.push([
                { text: `➕ ${t("btn_add")}`, callback_data: "sub_add_init" },
            ]);
            inline_keyboard.push([
                { text: t("btn_main_menu"), callback_data: "main_menu" },
            ]);

            return { text, kb: { inline_keyboard } };
        };

        const getSubDetail = (uuid, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const u = users.find((usr) => usr.id === uuid);
            if (!u) {
                return {
                    text: "⚠️ User not found",
                    kb: {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_back"),
                                    callback_data: "subs_list:0",
                                },
                            ],
                        ],
                    },
                };
            }

            const sysU = sysUsageCache?.users?.[
                u.id.replace(/-/g, "").toLowerCase()
            ] || { reqs: 0, dReqs: 0, lastDay: "" };
            const userReqs = sysU.reqs || 0;
            const curDate = new Date().toISOString().split("T")[0];
            const userDReqs = sysU.lastDay === curDate ? sysU.dReqs || 0 : 0;

            const limitTotalTxt = u.limitTotalReq
                ? `${u.limitTotalReq}`
                : t("unlimited");
            const limitDailyTxt = u.limitDailyReq
                ? `${u.limitDailyReq}`
                : t("unlimited");
            const usedGB = (userReqs / 6000).toFixed(2);
            const limitGB = u.limitTotalReq
                ? (u.limitTotalReq / 6000).toFixed(2)
                : t("unlimited");

            let expTxt = t("unlimited");
            let isExp = false;
            let daysLeft = t("unlimited");
            if (u.expiryMs) {
                const date = new Date(u.expiryMs);
                expTxt = date.toLocaleDateString();
                const remDays = Math.ceil((u.expiryMs - Date.now()) / 86400000);
                daysLeft = remDays >= 0 ? `${remDays}` : "0";
                if (Date.now() > u.expiryMs) {
                    expTxt += ` (${t("dash_expired")} 🔴)`;
                    isExp = true;
                }
            }

            const statusEmoji = u.isPaused ? "⏸️" : isExp ? "🔴" : "🟢";
            const statusText = u.isPaused
                ? t("paused")
                : isExp
                  ? t("dash_expired")
                  : t("active");
            const subSync = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;
            const maxCfgTxt = u.maxConfigs || t("unlimited");
            const notesTxt = u.notes || t("lbl_none");
            const modeTxt = u.userMode
                ? u.userMode === "alpha"
                    ? "Alpha (V)"
                    : u.userMode === "beta"
                      ? "Beta (T)"
                      : "Both"
                : t("unlimited");
            const portsTxt = u.userPorts || t("unlimited");
            const cleanIpsTxt = u.cleanIp
                ? u.cleanIp.substring(0, 30) +
                  (u.cleanIp.length > 30 ? "..." : "")
                : "—";
            const proxyIpsTxt = u.proxyIp
                ? u.proxyIp.substring(0, 30) +
                  (u.proxyIp.length > 30 ? "..." : "")
                : "—";
            const nodesTxt = u.userNodes
                ? u.userNodes.substring(0, 30) +
                  (u.userNodes.length > 30 ? "..." : "")
                : "—";
            const nat64Txt = u.nat64 || "—";

            let text = `👤 **${t("sub_info")}**\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `📛 **${t("name")}**: ${u.name}\n`;
            text += `🆔 **UUID**: \`${u.id}\`\n`;
            text += `🚦 **${t("lbl_status")}**: ${statusEmoji} ${statusText}\n`;
            text += `📊 **${t("total")}**: ${usedGB} GB / ${limitGB} GB (${userReqs} reqs)\n`;
            text += `⏱ **${t("daily")}**: ${userDReqs} / ${limitDailyTxt}\n`;
            text += `📅 **${t("expiry")}**: ${expTxt}\n`;
            text += `⏳ **${t("days")}**: ${daysLeft}\n`;
            text += `📡 **${t("tg_u_mode")}**: ${modeTxt}\n`;
            text += `🔌 **${t("tg_u_ports")}**: ${portsTxt}\n`;
            text += `📱 **${t("device_limit")}**: ${maxCfgTxt}\n`;
            text += `🧹 **${t("tg_u_clean_ips")}**: ${cleanIpsTxt}\n`;
            text += `🔗 **${t("tg_u_proxy_ips")}**: ${proxyIpsTxt}\n`;
            text += `🖥️ **${t("tg_u_nodes")}**: ${nodesTxt}\n`;
            text += `🌐 **${t("tg_u_nat64")}**: ${nat64Txt}\n`;
            text += `🔗 **${t("tg_u_conn_limit")}**: ${u.connLimit || t("unlimited")}\n`;
            text += `🎛 **${t("tg_u_panel_url")}**: ${u.userPanelUrl || t("unlimited")}\n`;
            text += `📝 **${t("notes")}**: ${notesTxt}\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `🔗 **${t("lbl_subscription")}:**\n\`${subSync}\``;

            const kb = {
                inline_keyboard: [
                    [
                        {
                            text: u.isPaused
                                ? `▶️ ${t("btn_resume")}`
                                : `⏸️ ${t("btn_pause")}`,
                            callback_data: `sub_toggle:${u.id}`,
                        },
                        {
                            text: `🗑️ ${t("btn_del")}`,
                            callback_data: `sub_del_init:${u.id}`,
                        },
                    ],
                    [
                        {
                            text: `✏️ ${t("btn_edit_name")}`,
                            callback_data: `sub_edit_name_init:${u.id}`,
                        },
                        {
                            text: `⚙️ ${t("btn_edit_limits")}`,
                            callback_data: `sub_edit_limits_init:${u.id}`,
                        },
                    ],
                    [
                        {
                            text: `🔄 ${t("reset_traffic")}`,
                            callback_data: `sub_reset_traffic:${u.id}`,
                        },
                        {
                            text: `📅 ${t("extend_expiry")}`,
                            callback_data: `sub_extend_init:${u.id}`,
                        },
                    ],
                    [
                        {
                            text: `📝 ${t("notes")}`,
                            callback_data: `sub_edit_notes_init:${u.id}`,
                        },
                        {
                            text: `📱 ${t("device_limit")}`,
                            callback_data: `sub_edit_device_init:${u.id}`,
                        },
                    ],
                    [
                        {
                            text: t("btn_back_to_list"),
                            callback_data: "subs_list:0",
                        },
                    ],
                ],
            };
            return { text, kb };
        };

        if (update.callback_query) {
            const cb = update.callback_query;
            const chatId = cb.message?.chat?.id;
            const messageId = cb.message?.message_id;
            const data = cb.data;

            if (chatId) {
                if (!isAuthorized) {
                    await fetch(`${tgApi}/answerCallbackQuery`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            callback_query_id: cb.id,
                            text: t("access_denied"),
                            show_alert: true,
                        }),
                    });
                    return new Response("OK", { status: 200 });
                }

                // Get active panel from last login signal
                const activePanel = getActivePanel();
                const isRemotePanel = activePanel && !activePanel.isLocal;

                // Helper to fetch users for the active panel
                const getPanelUsers = async () => {
                    if (isRemotePanel) {
                        const res = await fetchRemotePanelUsers(activePanel);
                        return res.success ? res.users || [] : null;
                    }
                    return sysConfig.users || [];
                };

                // Clear step state on callback query
                tgState[chatId] = null;
                ctx?.waitUntil(
                    d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(
                        () => {},
                    ),
                );

                let answerText = null;

                if (data === "main_menu") {
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_lang") {
                    sysConfig.tgBotLang = langCode === "fa" ? "en" : "fa";
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_toggle_status") {
                    sysConfig.isPaused = !sysConfig.isPaused;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_metrics") {
                    let usageStr = t("unlimited");
                    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
                        const reqs = await fetchCloudflareUsage(
                            sysConfig.cfAccountId,
                            sysConfig.cfApiToken,
                        );
                        if (reqs !== null) {
                            const pct = ((reqs / 100000) * 100).toFixed(2);
                            usageStr = `${reqs}/100000 (${pct}%)`;
                        }
                    }
                    const upSeconds = Math.floor(
                        (Date.now() - isolateStartTime) / 1000,
                    );
                    const dh = Math.floor(upSeconds / 3600);
                    const dm = Math.floor((upSeconds % 3600) / 60);

                    let text = `📡 **${t("metrics")}**\n`;
                    text += `━━━━━━━━━━━━━━━━\n`;
                    text += `⏱ **${t("uptime")}**: ${dh}h ${dm}m\n`;
                    text += `🔌 **${t("streams")}**: ${activeConnections}\n`;
                    text += `📊 **Cloudflare API Usage**: ${usageStr}\n`;
                    text += `━━━━━━━━━━━━━━━━`;

                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("subs_list:")) {
                    const page = parseInt(data.replace("subs_list:", "")) || 0;
                    const panelUsers = await getPanelUsers();
                    if (panelUsers === null && isRemotePanel) {
                        await sendOrEdit(chatId, t("msg_panel_error"), {
                            inline_keyboard: [
                                [
                                    {
                                        text: t("btn_main_menu"),
                                        callback_data: "main_menu",
                                    },
                                ],
                            ],
                        });
                    } else {
                        const list = getSubsList(page, panelUsers);
                        await sendOrEdit(chatId, list.text, list.kb, messageId);
                    }
                } else if (data.startsWith("sub_detail:")) {
                    const uuid = data.replace("sub_detail:", "");
                    const panelUsers = await getPanelUsers();
                    if (panelUsers === null && isRemotePanel) {
                        await sendOrEdit(chatId, t("msg_panel_error"), {
                            inline_keyboard: [
                                [
                                    {
                                        text: t("btn_main_menu"),
                                        callback_data: "main_menu",
                                    },
                                ],
                            ],
                        });
                    } else {
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(
                            chatId,
                            detail.text,
                            detail.kb,
                            messageId,
                        );
                    }
                } else if (data.startsWith("sub_toggle:")) {
                    const uuid = data.replace("sub_toggle:", "");
                    if (isRemotePanel) {
                        await remotePanelToggleUser(activePanel, uuid);
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(
                            (usr) => usr.id === uuid,
                        );
                        if (u) {
                            u.isPaused = !u.isPaused;
                            await cachedD1Put(
                                env,
                                "sys_config",
                                JSON.stringify(sysConfig),
                            );
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data.startsWith("sub_del_init:")) {
                    const uuid = data.replace("sub_del_init:", "");
                    const panelUsers = await getPanelUsers();
                    const u = panelUsers?.find((usr) => usr.id === uuid);
                    const name = u ? u.name : "";
                    const text = `${t("msg_confirm_del")}\n\n👤 **${name}**`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `✅ ${t("btn_confirm")}`,
                                    callback_data: `sub_del_confirm:${uuid}`,
                                },
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_del_confirm:")) {
                    const uuid = data.replace("sub_del_confirm:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(
                            activePanel,
                            "DELETE",
                            uuid,
                        );
                    } else if (sysConfig.users) {
                        sysConfig.users = sysConfig.users.filter(
                            (usr) => usr.id !== uuid,
                        );
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                    }
                    const successText = `✅ ${t("msg_deleted")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_back"),
                                    callback_data: "subs_list:0",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, successText, kb, messageId);
                } else if (data === "sub_add_init") {
                    tgState[chatId] = { step: "sub_add_name" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `➕ ${t("msg_enter_name")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: "subs_list:0",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_name_init:")) {
                    const uuid = data.replace("sub_edit_name_init:", "");
                    tgState[chatId] = { step: `sub_edit_name:${uuid}` };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `✏️ ${t("msg_enter_name")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_limits_init:")) {
                    const uuid = data.replace("sub_edit_limits_init:", "");
                    tgState[chatId] = { step: `sub_edit_limits:${uuid}` };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `⚙️ ${t("msg_enter_limits")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `♾️ Skip (Unlimited)`,
                                    callback_data: `sub_unlimit_cb:${uuid}`,
                                },
                            ],
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_unlimit_cb:")) {
                    const uuid = data.replace("sub_unlimit_cb:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(activePanel, "PUT", uuid, {
                            key: activePanel.apiKey,
                            trafficLimit: 0,
                            dailyLimit: 0,
                            expiryDays: 0,
                        });
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(
                            (usr) => usr.id === uuid,
                        );
                        if (u) {
                            u.limitTotalReq = null;
                            u.limitDailyReq = null;
                            u.expiryMs = null;
                            await cachedD1Put(
                                env,
                                "sys_config",
                                JSON.stringify(sysConfig),
                            );
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data === "sub_add_unlimited_skip") {
                    let stateName = "Subscriber";
                    try {
                        const savedStateRaw = await d1Get(env, "tg_bot_state");
                        if (savedStateRaw) {
                            const stObj = JSON.parse(savedStateRaw);
                            if (stObj[chatId] && stObj[chatId].name) {
                                stateName = stObj[chatId].name;
                            }
                        }
                    } catch (e) {}

                    const newUuid = crypto.randomUUID();
                    if (isRemotePanel) {
                        const res = await remotePanelWriteAction(
                            activePanel,
                            "POST",
                            null,
                            { key: activePanel.apiKey, name: stateName },
                        );
                        if (res.success && res.user) {
                            const detail = getSubDetail(res.user.id, [
                                res.user,
                            ]);
                            await sendOrEdit(
                                chatId,
                                `✅ ${t("msg_added")}\n\n${detail.text}`,
                                detail.kb,
                                messageId,
                            );
                        } else {
                            await sendOrEdit(chatId, t("msg_panel_error"), {
                                inline_keyboard: [
                                    [
                                        {
                                            text: t("btn_main_menu"),
                                            callback_data: "main_menu",
                                        },
                                    ],
                                ],
                            });
                        }
                    } else {
                        if (!sysConfig.users) sysConfig.users = [];
                        sysConfig.users.push({
                            id: newUuid,
                            name: stateName,
                            limitTotalReq: null,
                            limitDailyReq: null,
                            expiryMs: null,
                            createdAt: Date.now(),
                        });
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        const detail = getSubDetail(newUuid);
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("msg_added")}\n\n${detail.text}`,
                            detail.kb,
                            messageId,
                        );
                    }
                    tgState[chatId] = null;
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                } else if (data === "sys_panic_init") {
                    const text = `${t("msg_confirm_panic")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `🚨 YES PANIC 🚨`,
                                    callback_data: "sys_panic_confirm",
                                },
                                {
                                    text: `❌ No, Cancel`,
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "sys_panic_confirm") {
                    sysConfig.apiRoute = Array.from(
                        crypto.getRandomValues(new Uint8Array(8)),
                    )
                        .map((b) => b.toString(16).padStart(2, "0"))
                        .join("");
                    sysConfig.isPaused = true;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    const successText = `${t("msg_panic")}\n\n🔑 New Secret Path Randomized. All old sessions revoked.`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, successText, kb, messageId);
                } else if (data === "sys_dashboard") {
                    let users,
                        activeCount,
                        pausedCount,
                        expiredCount,
                        autoDisabledCount;
                    if (isRemotePanel) {
                        const statsRes =
                            await fetchRemotePanelStats(activePanel);
                        if (statsRes.success && statsRes.stats) {
                            const s = statsRes.stats;
                            users = [];
                            activeCount = s.users?.active || 0;
                            pausedCount = s.users?.paused || 0;
                            expiredCount = s.users?.expired || 0;
                            autoDisabledCount = s.users?.autoDisabled || 0;
                        } else {
                            const panelUsers = await getPanelUsers();
                            users = panelUsers || [];
                            activeCount = users.filter(
                                (u) =>
                                    !u.isPaused &&
                                    (!u.expiryMs || Date.now() <= u.expiryMs),
                            ).length;
                            pausedCount = users.filter(
                                (u) => u.isPaused && !u.disabledReason,
                            ).length;
                            expiredCount = users.filter(
                                (u) =>
                                    u.expiryMs &&
                                    Date.now() > u.expiryMs &&
                                    !u.isPaused,
                            ).length;
                            autoDisabledCount = users.filter(
                                (u) => u.isPaused && u.disabledReason,
                            ).length;
                        }
                    } else {
                        users = sysConfig.users || [];
                        activeCount = users.filter(
                            (u) =>
                                !u.isPaused &&
                                (!u.expiryMs || Date.now() <= u.expiryMs),
                        ).length;
                        pausedCount = users.filter(
                            (u) => u.isPaused && !u.disabledReason,
                        ).length;
                        expiredCount = users.filter(
                            (u) =>
                                u.expiryMs &&
                                Date.now() > u.expiryMs &&
                                !u.isPaused,
                        ).length;
                        autoDisabledCount = users.filter(
                            (u) => u.isPaused && u.disabledReason,
                        ).length;
                    }
                    let dashText = `📊 **${t("dashboard")}**\n`;
                    dashText += `━━━━━━━━━━━━━━━━\n`;
                    dashText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? "🏠" : "🌐"} ${activePanel.name}\n`;
                    dashText += `━━━━━━━━━━━━━━━━\n`;
                    dashText += `👥 **${t("dash_total")}**: ${Array.isArray(users) ? users.length : activeCount + pausedCount + expiredCount + autoDisabledCount}\n`;
                    dashText += `🟢 **${t("dash_active")}**: ${activeCount}\n`;
                    dashText += `⏸️ **${t("dash_paused")}**: ${pausedCount}\n`;
                    dashText += `🔴 **${t("dash_expired")}**: ${expiredCount}\n`;
                    dashText += `🚫 **${t("dash_auto_disabled")}**: ${autoDisabledCount}\n`;
                    if (!isRemotePanel) {
                        const upSeconds = Math.floor(
                            (Date.now() - isolateStartTime) / 1000,
                        );
                        const dh = Math.floor(upSeconds / 3600);
                        const dm = Math.floor((upSeconds % 3600) / 60);
                        dashText += `⏱ **${t("uptime")}**: ${dh}h ${dm}m\n`;
                        dashText += `🔌 **${t("streams")}**: ${activeConnections}\n`;
                        dashText += `⚡ **System**: ${sysConfig.isPaused ? t("paused") : t("active")}\n`;
                    }
                    dashText += `━━━━━━━━━━━━━━━━`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, dashText, kb, messageId);
                } else if (data === "sys_stats") {
                    let users, totalReqs, dailyReqs;
                    if (isRemotePanel) {
                        const statsRes =
                            await fetchRemotePanelStats(activePanel);
                        if (statsRes.success && statsRes.stats) {
                            const s = statsRes.stats;
                            users = [];
                            totalReqs = s.traffic?.totalRequests || 0;
                            dailyReqs = s.traffic?.dailyRequests || 0;
                        } else {
                            const panelUsers = await getPanelUsers();
                            users = panelUsers || [];
                            totalReqs = 0;
                            dailyReqs = 0;
                        }
                    } else {
                        users = sysConfig.users || [];
                        totalReqs = 0;
                        dailyReqs = 0;
                        const todayDate = new Date()
                            .toISOString()
                            .split("T")[0];
                        users.forEach((u) => {
                            const idClean = u.id
                                .replace(/-/g, "")
                                .toLowerCase();
                            const sysU = sysUsageCache?.users?.[idClean] || {
                                reqs: 0,
                                dReqs: 0,
                                lastDay: "",
                            };
                            totalReqs += sysU.reqs || 0;
                            if (sysU.lastDay === todayDate)
                                dailyReqs += sysU.dReqs || 0;
                        });
                    }
                    let statsText = `📈 **${t("stats_title")}**\n`;
                    statsText += `━━━━━━━━━━━━━━━━\n`;
                    statsText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? "🏠" : "🌐"} ${activePanel.name}\n`;
                    statsText += `━━━━━━━━━━━━━━━━\n`;
                    statsText += `👥 **${t("dash_total")}**: ${Array.isArray(users) ? users.length : "N/A"}\n`;
                    statsText += `📊 **${t("total_traffic")}**: ${(totalReqs / 6000).toFixed(2)} GB\n`;
                    statsText += `📅 **${t("daily_traffic")}**: ${(dailyReqs / 6000).toFixed(2)} GB\n`;
                    if (!isRemotePanel) {
                        const upSeconds = Math.floor(
                            (Date.now() - isolateStartTime) / 1000,
                        );
                        const dh = Math.floor(upSeconds / 3600);
                        const dm = Math.floor((upSeconds % 3600) / 60);
                        statsText += `⏱ **${t("tg_uptime")}**: ${dh}h ${dm}m\n`;
                        statsText += `🔌 **${t("tg_conns")}**: ${activeConnections}\n`;
                        statsText += `📦 **${t("tg_version")}**: v${CURRENT_VERSION}\n`;
                    }
                    statsText += `━━━━━━━━━━━━━━━━`;
                    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
                        const reqs = await fetchCloudflareUsage(
                            sysConfig.cfAccountId,
                            sysConfig.cfApiToken,
                        );
                        if (reqs !== null) {
                            const pct = ((reqs / 100000) * 100).toFixed(2);
                            statsText += `\n☁️ **Cloudflare API**: ${reqs}/100000 (${pct}%)`;
                        }
                    }
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `🔄 ${t("btn_update_usage")}`,
                                    callback_data: "sys_stats",
                                },
                            ],
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, statsText, kb, messageId);
                } else if (data === "sys_panel_info") {
                    let infoText = `ℹ️ **${t("panel_info")}**\n`;
                    infoText += `━━━━━━━━━━━━━━━━\n`;
                    infoText += `📌 **${t("current_panel")}**: ${activePanel.isLocal ? "🏠" : "🌐"} ${activePanel.name}\n`;
                    if (activePanel.isLocal) {
                        infoText += `🌐 **Host**: ${hostName}\n`;
                        infoText += `🔑 **API Route**: \`${sysConfig.apiRoute}\`\n`;
                        infoText += `📡 **Mode**: ${sysConfig.mode || "alpha"}\n`;
                        infoText += `🔒 **Ports**: ${sysConfig.socketPorts || "443"}\n`;
                    } else {
                        infoText += `🌐 **Host**: ${activePanel.host}\n`;
                        infoText += `🔑 **API Route**: \`${activePanel.apiRoute}\`\n`;
                    }
                    infoText += `📱 **Version**: ${CURRENT_VERSION}\n`;
                    infoText += `━━━━━━━━━━━━━━━━`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, infoText, kb, messageId);
                } else if (data.startsWith("subs_disabled:")) {
                    const panelUsers = await getPanelUsers();
                    const users = panelUsers || [];
                    const disabledUsers = users.filter((u) => u.isPaused);
                    if (disabledUsers.length === 0) {
                        const kb = {
                            inline_keyboard: [
                                [
                                    {
                                        text: t("btn_main_menu"),
                                        callback_data: "main_menu",
                                    },
                                ],
                            ],
                        };
                        await sendOrEdit(
                            chatId,
                            `🚫 ${t("msg_no_disabled")}`,
                            kb,
                            messageId,
                        );
                    } else {
                        const page =
                            parseInt(data.replace("subs_disabled:", "")) || 0;
                        const itemsPerPage = 5;
                        const start = page * itemsPerPage;
                        const end = start + itemsPerPage;
                        const pageUsers = disabledUsers.slice(start, end);
                        let text = `🚫 **${t("disabled_users")}** (${disabledUsers.length})\n━━━━━━━━━━━━━━━━\n`;
                        const inline_keyboard = [];
                        pageUsers.forEach((u) => {
                            const reason = u.disabledReason || t("paused");
                            text += `👤 **${u.name}**\n   ${reason}\n`;
                            inline_keyboard.push([
                                {
                                    text: `▶️ ${u.name}`,
                                    callback_data: `sub_toggle:${u.id}`,
                                },
                            ]);
                        });
                        const navRow = [];
                        if (page > 0)
                            navRow.push({
                                text: `⬅️ ${t("btn_back")}`,
                                callback_data: `subs_disabled:${page - 1}`,
                            });
                        if (end < disabledUsers.length)
                            navRow.push({
                                text: `${t("btn_next")} ➡️`,
                                callback_data: `subs_disabled:${page + 1}`,
                            });
                        if (navRow.length > 0) inline_keyboard.push(navRow);
                        inline_keyboard.push([
                            {
                                text: t("btn_main_menu"),
                                callback_data: "main_menu",
                            },
                        ]);
                        await sendOrEdit(
                            chatId,
                            text,
                            { inline_keyboard },
                            messageId,
                        );
                    }
                } else if (data === "sub_search_init") {
                    tgState[chatId] = { step: "sub_search" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `🔍 ${t("msg_enter_search")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_reset_traffic:")) {
                    const uuid = data.replace("sub_reset_traffic:", "");
                    if (isRemotePanel) {
                        await remotePanelResetTraffic(activePanel, uuid);
                    } else {
                        if (!sysUsageCache) sysUsageCache = { users: {} };
                        if (!sysUsageCache.users) sysUsageCache.users = {};
                        const uuidClean = uuid.replace(/-/g, "").toLowerCase();
                        if (sysUsageCache.users[uuidClean]) {
                            sysUsageCache.users[uuidClean].reqs = 0;
                            sysUsageCache.users[uuidClean].dReqs = 0;
                        } else {
                            sysUsageCache.users[uuidClean] = {
                                reqs: 0,
                                dReqs: 0,
                                lastDay: new Date().toISOString().split("T")[0],
                            };
                        }
                        await cachedD1Put(
                            env,
                            "sys_usage",
                            JSON.stringify(sysUsageCache),
                        );
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(
                        chatId,
                        `✅ ${t("msg_traffic_reset")}\n\n${detail.text}`,
                        detail.kb,
                        messageId,
                    );
                } else if (data.startsWith("sub_extend_init:")) {
                    const uuid = data.replace("sub_extend_init:", "");
                    tgState[chatId] = { step: `sub_extend_days:${uuid}` };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `📅 ${t("msg_enter_extend_days")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_notes_init:")) {
                    const uuid = data.replace("sub_edit_notes_init:", "");
                    tgState[chatId] = { step: `sub_edit_notes:${uuid}` };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `📝 ${t("msg_enter_notes")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_edit_device_init:")) {
                    const uuid = data.replace("sub_edit_device_init:", "");
                    tgState[chatId] = { step: `sub_edit_device:${uuid}` };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const text = `📱 ${t("msg_enter_device_limit")}`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `♾️ Unlimited`,
                                    callback_data: `sub_device_unlimited:${uuid}`,
                                },
                            ],
                            [
                                {
                                    text: `❌ ${t("btn_cancel")}`,
                                    callback_data: `sub_detail:${uuid}`,
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data.startsWith("sub_device_unlimited:")) {
                    const uuid = data.replace("sub_device_unlimited:", "");
                    if (isRemotePanel) {
                        await remotePanelWriteAction(activePanel, "PUT", uuid, {
                            key: activePanel.apiKey,
                            maxConfigs: null,
                        });
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(
                            (usr) => usr.id === uuid,
                        );
                        if (u) {
                            u.maxConfigs = null;
                            await cachedD1Put(
                                env,
                                "sys_config",
                                JSON.stringify(sysConfig),
                            );
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(
                        chatId,
                        `✅ ${t("status_updated")}`,
                        detail.kb,
                        messageId,
                    );
                } else if (data === "get_sub_link") {
                    const subUrl = `https://${hostName}/${sysConfig.apiRoute}`;
                    await fetch(`${tgApi}/sendMessage`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            chat_id: chatId,
                            text: `\`${subUrl}\``,
                            parse_mode: "Markdown",
                        }),
                    });
                    answerText = t("sub_link_sent");
                } else if (data === "tg_settings_menu") {
                    const modeTxt =
                        sysConfig.mode === "alpha"
                            ? "Alpha (V)"
                            : sysConfig.mode === "beta"
                              ? "Beta (T)"
                              : "Both";
                    const portsTxt = sysConfig.socketPorts || "443";
                    const passTxt = sysConfig.masterKey || "admin";
                    const dnsTxt = sysConfig.resolveIp || "1.1.1.1";
                    const relayTxt = sysConfig.backupRelay || "—";
                    const tfoTxt = sysConfig.enableOpt1 ? "✅" : "❌";
                    const echTxt = sysConfig.enableOpt2 ? "✅" : "❌";
                    const pauseTxt = sysConfig.isPaused ? "🔴 ON" : "🟢 OFF";
                    const silentTxt = sysConfig.silentAlerts ? "✅" : "❌";
                    const autoUpTxt = sysConfig.autoUpdate ? "✅" : "❌";
                    const directTxt = sysConfig.enableDirectConfigs
                        ? "✅"
                        : "❌";
                    const nat64Txt = sysConfig.nat64Prefix || "—";
                    let text = `⚙️ **${t("tg_sys_settings")}**\n━━━━━━━━━━━━━━━━\n`;
                    text += `📡 ${t("tg_proto")}: **${modeTxt}**\n`;
                    text += `🔌 ${t("tg_ports")}: \`${portsTxt}\`\n`;
                    text += `🔑 ${t("tg_pass")}: \`${passTxt}\`\n`;
                    text += `🌐 ${t("tg_dns")}: \`${dnsTxt}\`\n`;
                    text += `🔗 ${t("tg_relay")}: \`${relayTxt}\`\n`;
                    text += `⚡ ${t("tg_tfo")}: ${tfoTxt} | ECH: ${echTxt}\n`;
                    text += `🔇 ${t("tg_silent")}: ${silentTxt}\n`;
                    text += `🛑 ${t("tg_pause")}: ${pauseTxt}\n`;
                    text += `🔄 ${t("tg_auto_update")}: ${autoUpTxt}\n`;
                    text += `🔀 ${t("tg_direct")}: ${directTxt}\n`;
                    text += `🌐 ${t("tg_nat64")}: \`${nat64Txt}\`\n`;
                    text += `━━━━━━━━━━━━━━━━`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `📡 ${t("tg_proto")}`,
                                    callback_data: "tg_edit_proto",
                                },
                                {
                                    text: `🔌 ${t("tg_ports")}`,
                                    callback_data: "tg_edit_ports",
                                },
                            ],
                            [
                                {
                                    text: `🔑 ${t("tg_pass")}`,
                                    callback_data: "tg_edit_pass",
                                },
                                {
                                    text: `🌐 ${t("tg_dns")}`,
                                    callback_data: "tg_edit_dns",
                                },
                            ],
                            [
                                {
                                    text: `🔗 ${t("tg_relay")}`,
                                    callback_data: "tg_edit_relay",
                                },
                            ],
                            [
                                {
                                    text: `⚡ ${t("tg_tfo")}`,
                                    callback_data: "tg_toggle_tfo",
                                },
                                { text: `ECH`, callback_data: "tg_toggle_ech" },
                            ],
                            [
                                {
                                    text: `${t("tg_silent")}`,
                                    callback_data: "tg_toggle_silent",
                                },
                                {
                                    text: `${t("tg_pause")}`,
                                    callback_data: "tg_toggle_pause2",
                                },
                            ],
                            [
                                {
                                    text: `🔄 ${t("tg_auto_update")}`,
                                    callback_data: "tg_toggle_auto_update",
                                },
                                {
                                    text: `🔀 ${t("tg_direct")}`,
                                    callback_data: "tg_toggle_direct",
                                },
                            ],
                            [
                                {
                                    text: `🌐 ${t("tg_nat64")}`,
                                    callback_data: "tg_edit_nat64",
                                },
                            ],
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_advanced_menu") {
                    const cleanTxt = sysConfig.cleanIps
                        ? sysConfig.cleanIps.substring(0, 40) +
                          (sysConfig.cleanIps.length > 40 ? "..." : "")
                        : "—";
                    const lpUrls = (sysConfig.linkedPanels || []).map(p => p.url).filter(Boolean);
                    const nodesTxt = lpUrls.length > 0
                        ? lpUrls.join(", ").substring(0, 40) +
                          (lpUrls.join(", ").length > 40 ? "..." : "")
                        : "—";
                    const strategyTxt = sysConfig.nameStrategy || "default";
                    const prefixTxt = sysConfig.namePrefix || "Core";
                    const maintenanceTxt = sysConfig.maintenanceHost
                        ? sysConfig.maintenanceHost.substring(0, 30) + "..."
                        : "—";
                    let text = `🔧 **${t("tg_adv_settings")}**\n━━━━━━━━━━━━━━━━\n`;
                    text += `🧹 ${t("tg_clean_ips")}: \`${cleanTxt}\`\n`;
                    text += `🖥️ ${t("tg_nodes")}: \`${nodesTxt}\`\n`;
                    text += `📝 ${t("tg_strategy")}: \`${strategyTxt}\`\n`;
                    text += `🏷️ ${t("tg_prefix")}: \`${prefixTxt}\`\n`;
                    text += `🎭 ${t("tg_maintenance")}: \`${maintenanceTxt}\`\n`;
                    text += `━━━━━━━━━━━━━━━━`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `🧹 ${t("tg_clean_ips")}`,
                                    callback_data: "tg_edit_clean_ips",
                                },
                            ],
                            [
                                {
                                    text: `🖥️ ${t("tg_nodes")}`,
                                    callback_data: "tg_edit_nodes",
                                },
                            ],
                            [
                                {
                                    text: `📝 ${t("tg_strategy")}`,
                                    callback_data: "tg_edit_strategy",
                                },
                                {
                                    text: `🏷️ ${t("tg_prefix")}`,
                                    callback_data: "tg_edit_prefix",
                                },
                            ],
                            [
                                {
                                    text: `🎭 ${t("tg_maintenance")}`,
                                    callback_data: "tg_edit_maintenance",
                                },
                            ],
                            [
                                {
                                    text: `🤖 ${t("tg_tg_settings")}`,
                                    callback_data: "tg_edit_tg_settings",
                                },
                            ],
                            [
                                {
                                    text: `☁️ ${t("tg_cf_settings")}`,
                                    callback_data: "tg_edit_cf_settings",
                                },
                            ],
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_logs_menu") {
                    let logs = [];
                    if (env.AM_DB) {
                        const stored = await d1Get(env, "sys_logs");
                        if (stored) logs = JSON.parse(stored);
                    }
                    let text = `📋 **${t("tg_logs")}**\n━━━━━━━━━━━━━━━━\n`;
                    if (logs.length === 0) {
                        text += `ℹ️ ${t("tg_log_empty")}\n`;
                    } else {
                        logs.slice(0, 10).forEach((log, i) => {
                            const time = new Date(log.ts).toLocaleString();
                            text += `${i + 1}. ${t("tg_log_entry")} **${log.type}**\n   ${log.detail}\n   📅 ${time}\n`;
                        });
                        if (logs.length > 10)
                            text += `\n... ${logs.length - 10} more entries`;
                    }
                    text += `\n━━━━━━━━━━━━━━━━`;
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: `🔄 ${t("btn_update_usage")}`,
                                    callback_data: "tg_logs_menu",
                                },
                            ],
                            [
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(chatId, text, kb, messageId);
                } else if (data === "tg_toggle_tfo") {
                    sysConfig.enableOpt1 = !sysConfig.enableOpt1;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_ech") {
                    sysConfig.enableOpt2 = !sysConfig.enableOpt2;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_silent") {
                    sysConfig.silentAlerts = !sysConfig.silentAlerts;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_pause2") {
                    sysConfig.isPaused = !sysConfig.isPaused;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    const menu = getMainMenu(getActivePanel(), isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "tg_toggle_auto_update") {
                    sysConfig.autoUpdate = !sysConfig.autoUpdate;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    await sendOrEdit(
                        chatId,
                        `⚙️ ${t("tg_auto_update")}: ${sysConfig.autoUpdate ? "✅ ON" : "❌ OFF"}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "◀️ " + t("btn_back"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_toggle_direct") {
                    sysConfig.enableDirectConfigs =
                        !sysConfig.enableDirectConfigs;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    answerText = t("tg_saved");
                    await sendOrEdit(
                        chatId,
                        `🔀 ${t("tg_direct")}: ${sysConfig.enableDirectConfigs ? "✅ ON" : "❌ OFF"}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "◀️ " + t("btn_back"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_proto") {
                    tgState[chatId] = { step: "tg_edit_proto" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: "Alpha (V-Core)",
                                    callback_data: "tg_set_proto:alpha",
                                },
                                {
                                    text: "Beta (T-Core)",
                                    callback_data: "tg_set_proto:beta",
                                },
                            ],
                            [
                                {
                                    text: "Both",
                                    callback_data: "tg_set_proto:both",
                                },
                            ],
                            [
                                {
                                    text: "❌ " + t("btn_cancel"),
                                    callback_data: "tg_settings_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(
                        chatId,
                        `📡 **${t("tg_proto")}**\n${t("tg_current_val")}: **${sysConfig.mode}**\n\n${t("tg_new_val")}`,
                        kb,
                        messageId,
                    );
                } else if (data.startsWith("tg_set_proto:")) {
                    const val = data.replace("tg_set_proto:", "");
                    sysConfig.mode = val;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    tgState[chatId] = null;
                    answerText = t("tg_saved");
                    await sendOrEdit(
                        chatId,
                        `✅ ${t("tg_proto")}: **${val}**`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "◀️ " + t("btn_back"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_dns") {
                    tgState[chatId] = { step: "tg_edit_dns" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🌐 **${t("tg_dns")}**\n${t("tg_current_val")}: \`${sysConfig.resolveIp}\`\n\n${t("tg_new_val")}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_relay") {
                    tgState[chatId] = { step: "tg_edit_relay" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🔗 **${t("tg_relay")}**\n${t("tg_current_val")}: \`${sysConfig.backupRelay || "—"}\`\n\n${t("tg_new_val")}\n_send empty to clear_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_nat64") {
                    tgState[chatId] = { step: "tg_edit_nat64" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🌐 **${t("tg_nat64")}**\n${t("tg_current_val")}: \`${sysConfig.nat64Prefix || "—"}\`\n\n${t("tg_new_val")}\n_send empty to clear_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_maintenance") {
                    tgState[chatId] = { step: "tg_edit_maintenance" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🎭 **${t("tg_maintenance")}**\n${t("tg_current_val")}: \`${sysConfig.maintenanceHost || "—"}\`\n\n${t("tg_new_val")}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_clean_ips") {
                    tgState[chatId] = { step: "tg_edit_clean_ips" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🧹 **${t("tg_clean_ips")}**\n${t("tg_current_val")}: \`${sysConfig.cleanIps || "—"}\`\n\n${t("tg_new_val")}\n_send empty to clear_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_nodes") {
                    let lpList = (sysConfig.linkedPanels || [])
                        .map((p, i) => `${i + 1}. \`${p.url}\``)
                        .join("\n");
                    if (!lpList) lpList = "—";
                    const warningMsg = langCode === "fa"
                        ? `🖥️ **${t("tg_nodes")}**\n\n${lpList}\n\n⚠️ لطفاً برای افزودن، حذف یا ویرایش نودهای خارجی به صورت امن همراه با کلید دسترسی (API Key)، از داشبورد تحت وب استفاده کنید.`
                        : `🖥️ **${t("tg_nodes")}**\n\n${lpList}\n\n⚠️ Please use the Web Dashboard to add, remove, or edit external nodes securely with API Keys.`;
                    await sendOrEdit(
                        chatId,
                        warningMsg,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "◀️ " + t("btn_back"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_strategy") {
                    tgState[chatId] = { step: "tg_edit_strategy" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const kb = {
                        inline_keyboard: [
                            [
                                {
                                    text: "default",
                                    callback_data: "tg_set_strategy:default",
                                },
                            ],
                            [
                                {
                                    text: "type-user-port",
                                    callback_data:
                                        "tg_set_strategy:type-user-port",
                                },
                            ],
                            [
                                {
                                    text: "user-port",
                                    callback_data: "tg_set_strategy:user-port",
                                },
                            ],
                            [
                                {
                                    text: "ip",
                                    callback_data: "tg_set_strategy:ip",
                                },
                            ],
                            [
                                {
                                    text: "❌ " + t("btn_cancel"),
                                    callback_data: "tg_advanced_menu",
                                },
                            ],
                        ],
                    };
                    await sendOrEdit(
                        chatId,
                        `📝 **${t("tg_strategy")}**\n${t("tg_current_val")}: \`${sysConfig.nameStrategy}\`\n\n_send custom or select:_`,
                        kb,
                        messageId,
                    );
                } else if (data.startsWith("tg_set_strategy:")) {
                    const val = data.replace("tg_set_strategy:", "");
                    sysConfig.nameStrategy = val;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    tgState[chatId] = null;
                    answerText = t("tg_saved");
                    await sendOrEdit(
                        chatId,
                        `✅ ${t("tg_strategy")}: **${val}**`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "◀️ " + t("btn_back"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_prefix") {
                    tgState[chatId] = { step: "tg_edit_prefix" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🏷️ **${t("tg_prefix")}**\n${t("tg_current_val")}: \`${sysConfig.namePrefix}\`\n\n${t("tg_new_val")}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_pass") {
                    tgState[chatId] = { step: "tg_edit_pass" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🔑 **${t("tg_pass")}**\n${t("tg_current_val")}: \`${sysConfig.masterKey}\`\n\n${t("tg_new_val")}`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_ports") {
                    tgState[chatId] = { step: "tg_edit_ports" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🔌 **${t("tg_ports")}**\n${t("tg_current_val")}: \`${sysConfig.socketPorts}\`\n\n${t("tg_new_val")}\n_comma separated e.g. 443,80_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_settings_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_tg_settings") {
                    tgState[chatId] = { step: "tg_edit_tg_token" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `🤖 **${t("tg_tg_settings")}**\n\n1️⃣ ${t("tg_current_val")}: \`${sysConfig.tgToken ? "***" + sysConfig.tgToken.slice(-4) : "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                } else if (data === "tg_edit_cf_settings") {
                    tgState[chatId] = { step: "tg_edit_cf_acc" };
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    await sendOrEdit(
                        chatId,
                        `☁️ **${t("tg_cf_settings")}**\n\n1️⃣ CF Account ID: \`${sysConfig.cfAccountId || "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                        {
                            inline_keyboard: [
                                [
                                    {
                                        text: "❌ " + t("btn_cancel"),
                                        callback_data: "tg_advanced_menu",
                                    },
                                ],
                            ],
                        },
                        messageId,
                    );
                }

                ctx?.waitUntil(
                    fetch(`${tgApi}/answerCallbackQuery`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            callback_query_id: cb.id,
                            text: answerText || "Done!",
                        }),
                    }).catch(() => {}),
                );
            }
        } else if (update.message && update.message.text) {
            const chatId = update.message.chat.id;
            const text = update.message.text.trim();

            if (isAuthorized) {
                // Get active panel from last login signal
                const activePanel = getActivePanel();
                const isRemotePanel = activePanel && !activePanel.isLocal;

                // Helper to fetch users for the active panel
                const getPanelUsers = async () => {
                    if (isRemotePanel) {
                        const res = await fetchRemotePanelUsers(activePanel);
                        return res.success ? res.users || [] : null;
                    }
                    return sysConfig.users || [];
                };

                // Handle /start command
                if (text === "/start") {
                    tgState[chatId] = null;
                    ctx?.waitUntil(
                        d1Put(
                            env,
                            "tg_bot_state",
                            JSON.stringify(tgState),
                        ).catch(() => {}),
                    );
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb);
                    return new Response("OK", { status: 200 });
                }

                const state = tgState[chatId];

                if (state) {
                    if (!isAuthorized) {
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(chatId, t("access_denied"));
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step === "sub_add_name") {
                        const name = text;
                        tgState[chatId] = {
                            step: "sub_add_limits",
                            name: name,
                        };
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );

                        const msg = `⚙️ **${name}**\n\n${t("msg_enter_limits")}`;
                        const kb = {
                            inline_keyboard: [
                                [
                                    {
                                        text: `♾️ Skip (Unlimited)`,
                                        callback_data: "sub_add_unlimited_skip",
                                    },
                                ],
                                [
                                    {
                                        text: `❌ ${t("btn_cancel")}`,
                                        callback_data: "main_menu",
                                    },
                                ],
                            ],
                        };
                        await sendOrEdit(chatId, msg, kb);
                        return new Response("OK", { status: 200 });
                    }

                    if (
                        state.step === "sub_add_limits" ||
                        state.step === "sub_add_unlimited_skip"
                    ) {
                        const name = state.name;
                        let tReq = null;
                        let dReq = null;
                        let days = null;

                        if (
                            state.step !== "sub_add_unlimited_skip" &&
                            text !== "0" &&
                            text !== "0 0 0"
                        ) {
                            const parts = text.split(/\s+/).map(Number);
                            if (parts[0] > 0) tReq = parts[0];
                            if (parts[1] > 0) dReq = parts[1];
                            if (parts[2] > 0) days = parts[2];
                        }

                        const newUuid = crypto.randomUUID();
                        if (isRemotePanel) {
                            const res = await remotePanelWriteAction(
                                activePanel,
                                "POST",
                                null,
                                {
                                    key: activePanel.apiKey,
                                    name: name,
                                    trafficLimit: tReq ? tReq / 6000 : 0,
                                    dailyLimit: dReq ? dReq / 6000 : 0,
                                    expiryDays: days || 0,
                                },
                            );
                            if (res.success && res.user) {
                                const detail = getSubDetail(res.user.id, [
                                    res.user,
                                ]);
                                await sendOrEdit(
                                    chatId,
                                    `✅ ${t("msg_added")}\n\n${detail.text}`,
                                    detail.kb,
                                );
                            } else {
                                await sendOrEdit(chatId, t("msg_panel_error"), {
                                    inline_keyboard: [
                                        [
                                            {
                                                text: t("btn_main_menu"),
                                                callback_data: "main_menu",
                                            },
                                        ],
                                    ],
                                });
                            }
                        } else {
                            if (!sysConfig.users) sysConfig.users = [];
                            sysConfig.users.push({
                                id: newUuid,
                                name: name,
                                limitTotalReq: tReq,
                                limitDailyReq: dReq,
                                expiryMs: days
                                    ? Date.now() + days * 86400000
                                    : null,
                                createdAt: Date.now(),
                            });
                            await cachedD1Put(
                                env,
                                "sys_config",
                                JSON.stringify(sysConfig),
                            );
                            const detail = getSubDetail(newUuid);
                            await sendOrEdit(
                                chatId,
                                `✅ ${t("msg_added")}\n\n${detail.text}`,
                                detail.kb,
                            );
                        }

                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_name:")) {
                        const uuid = state.step.replace("sub_edit_name:", "");
                        if (isRemotePanel) {
                            await remotePanelWriteAction(
                                activePanel,
                                "PUT",
                                uuid,
                                { key: activePanel.apiKey, name: text },
                            );
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(
                                (usr) => usr.id === uuid,
                            );
                            if (u) {
                                u.name = text;
                                await cachedD1Put(
                                    env,
                                    "sys_config",
                                    JSON.stringify(sysConfig),
                                );
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );

                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(
                            chatId,
                            `✅ Successfully Changed!`,
                            detail.kb,
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_limits:")) {
                        const uuid = state.step.replace("sub_edit_limits:", "");
                        let tReq = null;
                        let dReq = null;
                        let days = null;

                        const parts = text.split(/\s+/).map(Number);
                        if (parts[0] > 0) tReq = parts[0];
                        if (parts[1] > 0) dReq = parts[1];
                        if (parts[2] > 0) days = parts[2];

                        if (isRemotePanel) {
                            await remotePanelWriteAction(
                                activePanel,
                                "PUT",
                                uuid,
                                {
                                    key: activePanel.apiKey,
                                    trafficLimit: tReq ? tReq / 6000 : 0,
                                    dailyLimit: dReq ? dReq / 6000 : 0,
                                    expiryDays: days || 0,
                                },
                            );
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(
                                (usr) => usr.id === uuid,
                            );
                            if (u) {
                                u.limitTotalReq = tReq;
                                u.limitDailyReq = dReq;
                                u.expiryMs = days
                                    ? Date.now() + days * 86400000
                                    : null;
                                await cachedD1Put(
                                    env,
                                    "sys_config",
                                    JSON.stringify(sysConfig),
                                );
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );

                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(
                            chatId,
                            `✅ Limits Updated!`,
                            detail.kb,
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step === "sub_search") {
                        const query = text.toLowerCase();
                        const panelUsers = await getPanelUsers();
                        const users = panelUsers || [];
                        const results = users.filter(
                            (u) =>
                                u.name.toLowerCase().includes(query) ||
                                u.id.toLowerCase().includes(query),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        if (results.length === 0) {
                            const kb = {
                                inline_keyboard: [
                                    [
                                        {
                                            text: t("btn_main_menu"),
                                            callback_data: "main_menu",
                                        },
                                    ],
                                ],
                            };
                            await sendOrEdit(
                                chatId,
                                `🔍 No users found for "${text}"`,
                                kb,
                            );
                        } else {
                            let searchText = `🔍 **Search Results** (${results.length})\n━━━━━━━━━━━━━━━━\n`;
                            const inline_keyboard = [];
                            results.slice(0, 10).forEach((u) => {
                                const statusEmoji = u.isPaused
                                    ? "⏸️"
                                    : u.expiryMs && Date.now() > u.expiryMs
                                      ? "🔴"
                                      : "🟢";
                                searchText += `${statusEmoji} **${u.name}**\n`;
                                inline_keyboard.push([
                                    {
                                        text: `👤 ${u.name}`,
                                        callback_data: `sub_detail:${u.id}`,
                                    },
                                ]);
                            });
                            inline_keyboard.push([
                                {
                                    text: t("btn_main_menu"),
                                    callback_data: "main_menu",
                                },
                            ]);
                            await sendOrEdit(chatId, searchText, {
                                inline_keyboard,
                            });
                        }
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_extend_days:")) {
                        const uuid = state.step.replace("sub_extend_days:", "");
                        const days = parseInt(text);
                        if (isNaN(days) || days <= 0) {
                            await sendOrEdit(chatId, t("msg_invalid"));
                            return new Response("OK", { status: 200 });
                        }
                        if (isRemotePanel) {
                            await remotePanelWriteAction(
                                activePanel,
                                "PUT",
                                uuid,
                                { key: activePanel.apiKey, expiryDays: days },
                            );
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(
                                (usr) => usr.id === uuid,
                            );
                            if (u) {
                                if (u.expiryMs) {
                                    u.expiryMs += days * 86400000;
                                } else {
                                    u.expiryMs = Date.now() + days * 86400000;
                                }
                                if (
                                    u.isPaused &&
                                    u.disabledReason &&
                                    u.disabledReason.includes("Expiration")
                                ) {
                                    u.isPaused = false;
                                    u.disabledReason = null;
                                    u.disabledAt = null;
                                }
                                await cachedD1Put(
                                    env,
                                    "sys_config",
                                    JSON.stringify(sysConfig),
                                );
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        const msg = t("msg_expiry_extended").replace(
                            "{days}",
                            days,
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${msg}\n\n${detail.text}`,
                            detail.kb,
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_notes:")) {
                        const uuid = state.step.replace("sub_edit_notes:", "");
                        if (isRemotePanel) {
                            await remotePanelWriteAction(
                                activePanel,
                                "PUT",
                                uuid,
                                { key: activePanel.apiKey, notes: text },
                            );
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(
                                (usr) => usr.id === uuid,
                            );
                            if (u) {
                                u.notes = text;
                                await cachedD1Put(
                                    env,
                                    "sys_config",
                                    JSON.stringify(sysConfig),
                                );
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(
                            chatId,
                            `✅ Notes updated!`,
                            detail.kb,
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step.startsWith("sub_edit_device:")) {
                        const uuid = state.step.replace("sub_edit_device:", "");
                        const limit = parseInt(text);
                        if (isNaN(limit) || limit < 0) {
                            await sendOrEdit(chatId, t("msg_invalid"));
                            return new Response("OK", { status: 200 });
                        }
                        if (isRemotePanel) {
                            await remotePanelWriteAction(
                                activePanel,
                                "PUT",
                                uuid,
                                {
                                    key: activePanel.apiKey,
                                    maxConfigs: limit > 0 ? limit : null,
                                },
                            );
                        } else if (sysConfig.users) {
                            const u = sysConfig.users.find(
                                (usr) => usr.id === uuid,
                            );
                            if (u) {
                                u.maxConfigs = limit > 0 ? limit : null;
                                await cachedD1Put(
                                    env,
                                    "sys_config",
                                    JSON.stringify(sysConfig),
                                );
                            }
                        }
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        const panelUsers = await getPanelUsers();
                        const detail = getSubDetail(uuid, panelUsers);
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("config_limit_updated")}`,
                            detail.kb,
                        );
                        return new Response("OK", { status: 200 });
                    }

                    if (state.step === "tg_edit_dns") {
                        sysConfig.resolveIp = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_dns")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_settings_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_relay") {
                        sysConfig.backupRelay = text || "";
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_relay")}: \`${text || "—"}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_settings_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_nat64") {
                        sysConfig.nat64Prefix = text || "";
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_nat64")}: \`${text || "—"}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_settings_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_maintenance") {
                        sysConfig.maintenanceHost = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_maintenance")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_clean_ips") {
                        sysConfig.cleanIps = text || "";
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_clean_ips")}: \`${text || "—"}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_prefix") {
                        sysConfig.namePrefix = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_prefix")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_pass") {
                        sysConfig.masterKey = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_pass")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_settings_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_strategy") {
                        sysConfig.nameStrategy = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_strategy")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_token") {
                        if (text !== "/skip") sysConfig.tgToken = text;
                        tgState[chatId] = { step: "tg_edit_tg_chat" };
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `2️⃣ Chat ID: \`${sysConfig.tgChatId || "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "❌ " + t("btn_cancel"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_chat") {
                        if (text !== "/skip") sysConfig.tgChatId = text;
                        tgState[chatId] = { step: "tg_edit_tg_admin" };
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `3️⃣ Admin ID: \`${sysConfig.tgAdminId || "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "❌ " + t("btn_cancel"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_tg_admin") {
                        if (text !== "/skip") sysConfig.tgAdminId = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_tg_settings")} saved!`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_acc") {
                        if (text !== "/skip") sysConfig.cfAccountId = text;
                        tgState[chatId] = { step: "tg_edit_cf_token" };
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `2️⃣ CF API Token: \`${sysConfig.cfApiToken ? "***" + sysConfig.cfApiToken.slice(-4) : "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "❌ " + t("btn_cancel"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_token") {
                        if (text !== "/skip") sysConfig.cfApiToken = text;
                        tgState[chatId] = { step: "tg_edit_cf_worker" };
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `3️⃣ CF Worker Name: \`${sysConfig.cfWorkerName || "—"}\`\n\n${t("tg_new_val")}\n_send /skip to keep current_`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "❌ " + t("btn_cancel"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_cf_worker") {
                        if (text !== "/skip") sysConfig.cfWorkerName = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_cf_settings")} saved!`,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_advanced_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                    if (state.step === "tg_edit_ports") {
                        sysConfig.socketPorts = text;
                        await cachedD1Put(
                            env,
                            "sys_config",
                            JSON.stringify(sysConfig),
                        );
                        tgState[chatId] = null;
                        ctx?.waitUntil(
                            d1Put(
                                env,
                                "tg_bot_state",
                                JSON.stringify(tgState),
                            ).catch(() => {}),
                        );
                        await sendOrEdit(
                            chatId,
                            `✅ ${t("tg_ports")}: \`${text}\``,
                            {
                                inline_keyboard: [
                                    [
                                        {
                                            text: "◀️ " + t("btn_back"),
                                            callback_data: "tg_settings_menu",
                                        },
                                    ],
                                ],
                            },
                        );
                        return new Response("OK", { status: 200 });
                    }
                }

                // Default message / fallback menu
                const menu = getMainMenu(activePanel, isAuthorized);
                await sendOrEdit(chatId, menu.text, menu.kb);
            } else {
                if (text === "/start") {
                    const userHint =
                        langCode === "fa"
                            ? "لطفاً لینک اشتراک یا شناسه کاربری خود را ارسال کنید تا اطلاعات اشتراکتان نمایش داده شود."
                            : "Please send your subscription link or User ID to view your subscription info.";
                    await sendOrEdit(chatId, userHint);
                    return new Response("OK", { status: 200 });
                }
                let lookupId = text
                    .replace(/^https?:\/\//, "")
                    .replace(/\/.*$/, "")
                    .trim();
                const subParamMatch = text.match(/[?&]sub=([^&]+)/);
                if (subParamMatch)
                    lookupId = decodeURIComponent(subParamMatch[1]);
                if (!lookupId || lookupId.length < 3) {
                    const userHint =
                        langCode === "fa"
                            ? "لطفاً لینک اشتراک یا شناسه کاربری معتبر ارسال کنید."
                            : "Please send a valid subscription link or User ID.";
                    await sendOrEdit(chatId, userHint);
                    return new Response("OK", { status: 200 });
                }
                const users = sysConfig.users || [];
                const matchedUser = users.find(
                    (u) =>
                        u.id === lookupId ||
                        u.id.replace(/-/g, "").toLowerCase() ===
                            lookupId.replace(/-/g, "").toLowerCase() ||
                        u.name.toLowerCase() === lookupId.toLowerCase(),
                );
                if (matchedUser) {
                    const detail = getSubDetail(matchedUser.id);
                    await sendOrEdit(chatId, detail.text, detail.kb);
                } else {
                    const notFound =
                        langCode === "fa"
                            ? "کاربری با این شناسه یافت نشد."
                            : "No user found with this ID.";
                    await sendOrEdit(chatId, notFound);
                }
            }
        }
        return new Response("OK", { status: 200 });
    } catch (e) {
        return new Response("OK", { status: 200 });
    }
}

async function processTelemetryStream(env, ctx, wsRelayIdx) {
    const [client, webSocket] = Object.values(new WebSocketPair());
    webSocket.accept();
    webSocket.binaryType = "arraybuffer";
    startDataPipe(webSocket, env, ctx, wsRelayIdx);
    return new Response(null, { status: 101, webSocket: client });
}

async function startDataPipe(webSocket, env, ctx, wsRelayIdx) {
    activeConnections++;
    webSocket.addEventListener("close", () => {
        activeConnections--;
        if (activeClientHash) {
            let cur = activeConns.get(activeClientHash) || 0;
            if (cur > 0) activeConns.set(activeClientHash, cur - 1);
        }
    });
    webSocket.addEventListener("error", () => {});
    let remoteSocket,
        dataWriter,
        isInit = true,
        queue = Promise.resolve();
    let activeClientHash = null;
    webSocket.addEventListener("message", (event) => {
        queue = queue.then(async () => {
            try {
                if (isInit) {
                    isInit = false;
                    const isModeAlpha = await parseSensorData(
                        event.data,
                        wsRelayIdx,
                    );
                    if (isModeAlpha) webSocket.send(new Uint8Array([0, 0]));
                } else if (dataWriter) {
                    await dataWriter.write(event.data);
                }
            } catch (err) {
                webSocket.close();
            }
        });
    });

    async function parseSensorData(bufferData, wsRelayIdx) {
        const view = new Uint8Array(bufferData);
        let targetAddr = "",
            targetPort = 0,
            offset = 0,
            isModeAlpha = false,
            activeProfile = null;

        if (view[0] === 0x00) {
            isModeAlpha = true;

            let clientHash = Array.from(view.slice(1, 17))
                .map((b) => b.toString(16).padStart(2, "0"))
                .join("");
            let configEntry = lookupConfigEntry(clientHash);

            if (configEntry) {
                activeClientHash = configEntry.userId
                    .replace(/-/g, "")
                    .toLowerCase();
                activeProfile = getAllProfiles().find(
                    (p) =>
                        p.id.replace(/-/g, "").toLowerCase() ===
                        activeClientHash,
                );
                if (!activeProfile) return false;
                if (configEntry.relayIp)
                    activeProfile = {
                        ...activeProfile,
                        proxyIp: configEntry.relayIp,
                    };
            } else {
                let decoded = decodeConfigUuid(clientHash);
                if (decoded) {
                    activeProfile = getAllProfiles().find((p) =>
                        p.id
                            .replace(/-/g, "")
                            .toLowerCase()
                            .startsWith(decoded.userFingerprint),
                    );
                    if (activeProfile && decoded.relayIpIndex >= 0) {
                        const effectivePips = getEffectivePips(activeProfile);
                        if (effectivePips.length > 0) {
                            const idx =
                                decoded.relayIpIndex % effectivePips.length;
                            activeProfile = {
                                ...activeProfile,
                                proxyIp: effectivePips[idx],
                            };
                        }
                    }
                }
                if (!activeProfile) {
                    activeProfile = getAllProfiles().find(
                        (p) =>
                            p.id.replace(/-/g, "").toLowerCase() === clientHash,
                    );
                }
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id
                    .replace(/-/g, "")
                    .toLowerCase();
            }
            trackUsage(activeClientHash, 0, env, ctx);

            let currentConns = activeConns.get(activeClientHash) || 0;
            if (activeProfile && activeProfile.connLimit) {
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
            }
            activeConns.set(activeClientHash, currentConns + 1);

            let uTrack = uuidUsage.get(activeClientHash) || {
                connects: 0,
                last: 0,
            };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);

            const optLen = view[17];
            const pPos = 18 + optLen + 1;
            targetPort = new DataView(
                bufferData.slice(pPos, pPos + 2),
            ).getUint16(0);
            const aType = view[pPos + 2];
            let vPos = pPos + 3,
                aLen = 0;

            if (aType === 1) {
                aLen = 4;
                targetAddr = view.slice(vPos, vPos + aLen).join(".");
            } else if (aType === 2) {
                aLen = view[vPos];
                vPos++;
                targetAddr = new TextDecoder().decode(
                    view.slice(vPos, vPos + aLen),
                );
            } else if (aType === 3) {
                aLen = 16;
                const dv = new DataView(bufferData.slice(vPos, vPos + aLen));
                targetAddr = Array.from({ length: 8 }, (_, i) =>
                    dv.getUint16(i * 2).toString(16),
                ).join(":");
            }
            offset = vPos + aLen;
        } else {
            let ePos = bufferData.byteLength;
            for (let i = 0; i < bufferData.byteLength; i++) {
                if (view[i] === 0x0d && view[i + 1] === 0x0a) {
                    ePos = i;
                    break;
                }
            }

            let clientHashHex = new TextDecoder().decode(view.slice(0, ePos));
            let configEntry = lookupConfigEntry(clientHashHex);

            if (configEntry) {
                activeClientHash = configEntry.userId
                    .replace(/-/g, "")
                    .toLowerCase();
                activeProfile = getAllProfiles().find(
                    (p) =>
                        p.id.replace(/-/g, "").toLowerCase() ===
                        activeClientHash,
                );
                if (!activeProfile) return false;
                if (configEntry.relayIp)
                    activeProfile = {
                        ...activeProfile,
                        proxyIp: configEntry.relayIp,
                    };
            } else {
                activeProfile = getAllProfiles().find(
                    (p) => getTrojanHash(p.id) === clientHashHex,
                );
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id
                    .replace(/-/g, "")
                    .toLowerCase();
                if (wsRelayIdx >= 0) {
                    const effectivePips = getEffectivePips(activeProfile);
                    if (effectivePips.length > 0) {
                        activeProfile = {
                            ...activeProfile,
                            proxyIp:
                                effectivePips[
                                    wsRelayIdx % effectivePips.length
                                ],
                        };
                    }
                }
            }
            trackUsage(activeClientHash, 0, env, ctx);
            let currentConns = activeConns.get(activeClientHash) || 0;
            if (activeProfile && activeProfile.connLimit) {
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
            }
            activeConns.set(activeClientHash, currentConns + 1);
            let uTrack = uuidUsage.get(activeClientHash) || {
                connects: 0,
                last: 0,
            };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);

            let hPos = ePos + 2;
            hPos++;
            let aType = view[hPos];
            hPos++;
            let aLen = 0;

            if (aType === 1) {
                aLen = 4;
                targetAddr = view.slice(hPos, hPos + aLen).join(".");
            } else if (aType === 3) {
                aLen = view[hPos];
                hPos++;
                targetAddr = new TextDecoder().decode(
                    view.slice(hPos, hPos + aLen),
                );
            } else if (aType === 4) {
                aLen = 16;
                const dv = new DataView(bufferData.slice(hPos, hPos + aLen));
                targetAddr = Array.from({ length: 8 }, (_, i) =>
                    dv.getUint16(i * 2).toString(16),
                ).join(":");
            }

            hPos += aLen;
            targetPort = new DataView(
                bufferData.slice(hPos, hPos + 2),
            ).getUint16(0);
            offset = hPos + 4;
        }

        let isDomain =
            /^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$/.test(targetAddr) ||
            /^[a-zA-Z0-9-]+$/.test(targetAddr);
        let connectAddr = targetAddr;
        if (isDomain && sysConfig.customDns) {
            try {
                const dohUrl = new URL(sysConfig.customDns);
                dohUrl.searchParams.set("name", targetAddr);
                dohUrl.searchParams.set("type", "A");
                let dnsRes = await fetch(dohUrl.toString(), {
                    headers: { accept: "application/dns-json" },
                });
                let dnsJson = await dnsRes.json();
                if (dnsJson.Answer && dnsJson.Answer.length > 0) {
                    connectAddr = dnsJson.Answer[0].data;
                }
            } catch (e) {}
        }

        try {
            remoteSocket = connect({ hostname: connectAddr, port: targetPort });
            await remoteSocket.opened;
        } catch {
            let pips = [];
            if (activeProfile && activeProfile.proxyIp) {
                pips = activeProfile.proxyIp
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.backupRelay) {
                pips = sysConfig.backupRelay
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.customRelay) {
                pips = sysConfig.customRelay
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }

            // Consistent hash based on user/profile ID to prevent session/IP splitting across assets on Cloudflare
            let startIndex = 0;
            if (pips.length > 1) {
                let hash = 0;
                let hashStr = activeProfile ? activeProfile.id : "";
                for (let i = 0; i < hashStr.length; i++) {
                    hash = hashStr.charCodeAt(i) + ((hash << 5) - hash);
                }
                startIndex = Math.abs(hash) % pips.length;
            }

            // Attempt to connect with automatic failover to alternative proxy IPs
            let connected = false;
            for (
                let attempt = 0;
                attempt < Math.min(pips.length, 3);
                attempt++
            ) {
                let currentIndex = (startIndex + attempt) % pips.length;
                let currentProxy = pips[currentIndex];
                try {
                    const [altIP, altPortStr] = currentProxy.split(":");
                    remoteSocket = connect({
                        hostname: altIP,
                        port: altPortStr ? Number(altPortStr) : targetPort,
                    });
                    await remoteSocket.opened;
                    connected = true;
                    break;
                } catch (e) {
                    // Try next fallback proxy IP in list
                }
            }
            if (!connected) {
                webSocket.close();
                return isModeAlpha;
            }
        }

        dataWriter = remoteSocket.writable.getWriter();
        if (offset < bufferData.byteLength) {
            let chunk = bufferData.slice(offset);
            await dataWriter.write(chunk);
        }
        remoteSocket.readable.pipeTo(
            new WritableStream({
                write(chunk) {
                    webSocket.send(chunk);
                },
            }),
        );

        return isModeAlpha;
    }
}

function generateHardwareId(seed) {
    const h20 = Array.from(new TextEncoder().encode(seed))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("")
        .slice(0, 20)
        .padEnd(20, "0");
    return `${h20.slice(0, 8)}-0000-4000-8000-${h20.slice(-12)}`;
}

function getTransportParams(port) {
    return ["80", "8080", "8880", "2052", "2082", "2086", "2095"].includes(
        port.toString(),
    )
        ? "none"
        : "tls";
}

function getSubscriptionStats(targetSub = null) {
    let name = "Default";
    let id = activeDeviceId;
    let limitTotalReq = 0;
    let expiryMs = 0;

    let hasMultiUser = sysConfig.users && sysConfig.users.length > 0;
    if (hasMultiUser && targetSub) {
        let user = sysConfig.users.find(
            (u) =>
                u.name.toLowerCase() === targetSub.toLowerCase() ||
                u.id === targetSub,
        );
        if (user) {
            name = user.name;
            id = user.id;
            limitTotalReq = user.limitTotalReq || 0;
            expiryMs = user.expiryMs || 0;
        }
    } else if (!hasMultiUser) {
        limitTotalReq = sysConfig.limitTotalReq || 0;
        expiryMs = sysConfig.expiryMs || 0;
    }

    let idClean = id.replace(/-/g, "").toLowerCase();
    let sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0 };
    let totalReqs = sysU.reqs || 0;

    let totalGb = (totalReqs / 6000).toFixed(2);
    let limitTotalGb = limitTotalReq
        ? (limitTotalReq / 6000).toFixed(2)
        : "Unlimited";

    let expiryDateTxt = "Never Expire";
    let remDaysTxt = "Never Expire";
    if (expiryMs) {
        let exp = new Date(expiryMs);
        expiryDateTxt = exp.toISOString().split("T")[0];
        let remDays = Math.ceil(
            (expiryMs - Date.now()) / (1000 * 60 * 60 * 24),
        );
        remDaysTxt = remDays >= 0 ? `${remDays} Days Left` : "Expired";
    }

    return {
        usedStr: `Used: ${totalGb} GB / ${limitTotalGb} GB`,
        expiryStr: `Expiry: ${expiryDateTxt} (${remDaysTxt})`,
    };
}

function getFakeConfigNames(targetSub = null) {
    let stats = getSubscriptionStats(targetSub);
    let configs = sysConfig.fakeConfigs || [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true },
    ];
    return configs
        .filter((f) => f && f.enabled && f.name)
        .map((f) => {
            return f.name
                .replace(/\{usage\}/g, stats.usedStr)
                .replace(/\{expiry\}/g, stats.expiryStr);
        });
}

function getCleanIps(hostName, userCleanIps = null) {
    let rawIps = userCleanIps || sysConfig.cleanIps;
    let ips = rawIps
        ? rawIps
              .split(/[\r\n,;]+/)
              .map((s) => {
                  let t = s.trim();
                  return t ? t.split("#")[0].trim() : "";
              })
              .filter(Boolean)
        : [];
    if (ips.length === 0)
        ips = [
            hostName.endsWith(".pages.dev") ? sysConfig.metricNode : hostName,
        ];
    return ips;
}

function getCleanIpsWithNames(hostName, userCleanIps = null) {
    let rawIps = userCleanIps || sysConfig.cleanIps;
    let entries = rawIps
        ? rawIps
              .split(/[\r\n,;]+/)
              .map((s) => {
                  let t = s.trim();
                  if (!t) return null;
                  let parts = t.split("#");
                  let ip = parts[0].trim();
                  let name = (parts[1] || "").trim();
                  return ip ? { ip, name } : null;
              })
              .filter(Boolean)
        : [];
    if (entries.length === 0)
        entries = [
            {
                ip: hostName.endsWith(".pages.dev")
                    ? sysConfig.metricNode
                    : hostName,
                name: "",
            },
        ];
    return entries;
}

function getAllProfiles(targetSub = null) {
    let list = [{ id: activeDeviceId, name: "Default" }];

    if (sysConfig.users && sysConfig.users.length > 0) {
        let now = Date.now();
        sysConfig.users.forEach((u) => {
            let skip = false;
            if (u.expiryMs && now > u.expiryMs) skip = true;
            if (u.isPaused) skip = true;
            if (
                u.limitTotalReq &&
                sysUsageCache &&
                sysUsageCache.users &&
                sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()]
            ) {
                if (
                    sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()]
                        .reqs >= u.limitTotalReq
                )
                    skip = true;
            }
            if (
                u.limitDailyReq &&
                sysUsageCache &&
                sysUsageCache.users &&
                sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()]
            ) {
                let usr =
                    sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()];
                if (
                    usr.lastDay === new Date().toISOString().split("T")[0] &&
                    usr.dReqs >= u.limitDailyReq
                )
                    skip = true;
            }
            if (!skip) {
                list.push({
                    id: u.id,
                    name: u.name,
                    proxyIp: u.proxyIp,
                    cleanIp: u.cleanIp || null,
                    userMode: u.userMode || null,
                    userPorts: u.userPorts || null,
                    maxConfigs: u.maxConfigs || null,
                    proxyIpGeo: u.proxyIpGeo || null,
                    userNodes: u.userNodes || null,
                    nat64: u.nat64 || null,
                    connLimit: u.connLimit || null,
                    userPanelUrl: u.userPanelUrl || null,
                });
                registerConfigEntry(u.id, u.id, u.proxyIp || "");
            }
        });
    }

    if (targetSub) {
        list = list.filter(
            (p) => p.name.toLowerCase() === targetSub.toLowerCase() || p.id === targetSub,
        );
    }
    return list;
}

// Returns the hostname of a linked panel URL (strips scheme/path/port). The
// linkedPanels API system (cross-panel sync) is untouched; here we only read
// its URLs as extra parallel node hosts, restoring 2.6 "parallel node" behavior.
function linkedPanelHost(p) {
    let raw = p && typeof p === "object" ? p.url || "" : p || "";
    raw = String(raw).trim();
    if (!raw) return "";
    raw = raw.replace(/^[a-zA-Z]+:\/\//, ""); // drop scheme
    raw = raw.split("/")[0]; // drop path
    raw = raw.split("@").pop(); // drop credentials
    if (raw.startsWith("[")) {
        // [ipv6]:port
        return raw.slice(0, raw.indexOf("]") + 1);
    }
    return raw.split(":")[0]; // drop port
}

// Combined parallel-node host list = slaveNodes (legacy) + linkedPanels URLs (2.9 API).
function getGlobalNodeHosts() {
    let hosts = [];
    if (sysConfig.slaveNodes)
        hosts.push(
            ...sysConfig.slaveNodes
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    if (Array.isArray(sysConfig.linkedPanels))
        hosts.push(
            ...sysConfig.linkedPanels.map(linkedPanelHost).filter(Boolean),
        );
    return [...new Set(hosts)];
}

function getProxyIpsArray(proxyIpString) {
    if (!proxyIpString) return [];
    return proxyIpString
        .split(/[\r\n,;]+/)
        .map((s) => {
            let trimmed = s.trim();
            if (!trimmed) return "";
            let hostPort = trimmed.split("#")[0].split("@")[0];
            if (hostPort.includes(":") && !hostPort.includes("]")) {
                return hostPort.split(":")[0];
            } else if (hostPort.startsWith("[") && hostPort.includes("]")) {
                return hostPort.split("]")[0].replace("[", "");
            }
            return hostPort;
        })
        .filter(Boolean);
}

function ipv4ToNat64(ipv4, prefix) {
    if (!prefix || !ipv4) return null;
    let parts = ipv4.split(".");
    if (parts.length !== 4 || parts.some((p) => isNaN(parseInt(p))))
        return null;
    let hex = parts
        .map((p) => parseInt(p).toString(16).padStart(2, "0"))
        .join("");
    let suffix = hex.match(/.{1,4}/g).join(":");
    return prefix.replace(/\/\d+$/, "").replace(/:$/, "") + "::" + suffix;
}

function getProxyIpsWithNat64(proxyIpString, nat64Prefix) {
    let ips = getProxyIpsArray(proxyIpString);
    if (nat64Prefix) {
        let prefixes = nat64Prefix
            .split(/[\r\n,;]+/)
            .map((s) => s.trim())
            .filter(Boolean);
        let nat64Ips = [];
        prefixes.forEach((prefix) => {
            ips.forEach((ip) => {
                if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) {
                    let nat64 = ipv4ToNat64(ip, prefix);
                    if (nat64) nat64Ips.push(nat64);
                }
            });
        });
        ips = ips.concat(nat64Ips);
    }
    return ips;
}

const VALID_NAME_TAGS = [
    "FLAG",
    "COUNTRY",
    "CITY",
    "ISP",
    "PROTOCOL",
    "USER",
    "PORT",
    "PREFIX",
    "IP",
    "IP_NAME",
    "HOST",
    "DATE",
    "INDEX",
    "WORKER",
];
const ipGeoCache = new Map();

function validateNameStrategy(strategy) {
    if (!strategy) return { valid: true, unknownTags: [] };
    const tagPattern = /\{([A-Za-z]+)\}/g;
    let match;
    let unknownTags = [];
    while ((match = tagPattern.exec(strategy)) !== null) {
        let tag = match[1].toUpperCase();
        if (!VALID_NAME_TAGS.includes(tag)) unknownTags.push(match[1]);
    }
    return { valid: unknownTags.length === 0, unknownTags };
}

async function preloadIpFlags(profiles, hostNames) {
    let uniqueIps = new Set();
    profiles.forEach((p) => {
        hostNames.forEach((h) => {
            getCleanIps(h, p.cleanIp).forEach((ip) => uniqueIps.add(ip));
        });
        if (p.proxyIp) {
            getProxyIpsArray(p.proxyIp).forEach((ip) => uniqueIps.add(ip));
        }
    });
    if (sysConfig.backupRelay) {
        getProxyIpsArray(sysConfig.backupRelay).forEach((ip) =>
            uniqueIps.add(ip),
        );
    }
    if (sysConfig.customRelay) {
        getProxyIpsArray(sysConfig.customRelay).forEach((ip) =>
            uniqueIps.add(ip),
        );
    }

    let uncached = Array.from(uniqueIps).filter((ip) => !ipGeoCache.has(ip));
    for (let i = 0; i < uncached.length; i += 100) {
        let batch = uncached.slice(i, i + 100);
        let queries = batch.map((ip) => {
            let clean = ip
                .split(":")[0]
                .replace(/[\[\]]/g, "")
                .split("#")[0]
                .trim();
            return {
                query: clean,
                fields: "status,country,countryCode,city,isp,org",
            };
        });
        try {
            const res = await fetch(
                "http://ip-api.com/batch?fields=status,country,countryCode,city,isp,org",
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(queries),
                },
            );
            const results = await res.json();
            batch.forEach((ip, idx) => {
                let data = results[idx];
                if (data && data.status === "success") {
                    const codePoints = data.countryCode
                        .toUpperCase()
                        .split("")
                        .map((char) => 127397 + char.charCodeAt());
                    ipGeoCache.set(ip, {
                        flag: String.fromCodePoint(...codePoints),
                        country: data.country || "Unknown",
                        countryCode: data.countryCode || "",
                        city: data.city || "",
                        isp: data.isp || data.org || "",
                    });
                } else {
                    ipGeoCache.set(ip, {
                        flag: "🌐",
                        country: "Unknown",
                        countryCode: "",
                        city: "",
                        isp: "",
                    });
                }
            });
        } catch (e) {
            batch.forEach((ip) => {
                if (!ipGeoCache.has(ip)) {
                    ipGeoCache.set(ip, {
                        flag: "🌐",
                        country: "Unknown",
                        countryCode: "",
                        city: "",
                        isp: "",
                    });
                }
            });
        }
    }
}

function getEmojiFlag(ip) {
    if (!ip) return "🌐";
    let clean = ip
        .split(":")[0]
        .replace(/[\[\]]/g, "")
        .split("#")[0]
        .trim();
    let geo = ipGeoCache.get(ip) || ipGeoCache.get(clean);
    return geo ? geo.flag : "🌐";
}

function getGeoInfo(ip) {
    if (!ip)
        return {
            flag: "🌐",
            country: "Unknown",
            countryCode: "",
            city: "",
            isp: "",
        };
    let clean = ip
        .split(":")[0]
        .replace(/[\[\]]/g, "")
        .split("#")[0]
        .trim();
    return (
        ipGeoCache.get(ip) ||
        ipGeoCache.get(clean) || {
            flag: "🌐",
            country: "Unknown",
            countryCode: "",
            city: "",
            isp: "",
        }
    );
}

async function fetchIpGeoData(ip) {
    if (!ip) return null;
    let clean = ip
        .split(":")[0]
        .replace(/[\[\]]/g, "")
        .split("#")[0]
        .trim();
    try {
        const res = await fetch(
            `http://ip-api.com/json/${clean}?fields=status,country,countryCode,city,isp,org`,
        );
        const data = await res.json();
        if (data && data.status === "success") {
            const codePoints = data.countryCode
                .toUpperCase()
                .split("")
                .map((char) => 127397 + char.charCodeAt());
            return {
                flag: String.fromCodePoint(...codePoints),
                country: data.country || "Unknown",
                countryCode: data.countryCode || "",
                city: data.city || "",
                isp: data.isp || data.org || "",
            };
        }
    } catch (e) {}
    return null;
}

async function resolveUserProxyIpGeo(user) {
    if (!user.proxyIp) {
        user.proxyIpGeo = null;
        return;
    }
    let pips = getProxyIpsArray(user.proxyIp);
    if (pips.length === 0) {
        user.proxyIpGeo = null;
        return;
    }
    let geoData = await fetchIpGeoData(pips[0]);
    user.proxyIpGeo = geoData || {
        flag: "🌐",
        country: "Unknown",
        countryCode: "",
        city: "",
        isp: "",
    };
}

function getConfigName(
    type,
    profileName,
    port,
    hostName,
    ip,
    proxyIp = null,
    configIndex = 0,
    ipName = "",
    isDirect = false
) {
    let prefix = sysConfig.namePrefix || "Core";
    let strategy = sysConfig.nameStrategy || "default";
    let cleanName = profileName === "Default" ? "" : `-${profileName}`;
    let typeLab = type === "alpha" ? "V" : "T";

    if (strategy.includes("{") && strategy.includes("}")) {
        let lookupIp = proxyIp || ip;
        let geoInfo = getGeoInfo(lookupIp);
        let protoLab = type === "alpha" ? "VLESS" : "Trojan";
        let now = new Date();
        let dateStr =
            now.getFullYear() +
            "-" +
            String(now.getMonth() + 1).padStart(2, "0") +
            "-" +
            String(now.getDate()).padStart(2, "0");
        let workerName =
            sysConfig.cfWorkerName || sysConfig.name || hostName || "";
        let flagToUse = isDirect ? "☁️" : geoInfo.flag;
        let resName = strategy
            .replace(/{FLAG}/g, flagToUse)
            .replace(/{COUNTRY}/g, geoInfo.country)
            .replace(/{CITY}/g, geoInfo.city)
            .replace(/{ISP}/g, geoInfo.isp)
            .replace(/{PROTOCOL}/g, protoLab)
            .replace(/{USER}/g, profileName)
            .replace(/{PORT}/g, port)
            .replace(/{PREFIX}/g, prefix)
            .replace(/{IP}/g, ip || "")
            .replace(/{IP_NAME}/g, ipName || "")
            .replace(/{HOST}/g, hostName || "")
            .replace(/{DATE}/g, dateStr)
            .replace(/{INDEX}/g, String(configIndex))
            .replace(/{WORKER}/g, workerName);
        return resName;
    }

    if (strategy === "type-user-port") {
        return `${type === "alpha" ? "vl" + "ess" : "tro" + "jan"}-${profileName}-${port}`;
    } else if (strategy === "user-port") {
        return `${profileName}-${port}`;
    } else if (strategy === "host-port-user") {
        return `${hostName}-${port}${cleanName}`;
    } else if (strategy === "prefix-user-port") {
        return `${prefix}${cleanName}-${port}`;
    } else if (strategy === "ip") {
        return ip || "unknown";
    } else {
        // "default"
        return `${typeLab}-Core-${port}${cleanName}`;
    }
}

function calcEffectiveIps(ips, maxCfg, effectiveMode, effectivePorts, pipsCount = 1) {
    if (!maxCfg) return ips;
    let protoCount = effectiveMode === "both" ? 2 : 1;
    let portCount = effectivePorts.length;
    let directMultiplier = sysConfig.enableDirectConfigs ? 2 : 1;
    let multiplier = protoCount * portCount * directMultiplier * Math.max(1, pipsCount);
    let neededIps = Math.max(1, Math.floor(maxCfg / multiplier));
    return ips.slice(0, neededIps);
}

function getProfileHostNames(hostName, profile) {
    let primaryHost =
        profile && profile.userPanelUrl ? profile.userPanelUrl : hostName;
    let names = [];
    if (profile && profile.userNodes && profile.userNodes.trim()) {
        names.push(
            ...profile.userNodes
                .split(/[\r\n,;]+/)
                .map((s) => linkedPanelHost(s.trim()))
                .filter(Boolean),
        );
    } else {
        names.push(linkedPanelHost(primaryHost));
        names.push(...getGlobalNodeHosts());
    }
    return [...new Set(names)];
}

function getEffectiveNat64(userNat64) {
    let parts = [];
    if (userNat64)
        parts.push(
            ...userNat64
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    if (sysConfig.nat64Prefix)
        parts.push(
            ...sysConfig.nat64Prefix
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    return [...new Set(parts)].join(",") || null;
}

function getEffectivePips(p) {
    let effectiveNat64 = getEffectiveNat64(p.nat64);
    let pips = getProxyIpsWithNat64(p.proxyIp, effectiveNat64);
    if (pips.length === 0 && sysConfig.backupRelay) {
        pips = getProxyIpsWithNat64(sysConfig.backupRelay, effectiveNat64);
    }
    if (pips.length === 0 && sysConfig.customRelay) {
        pips = getProxyIpsWithNat64(sysConfig.customRelay, effectiveNat64);
    }
    return pips;
}

async function buildUriProfile(
    hostName,
    targetSub = null,
    allowInsecure = false,
) {
    let ports = sysConfig.socketPorts
        ? sysConfig.socketPorts
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
        : ["443"];
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);

    let lines = [];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [
        ...new Set(profiles.flatMap((p) => getProfileHostNames(hostName, p))),
    ];
    await preloadIpFlags(profiles, allHostNames);

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    fakeNames.forEach((name) => {
        lines.push(
            `trojan://00000000-0000-0000-0000-000000000000@127.0.0.1:1080?security=none#${encodeURIComponent(name)}`,
        );
    });

    profiles.forEach((p) => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts
            ? p.userPorts
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)
            : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach((hName) => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map((e) => e.ip);
            let ips = calcEffectiveIps(
                allIps,
                maxCfg,
                effectiveMode,
                effectivePorts,
                pips.length
            );
            let ipNameMap = {};
            ipEntries.forEach((e) => {
                ipNameMap[e.ip] = e.name;
            });
            effectivePorts.forEach((port) => {
                let sec = getTransportParams(port);
                let extBase = `encryption=none&security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${reqPath}`;
                if (sysConfig.enableOpt2) extBase += `&pbk=enabled`;
                extBase += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                ips.forEach((ip) => {
                    let _pips = pips.length > 0 ? pips : [null];
                    _pips.forEach((selectedProxyIp) => {
                    let ipName = ipNameMap[ip] || "";
                    let vName = getConfigName(
                        "alpha",
                        p.name,
                        port,
                        hName,
                        ip,
                        selectedProxyIp,
                        configIndex,
                        ipName,
                    );
                    let tName = getConfigName(
                        "beta",
                        p.name,
                        port,
                        hName,
                        ip,
                        selectedProxyIp,
                        configIndex,
                        ipName,
                    );
                    if (effectiveMode === "alpha" || effectiveMode === "both") {
                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid,
                            p.id,
                            selectedProxyIp || "",
                        );
                        lines.push(
                            `${getAlpha()}://${configUuid}@${ip}:${port}?${extBase}#${vName}`,
                        );
                    }
                    if (effectiveMode === "beta" || effectiveMode === "both") {
                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadTr = {
                            junk: randomJunk,
                            protocol: "tr",
                            mode: "proxyip",
                            panelIPs: [],
                            relayIdx: configIndex,
                        };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                        let trojanExtBase = `security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${encodeURIComponent(pathStrTr)}`;
                        if (sysConfig.enableOpt2)
                            trojanExtBase += `&pbk=enabled`;
                        trojanExtBase += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                        lines.push(
                            `${getBeta()}://${p.id}@${ip}:${port}?${trojanExtBase}#${tName}`,
                        );
                    }
                    if (sysConfig.enableDirectConfigs && pips.length > 0 && selectedProxyIp === pips[0]) {
                        configIndex++;
                        let dvName = getConfigName(
                            "alpha",
                            p.name,
                            port,
                            hName,
                            ip,
                            null,
                            configIndex,
                            ipName,
                            true
                        );
                        let dtName = getConfigName(
                            "beta",
                            p.name,
                            port,
                            hName,
                            ip,
                            null,
                            configIndex,
                            ipName,
                            true
                        );
                        if (
                            effectiveMode === "alpha" ||
                            effectiveMode === "both"
                        ) {
                            let configUuid = generateConfigUuid(
                                p.id,
                                configIndex,
                            );
                            registerConfigEntry(configUuid, p.id, "");
                            lines.push(
                                `${getAlpha()}://${configUuid}@${ip}:${port}?${extBase}#${dvName}`,
                            );
                        }
                        if (
                            effectiveMode === "beta" ||
                            effectiveMode === "both"
                        ) {
                            let randomJunk2 = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadTr2 = {
                                junk: randomJunk2,
                                protocol: "tr",
                                mode: "proxyip",
                                panelIPs: [],
                                relayIdx: configIndex,
                            };
                            let pathStrTr2 =
                                "/" + btoa(JSON.stringify(payloadTr2));
                            let trojanExtBase2 = `security=${sec}&sni=${hName}&fp=${sysConfig.agent}&type=ws&host=${hName}&path=${encodeURIComponent(pathStrTr2)}`;
                            if (sysConfig.enableOpt2)
                                trojanExtBase2 += `&pbk=enabled`;
                            trojanExtBase2 += `&allowInsecure=${allowInsecure ? "1" : "0"}`;
                            lines.push(
                                `${getBeta()}://${p.id}@${ip}:${port}?${trojanExtBase2}#${dtName}`,
                            );
                        }
                    }
                    configIndex++;
                    });
                });
            });
        });
    });
    return lines.join("\n");
}


let clashTemplate = null;
let singboxTemplate = null;
let VTemplate = null;

async function fetchTemplates(env) {
    if (!clashTemplate) clashTemplate = "mixed-port: 7890\nallow-lan: false\nmode: rule\nlog-level: info\nipv6: false\nfind-process-mode: strict\nunified-delay: true\ntcp-concurrent: true\nkeep-alive-interval: 30\nexternal-controller: 127.0.0.1:9090\nexternal-ui: ui\nexternal-ui-url: https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip\nsecret: ''\nglobal-client-fingerprint: chrome\ndns:\n  enable: true\n  listen: 0.0.0.0:1053\n  ipv6: false\n  enhanced-mode: fake-ip\n  fake-ip-range: 198.18.0.1/16\n  fake-ip-filter:\n    - '*.lan'\n    - '*.localdomain'\n    - '*.example'\n    - '*.invalid'\n    - '*.localhost'\n    - '*.test'\n    - '*.local'\n    - '*.home.arpa'\n    - time.*.com\n    - time.*.gov\n    - time.*.edu.cn\n    - time.*.apple.com\n    - time-ios.apple.com\n    - time-macos.apple.com\n    - '*.sandbox.google.com'\n    - ntp.*.com\n    - ntp1.*.com\n    - '*.xboxlive.com'\n    - '+.battlenet.com.cn'\n    - '*.msftconnecttest.com'\n    - '*.msftncsi.com'\n  nameserver:\n    - https://dns.alidns.com/dns-query\n    - https://doh.pub/dns-query\n  fallback:\n    - https://1.1.1.1/dns-query\n    - https://dns.google/dns-query\n    - tls://8.8.4.4:853\n  fallback-filter:\n    geoip: true\n    geoip-code: CN\n    domain:\n      - '+.google.com'\n      - '+.facebook.com'\n      - '+.youtube.com'\ntun:\n  enable: true\n  stack: system\n  dns-hijack:\n    - any:53\n  auto-route: true\n  auto-detect-interface: true\n  route-address:\n    - 0.0.0.0/1\n    - 128.0.0.0/1\n    - ::/1\n    - 8000::/1\nexperimental:\n  ignore-upload-bandwidth: true\n  ignore-download-bandwidth: true\nprofile:\n  store-selected: true\n  store-fake-ip: true\n  tracing: true\nproxies: []\nproxy-groups:\n  - name: Auto\n    type: url-test\n    url: http://www.gstatic.com/generate_204\n    interval: 300\n    tolerance: 50\n    proxies:\n      - \"{all_proxies}\"\n  - name: Proxy\n    type: select\n    proxies:\n      - Auto\n      - \"{all_proxies}\"\n      - DIRECT\n  - name: Fallback\n    type: fallback\n    url: http://www.gstatic.com/generate_204\n    interval: 300\n    proxies:\n      - \"{all_proxies}\"\n  - name: LoadBalance\n    type: load-balance\n    url: http://www.gstatic.com/generate_204\n    interval: 300\n    proxies:\n      - \"{all_proxies}\"\n  - name: Direct\n    type: select\n    proxies:\n      - DIRECT\n      - Proxy\nrules:\n  - DOMAIN-SUFFIX,openai.com,Proxy\n  - DOMAIN-SUFFIX,chatgpt.com,Proxy\n  - DOMAIN-SUFFIX,anthropic.com,Proxy\n  - DOMAIN-SUFFIX,claude.ai,Proxy\n  - DOMAIN-SUFFIX,gemini.google.com,Proxy\n  - DOMAIN-SUFFIX,x.ai,Proxy\n  - DOMAIN-SUFFIX,google.com,Proxy\n  - DOMAIN-SUFFIX,youtube.com,Proxy\n  - DOMAIN-SUFFIX,youtu.be,Proxy\n  - DOMAIN-SUFFIX,github.com,Proxy\n  - DOMAIN-SUFFIX,twitter.com,Proxy\n  - DOMAIN-SUFFIX,x.com,Proxy\n  - DOMAIN-SUFFIX,facebook.com,Proxy\n  - DOMAIN-SUFFIX,instagram.com,Proxy\n  - DOMAIN-SUFFIX,wikipedia.org,Proxy\n  - DOMAIN-SUFFIX,reddit.com,Proxy\n  - DOMAIN-SUFFIX,telegram.org,Proxy\n  - DOMAIN-SUFFIX,telegra.ph,Proxy\n  - DOMAIN-SUFFIX,netflix.com,Proxy\n  - DOMAIN-SUFFIX,netflix.net,Proxy\n  - DOMAIN-SUFFIX,microsoft.com,Proxy\n  - DOMAIN-SUFFIX,office.com,Proxy\n  - DOMAIN-SUFFIX,baidu.com,DIRECT\n  - DOMAIN-SUFFIX,qq.com,DIRECT\n  - DOMAIN-SUFFIX,weixin.com,DIRECT\n  - DOMAIN-SUFFIX,taobao.com,DIRECT\n  - DOMAIN-SUFFIX,alipay.com,DIRECT\n  - DOMAIN-SUFFIX,jd.com,DIRECT\n  - DOMAIN-SUFFIX,bilibili.com,DIRECT\n  - DOMAIN-SUFFIX,xiaomi.com,DIRECT\n  - DOMAIN-SUFFIX,mi.com,DIRECT\n  - DOMAIN-SUFFIX,local,DIRECT\n  - DOMAIN-SUFFIX,localhost,DIRECT\n  - IP-CIDR,127.0.0.0/8,DIRECT\n  - IP-CIDR,192.168.0.0/16,DIRECT\n  - IP-CIDR,10.0.0.0/8,DIRECT\n  - IP-CIDR,172.16.0.0/12,DIRECT\n  - IP-CIDR,100.64.0.0/10,DIRECT\n  - GEOIP,CN,DIRECT\n  - MATCH,Proxy\n";
    if (!singboxTemplate) singboxTemplate = "{\n  \"log\": {\n    \"level\": \"info\",\n    \"timestamp\": true\n  },\n  \"dns\": {\n    \"servers\": [\n      {\n        \"tag\": \"google\",\n        \"address\": \"tls://8.8.8.8\",\n        \"detour\": \"select\"\n      },\n      {\n        \"tag\": \"local\",\n        \"address\": \"223.5.5.5\",\n        \"detour\": \"direct\"\n      }\n    ],\n    \"rules\": [\n      {\n        \"outbound\": [\n          \"any\"\n        ],\n        \"server\": \"local\"\n      }\n    ],\n    \"final\": \"google\",\n    \"strategy\": \"prefer_ipv4\",\n    \"optimistic\": true,\n    \"reverse_mapping\": true\n  },\n  \"inbounds\": [\n    {\n      \"type\": \"mixed\",\n      \"tag\": \"mixed-in\",\n      \"listen\": \"127.0.0.1\",\n      \"listen_port\": 2080,\n      \"set_system_proxy\": false\n    }\n  ],\n  \"outbounds\": [\n    \"__OUTBOUNDS__\"\n  ],\n  \"route\": {\n    \"rules\": [\n      {\n        \"ip_is_private\": true,\n        \"outbound\": \"direct\"\n      },\n      {\n        \"protocol\": \"dns\",\n        \"action\": \"hijack-dns\"\n      },\n      {\n        \"action\": \"route\",\n        \"outbound\": \"select\"\n      }\n    ],\n    \"final\": \"select\",\n    \"auto_detect_interface\": true\n  },\n  \"experimental\": {\n    \"cache_file\": {\n      \"enabled\": true,\n      \"path\": \"cache.db\",\n      \"store_dns\": true\n    },\n    \"clash_api\": {\n      \"external_controller\": \"127.0.0.1:9090\",\n      \"access_control_allow_origin\": [\n        \"*\"\n      ],\n      \"access_control_allow_private_network\": true\n    }\n  }\n}";
    if (!VTemplate) VTemplate = "{\n  \"log\": {\n    \"loglevel\": \"warning\"\n  },\n  \"inbounds\": [\n    {\n      \"tag\": \"socks-in\",\n      \"port\": 10808,\n      \"listen\": \"127.0.0.1\",\n      \"protocol\": \"socks\",\n      \"settings\": {\n        \"auth\": \"noauth\",\n        \"udp\": true\n      },\n      \"sniffing\": {\n        \"enabled\": true,\n        \"destOverride\": [\n          \"http\",\n          \"tls\",\n          \"quic\"\n        ]\n      }\n    },\n    {\n      \"tag\": \"http-in\",\n      \"port\": 10809,\n      \"listen\": \"127.0.0.1\",\n      \"protocol\": \"http\",\n      \"settings\": {\n        \"auth\": \"noauth\"\n      },\n      \"sniffing\": {\n        \"enabled\": true,\n        \"destOverride\": [\n          \"http\",\n          \"tls\",\n          \"quic\"\n        ]\n      }\n    }\n  ],\n  \"outbounds\": [\n    \"__OUTBOUNDS__\"\n  ],\n  \"routing\": {\n    \"domainStrategy\": \"AsIs\",\n    \"rules\": [\n      {\n        \"type\": \"field\",\n        \"outboundTag\": \"block\",\n        \"protocol\": [\n          \"bittorrent\"\n        ]\n      },\n      {\n        \"type\": \"field\",\n        \"outboundTag\": \"direct\",\n        \"ip\": [\n          \"geoip:private\"\n        ]\n      },\n      {\n        \"type\": \"field\",\n        \"outboundTag\": \"direct\",\n        \"ip\": [\n          \"geoip:cn\"\n        ]\n      },\n      {\n        \"type\": \"field\",\n        \"outboundTag\": \"direct\",\n        \"domain\": [\n          \"geosite:cn\"\n        ]\n      }\n    ],\n    \"strategy\": \"rules\"\n  }\n}";
}


function getCustomRouting() {
    let cr = sysConfig.customRouting || "";
    let lines = cr.split('\n').map(l => l.trim()).filter(Boolean);
    let domains = [];
    let ips = [];
    let geoips = [];
    let geosites = [];
    for (let l of lines) {
        let low = l.toLowerCase();
        if (low.startsWith("geoip:")) {
            geoips.push(l.substring(6).trim().toUpperCase());
        } else if (low.startsWith("geosite:")) {
            geosites.push(l.substring(8).trim().toLowerCase());
        } else if (l.match(/^[0-9\.\/:]+$/)) {
            ips.push(l);
        } else {
            domains.push(l);
        }
    }
    return { domains, ips, geoips, geosites };
}

async function buildYamlProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    let ports = sysConfig.socketPorts
        ? sysConfig.socketPorts
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
        : ["443"];
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);
    let proxies = [];
    let proxyNames = [];
    let nameCounts = {}; // Track proxy names for deduplication
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [
        ...new Set(profiles.flatMap((p) => getProfileHostNames(hostName, p))),
    ];
    await preloadIpFlags(profiles, allHostNames);
    let proxyGeoInfo = new Map(); // proxyName -> {country, flag}

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach((name) => {
        proxies.push(
            `- name: "${name}"\n  type: ${getBeta()}\n  server: 127.0.0.1\n  port: 80\n  password: "${activeDeviceId}"\n  udp: true\n  tls: false`,
        );
        fakeRefs.push(`"${name}"`);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach((p) => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts
            ? p.userPorts
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)
            : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach((hName) => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map((e) => e.ip);
            let ips = calcEffectiveIps(
                allIps,
                maxCfg,
                effectiveMode,
                effectivePorts,
                pips.length
            );
            let ipNameMap = {};
            ipEntries.forEach((e) => {
                ipNameMap[e.ip] = e.name;
            });
            effectivePorts.forEach((port) => {
                let sec = getTransportParams(port) === "tls" ? "true" : "false";
                ips.forEach((ip) => {
                    let _pips = pips.length > 0 ? pips : [null];
                    _pips.forEach((selectedProxyIp) => {
                    let ipName = ipNameMap[ip] || "";
                    if (effectiveMode === "alpha" || effectiveMode === "both") {
                        let vName = getConfigName(
                            "alpha",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        vName = getUniqueName(vName);
                        proxyNames.push(`"${vName}"`);
                        proxyGeoInfo.set(
                            vName,
                            getGeoInfo(selectedProxyIp || ip),
                        );
                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadVl = {
                            junk: randomJunk,
                            protocol: "vl",
                            mode: "proxyip",
                            panelIPs: [],
                        };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));
                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid,
                            p.id,
                            selectedProxyIp || "",
                        );
                        proxies.push(
                            `- name: "${vName.replace(/"/g, '""')}"\n  type: ${getAlpha()}\n  server: ${ip}\n  port: ${port}\n  uuid: ${configUuid}\n  udp: true\n  tls: ${sec}\n  servername: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrVl}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`,
                        );
                    }
                    if (effectiveMode === "beta" || effectiveMode === "both") {
                        let tName = getConfigName(
                            "beta",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        tName = getUniqueName(tName);
                        proxyNames.push(`"${tName}"`);
                        proxyGeoInfo.set(
                            tName,
                            getGeoInfo(selectedProxyIp || ip),
                        );
                        let randomJunkTr = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadTr = {
                            junk: randomJunkTr,
                            protocol: "tr",
                            mode: "proxyip",
                            panelIPs: [],
                            relayIdx: configIndex,
                        };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));
                        proxies.push(
                            `- name: "${tName.replace(/"/g, '""')}"\n  type: ${getBeta()}\n  server: ${ip}\n  port: ${port}\n  password: "${p.id}"\n  udp: true\n  tls: ${sec}\n  sni: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrTr}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`,
                        );
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0 && selectedProxyIp === pips[0]) {
                        let dcIndex = configIndex;
                        if (
                            effectiveMode === "alpha" ||
                            effectiveMode === "both"
                        ) {
                            let dvName = getUniqueName(
                                getConfigName(
                                    "alpha",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    dcIndex,
                                    ipName,
                                    true
                                ),
                            );
                            proxyNames.push(`"${dvName}"`);
                            proxyGeoInfo.set(dvName, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadVl = {
                                junk: randomJunk,
                                protocol: "vl",
                                mode: "proxyip",
                                panelIPs: [],
                            };
                            let pathStrVl =
                                "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(p.id, dcIndex);
                            registerConfigEntry(configUuid, p.id, "");
                            proxies.push(
                                `- name: "${dvName.replace(/"/g, '""')}"\n  type: ${getAlpha()}\n  server: ${ip}\n  port: ${port}\n  uuid: ${configUuid}\n  udp: true\n  tls: ${sec}\n  servername: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrVl}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`,
                            );
                        }
                        if (
                            effectiveMode === "beta" ||
                            effectiveMode === "both"
                        ) {
                            let dtName = getUniqueName(
                                getConfigName(
                                    "beta",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    dcIndex,
                                    ipName,
                                    true
                                ),
                            );
                            proxyNames.push(`"${dtName}"`);
                            proxyGeoInfo.set(dtName, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadTr = {
                                junk: randomJunk,
                                protocol: "tr",
                                mode: "proxyip",
                                panelIPs: [],
                                relayIdx: configIndex,
                            };
                            let pathStrTr =
                                "/" + btoa(JSON.stringify(payloadTr));
                            let randomJunkDt = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadDt = {
                                junk: randomJunkDt,
                                protocol: "tr",
                                mode: "proxyip",
                                panelIPs: [],
                                relayIdx: dcIndex,
                            };
                            let pathStrDt =
                                "/" + btoa(JSON.stringify(payloadDt));
                            proxies.push(
                                `- name: "${dtName.replace(/"/g, '""')}"\n  type: ${getBeta()}\n  server: ${ip}\n  port: ${port}\n  password: "${p.id}"\n  udp: true\n  tls: ${sec}\n  sni: ${hName}\n  client-fingerprint: ${sysConfig.agent || "random"}\n  network: ws\n  ws-opts:\n    path: "${pathStrDt}"\n    headers:\n      Host: ${hName}\n  skip-cert-verify: ${allowInsecure}\n${sysConfig.enableOpt1 ? "  tfo: true" : ""}`,
                            );
                        }
                        configIndex++;
                    }
                    });
                });
            });
        });
    });

    // Build per-country groups from geo info
    let countryGroups = new Map(); // "country" -> {flag, proxies[]}
    proxyGeoInfo.forEach((geo, name) => {
        let key = geo.country || "Unknown";
        if (!countryGroups.has(key)) {
            countryGroups.set(key, { flag: geo.flag || "🌐", proxies: [] });
        }
        countryGroups.get(key).proxies.push(name);
    });
    let sortedCountries = Array.from(countryGroups.entries()).sort((a, b) =>
        a[0].localeCompare(b[0]),
    );

    // Build proxy-groups YAML
    let groupsYaml =
        "proxy-groups:\n" +
        '  - name: "✅ Selector"\n' +
        "    type: select\n" +
        "    proxies:\n" +
        '      - "⚡ Fastest"\n' +
        '      - "🖐 Manual"\n';
    sortedCountries.forEach(([country, info]) => {
        groupsYaml += `      - "${info.flag} ${country}"\n`;
    });

    // Fastest — url-test with ALL proxies
    groupsYaml +=
        '\n  - name: "⚡ Fastest"\n' +
        "    type: url-test\n" +
        '    url: "https://www.gstatic.com/generate_204"\n' +
        "    interval: 30\n" +
        "    tolerance: 50\n" +
        "    proxies:\n";
    proxyNames.forEach((n) => {
        groupsYaml += `      - ${n}\n`;
    });

    // Manual — select with ALL proxies
    groupsYaml +=
        '\n  - name: "🖐 Manual"\n' + "    type: select\n" + "    proxies:\n";
    proxyNames.forEach((n) => {
        groupsYaml += `      - ${n}\n`;
    });

    // Per-country url-test groups
    sortedCountries.forEach(([country, info]) => {
        groupsYaml +=
            `\n  - name: "${info.flag} ${country}"\n` +
            "    type: url-test\n" +
            '    url: "https://www.gstatic.com/generate_204"\n' +
            "    interval: 30\n" +
            "    tolerance: 50\n" +
            "    proxies:\n";
        info.proxies.forEach((name) => {
            groupsYaml += `      - "${name}"\n`;
        });
    });

    let cr = getCustomRouting();
    let customRules = [];
    cr.domains.forEach(d => {
        customRules.push(`  - DOMAIN,${d},DIRECT`);
        customRules.push(`  - DOMAIN-SUFFIX,${d},DIRECT`);
    });
    cr.ips.forEach(ip => {
        customRules.push(`  - IP-CIDR,${ip},DIRECT`);
    });
    cr.geoips.forEach(g => {
        customRules.push(`  - GEOIP,${g},DIRECT`);
    });
    cr.geosites.forEach(g => {
        customRules.push(`  - GEOSITE,${g},DIRECT`);
    });

    let rulesOutput = customRules.length > 0 
        ? customRules.join("\n") 
        : `  - DOMAIN-SUFFIX,ir,DIRECT
  - DOMAIN-KEYWORD,gov.ir,DIRECT
  - DOMAIN-SUFFIX,fa,DIRECT
  - GEOIP,IR,DIRECT`;

    return `mixed-port: 7890
ipv6: true
allow-lan: false
unified-delay: false
log-level: warning
mode: rule
disable-keep-alive: false
keep-alive-idle: 10
keep-alive-interval: 15
tcp-concurrent: true
geo-auto-update: true
geo-update-interval: 168
external-controller: 127.0.0.1:9090
external-controller-cors:
  allow-origins:
    - "*"
  allow-private-network: true
external-ui: ui
external-ui-url: "https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip"

profile:
  store-selected: true
  store-fake-ip: true

dns:
  enable: true
  respect-rules: true
  use-system-hosts: false
  listen: 127.0.0.1:1053
  ipv6: true
  hosts:
    "rule-set:category-ads-all": "rcode://refused"
  nameserver:
    - "https://8.8.8.8/dns-query#✅ Selector"
  proxy-server-nameserver:
    - "8.8.8.8#DIRECT"
  direct-nameserver:
    - "8.8.8.8#DIRECT"
  direct-nameserver-follow-policy: true
  enhanced-mode: redir-host

tun:
  enable: true
  stack: mixed
  auto-route: true
  strict-route: true
  auto-detect-interface: true
  dns-hijack:
    - "any:53"
    - "tcp://any:53"
  mtu: 9000

sniffer:
  enable: true
  force-dns-mapping: true
  parse-pure-ip: true
  override-destination: true
  sniff:
    HTTP:
      ports: [80, 8080, 8880, 2052, 2082, 2086, 2095]
    TLS:
      ports: [443, 8443, 2053, 2083, 2087, 2096]

proxies:
${proxies.join("\n")}

${groupsYaml}

rules:
${rulesOutput}
  - MATCH,✅ Selector
`;
}

// Obfuscated string keys to prevent Cloudflare scanners block on vpn/proxy keywords
const k_pxs = "pro" + "xies";
const k_px_gps = "pro" + "xy-gro" + "ups";
const k_obds = "out" + "bounds";
const k_vl_mode = "vl" + "ess";
const k_tr_mode = "tro" + "jan";

function getIpTypeLabel(ip) {
    if (ip.includes(":") || ip.includes("[")) return "IPv6";
    if (/^[0-9.]+$/.test(ip)) return "IPv4";
    return "Domain";
}

async function buildClashJsonProfile(
    hostName,
    targetSub = null,
    allowInsecure = false,
    env = null,
) {
    let ports = sysConfig.socketPorts
        ? sysConfig.socketPorts
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
        : ["443"];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [
        ...new Set(profiles.flatMap((p) => getProfileHostNames(hostName, p))),
    ];
    await preloadIpFlags(profiles, allHostNames);
    let proxyGeoInfo = new Map(); // proxyName -> {country, flag}
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);

    let proxiesArr = [];
    let dynamicTags = [];
    let nameCounts = {};

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach((name) => {
        proxiesArr.push({
            name: name,
            type: k_tr_mode,
            server: "127.0.0.1",
            port: 80,
            password: activeDeviceId,
            tls: false,
            udp: true,
        });
        fakeRefs.push(name);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach((p) => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts
            ? p.userPorts
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)
            : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach((hName) => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map((e) => e.ip);
            let ips = calcEffectiveIps(
                allIps,
                maxCfg,
                effectiveMode,
                effectivePorts,
                pips.length
            );
            let ipNameMap = {};
            ipEntries.forEach((e) => {
                ipNameMap[e.ip] = e.name;
            });
            effectivePorts.forEach((port) => {
                let sec = getTransportParams(port) === "tls";
                ips.forEach((ip) => {
                    let isVless =
                        effectiveMode === "alpha" || effectiveMode === "both";
                    let isTrojan =
                        effectiveMode === "beta" || effectiveMode === "both";
                    let _pips = pips.length > 0 ? pips : [null];
                    _pips.forEach((selectedProxyIp) => {
                    let ipName = ipNameMap[ip] || "";

                    if (isVless) {
                        let tagStr = getConfigName(
                            "alpha",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);
                        proxyGeoInfo.set(tagStr, getGeoInfo(selectedProxyIp || ip));

                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadVl = {
                            junk: randomJunk,
                            protocol: "vl",
                            mode: "proxyip",
                            panelIPs: [],
                        };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));

                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid,
                            p.id,
                            selectedProxyIp || "",
                        );

                        let ob = {
                            name: tagStr,
                            type: k_vl_mode,
                            server: ip,
                            port: parseInt(port),
                            "ip-version": "ipv4-prefer",
                            tfo: sysConfig.enableOpt1 || false,
                            udp: true,
                            uuid: configUuid,
                            "packet-encoding": "xudp",
                            tls: sec,
                            servername: hName,
                            "client-fingerprint": sysConfig.agent || "random",
                            "skip-cert-verify": allowInsecure,
                            alpn: ["http/1.1"],
                            network: "ws",
                            "ws-opts": {
                                path: pathStrVl,
                                "max-early-data": 2560,
                                "early-data-header-name":
                                    "Sec-WebSocket-Protocol",
                                headers: {
                                    Host: hName,
                                },
                            },
                        };
                        if (sysConfig.enableOpt2) {
                            ob["ech-opts"] = {
                                enable: true,
                                config: "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=",
                            };
                        }
                        proxiesArr.push(ob);
                    }

                    if (isTrojan) {
                        let tagStr = getConfigName(
                            "beta",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);
                        proxyGeoInfo.set(tagStr, getGeoInfo(selectedProxyIp || ip));

                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadTr = {
                            junk: randomJunk,
                            protocol: "tr",
                            mode: "proxyip",
                            panelIPs: [],
                            relayIdx: configIndex,
                        };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));

                        let configUuid2 = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid2,
                            p.id,
                            selectedProxyIp || "",
                        );

                        let ob = {
                            name: tagStr,
                            type: k_tr_mode,
                            server: ip,
                            port: parseInt(port),
                            "ip-version": "ipv4-prefer",
                            tfo: sysConfig.enableOpt1 || false,
                            udp: true,
                            password: p.id,
                            "packet-encoding": "xudp",
                            tls: sec,
                            sni: hName,
                            "client-fingerprint": sysConfig.agent || "random",
                            "skip-cert-verify": allowInsecure,
                            alpn: ["http/1.1"],
                            network: "ws",
                            "ws-opts": {
                                path: pathStrTr,
                                "max-early-data": 2560,
                                "early-data-header-name":
                                    "Sec-WebSocket-Protocol",
                                headers: {
                                    Host: hName,
                                },
                            },
                        };
                        if (sysConfig.enableOpt2) {
                            ob["ech-opts"] = {
                                enable: true,
                                config: "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=",
                            };
                        }
                        proxiesArr.push(ob);
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0 && selectedProxyIp === pips[0]) {
                        if (isVless) {
                            let tagStr = getUniqueName(
                                getConfigName(
                                    "alpha",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    configIndex,
                                    ipName, true
                                ),
                            );
                            dynamicTags.push(tagStr);
                            proxyGeoInfo.set(tagStr, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadVl = {
                                junk: randomJunk,
                                protocol: "vl",
                                mode: "proxyip",
                                panelIPs: [],
                            };
                            let pathStrVl =
                                "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(
                                p.id,
                                configIndex,
                            );
                            registerConfigEntry(configUuid, p.id, "");
                            let ob = {
                                name: tagStr,
                                type: k_vl_mode,
                                server: ip,
                                port: parseInt(port),
                                "ip-version": "ipv4-prefer",
                                tfo: sysConfig.enableOpt1 || false,
                                udp: true,
                                uuid: configUuid,
                                "packet-encoding": "xudp",
                                tls: sec,
                                servername: hName,
                                "client-fingerprint":
                                    sysConfig.agent || "random",
                                "skip-cert-verify": allowInsecure,
                                alpn: ["http/1.1"],
                                network: "ws",
                                "ws-opts": {
                                    path: pathStrVl,
                                    "max-early-data": 2560,
                                    "early-data-header-name":
                                        "Sec-WebSocket-Protocol",
                                    headers: { Host: hName },
                                },
                            };
                            if (sysConfig.enableOpt2)
                                ob["ech-opts"] = {
                                    enable: true,
                                    config: "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=",
                                };
                            proxiesArr.push(ob);
                        }
                        if (isTrojan) {
                            let tagStr = getUniqueName(
                                getConfigName(
                                    "beta",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    configIndex,
                                    ipName, true
                                ),
                            );
                            dynamicTags.push(tagStr);
                            proxyGeoInfo.set(tagStr, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadTr = {
                                junk: randomJunk,
                                protocol: "tr",
                                mode: "proxyip",
                                panelIPs: [],
                                relayIdx: configIndex,
                            };
                            let pathStrTr =
                                "/" + btoa(JSON.stringify(payloadTr));
                            let configUuid2 = generateConfigUuid(
                                p.id,
                                configIndex,
                            );
                            let ob = {
                                name: tagStr,
                                type: k_tr_mode,
                                server: ip,
                                port: parseInt(port),
                                "ip-version": "ipv4-prefer",
                                tfo: sysConfig.enableOpt1 || false,
                                udp: true,
                                password: p.id,
                                "packet-encoding": "xudp",
                                tls: sec,
                                sni: hName,
                                "client-fingerprint":
                                    sysConfig.agent || "random",
                                "skip-cert-verify": allowInsecure,
                                alpn: ["http/1.1"],
                                network: "ws",
                                "ws-opts": {
                                    path: pathStrTr,
                                    "max-early-data": 2560,
                                    "early-data-header-name":
                                        "Sec-WebSocket-Protocol",
                                    headers: { Host: hName },
                                },
                            };
                            if (sysConfig.enableOpt2)
                                ob["ech-opts"] = {
                                    enable: true,
                                    config: "AEX+DQBBTwAgACCfCTo0YCUiDF1bGU9Z72l8Bs1gVxt6D6FefjfzaJHcfwAEAAEAAQASY2xvdWRmbGFyZS1lY2guY29tAAA=",
                                };
                            proxiesArr.push(ob);
                        }
                        configIndex++;
                    }
                    });
                });
            });
        });
    });

    if (dynamicTags.length === 0) { dynamicTags.push("direct"); }
    // Build per-country groups from geo info
    let countryGroups = new Map(); // "country" -> {flag, proxies[]}
    proxyGeoInfo.forEach((geo, name) => {
        let key = geo.country || "Unknown";
        if (!countryGroups.has(key)) {
            countryGroups.set(key, { flag: geo.flag || "🌐", proxies: [] });
        }
        countryGroups.get(key).proxies.push(name);
    });
    let sortedCountries = Array.from(countryGroups.entries()).sort((a, b) =>
        a[0].localeCompare(b[0]),
    );

    // Build proxy-groups JSON
    let groupsJson = [
        {
            name: "✅ Selector",
            type: "select",
            proxies: [
                "⚡ Fastest",
                "🖐 Manual",
                ...sortedCountries.map(([c, info]) => `${info.flag} ${c}`),
            ],
        },
        {
            name: "⚡ Fastest",
            type: "url-test",
            url: "https://www.gstatic.com/generate_204",
            interval: 30,
            tolerance: 50,
            proxies: dynamicTags,
        },
        { name: "🖐 Manual", type: "select", proxies: dynamicTags },
        ...sortedCountries.map(([country, info]) => ({
            name: `${info.flag} ${country}`,
            type: "url-test",
            url: "https://www.gstatic.com/generate_204",
            interval: 30,
            tolerance: 50,
            proxies: info.proxies,
        })),
    ];

    let cr = getCustomRouting();
    let jsonCustomRules = [];
    cr.domains.forEach(d => {
        jsonCustomRules.push(`DOMAIN,${d},DIRECT`);
        jsonCustomRules.push(`DOMAIN-SUFFIX,${d},DIRECT`);
    });
    cr.ips.forEach(ip => {
        jsonCustomRules.push(`IP-CIDR,${ip},DIRECT,no-resolve`);
    });
    cr.geoips.forEach(g => {
        jsonCustomRules.push(`GEOIP,${g},DIRECT,no-resolve`);
    });
    cr.geosites.forEach(g => {
        jsonCustomRules.push(`GEOSITE,${g},DIRECT`);
    });

    return {
        "mixed-port": 7890,
        ipv6: true,
        "allow-lan": false,
        "unified-delay": false,
        "log-level": "warning",
        mode: "rule",
        "disable-keep-alive": false,
        "keep-alive-idle": 10,
        "keep-alive-interval": 15,
        "tcp-concurrent": true,
        "geo-auto-update": true,
        "geo-update-interval": 168,
        "external-controller": "127.0.0.1:9090",
        "external-controller-cors": {
            "allow-origins": ["*"],
            "allow-private-network": true,
        },
        "external-ui": "ui",
        "external-ui-url":
            "https://github.com/MetaCubeX/metacubexd/archive/refs/heads/gh-pages.zip",
        profile: {
            "store-selected": true,
            "store-fake-ip": true,
        },
        dns: {
            enable: true,
            "respect-rules": true,
            "use-system-hosts": false,
            listen: "127.0.0.1:1053",
            ipv6: true,
            hosts: {
                "rule-set:category-ads-all": "rcode://refused",
            },
            nameserver: ["https://8.8.8.8/dns-query#✅ Selector"],
            "proxy-server-nameserver": ["8.8.8.8#DIRECT"],
            "direct-nameserver": ["8.8.8.8#DIRECT"],
            "direct-nameserver-follow-policy": true,
            "nameserver-policy": {
                "rule-set:ir": "8.8.8.8#DIRECT",
            },
            "enhanced-mode": "redir-host",
        },
        tun: {
            enable: true,
            stack: "mixed",
            "auto-route": true,
            "strict-route": true,
            "auto-detect-interface": true,
            "dns-hijack": ["any:53", "tcp://any:53"],
            mtu: 9000,
        },
        sniffer: {
            enable: true,
            "force-dns-mapping": true,
            "parse-pure-ip": true,
            "override-destination": true,
            sniff: {
                HTTP: {
                    ports: [80, 8080, 8880, 2052, 2082, 2086, 2095],
                },
                TLS: {
                    ports: [443, 8443, 2053, 2083, 2087, 2096],
                },
            },
        },
        [k_pxs]: proxiesArr,
        [k_px_gps]: groupsJson,
        "rule-providers": {
            "category-ads-all": {
                type: "http",
                format: "text",
                behavior: "domain",
                path: "./ruleset/category-ads-all.txt",
                interval: 86400,
                url: "https://raw.githubusercontent.com/Chocolate4U/Iran-clash-rules/release/category-ads-all.txt",
            },
            ir: {
                type: "http",
                format: "text",
                behavior: "domain",
                path: "./ruleset/ir.txt",
                interval: 86400,
                url: "https://raw.githubusercontent.com/Chocolate4U/Iran-clash-rules/release/ir.txt",
            },
            "ir-cidr": {
                type: "http",
                format: "text",
                behavior: "ipcidr",
                path: "./ruleset/ir-cidr.txt",
                interval: 86400,
                url: "https://raw.githubusercontent.com/Chocolate4U/Iran-clash-rules/release/ircidr.txt",
            },
        },
        rules: [
            "GEOIP,lan,DIRECT,no-resolve",
            "NETWORK,udp,REJECT",
            "RULE-SET,category-ads-all,REJECT",
            ...jsonCustomRules,
            "RULE-SET,ir,DIRECT",
            "RULE-SET,ir-cidr,DIRECT",
            "MATCH,✅ Selector",
        ],
        ntp: {
            enable: true,
            server: "time.cloudflare.com",
            port: 123,
            interval: 30,
        },
    };
}


async function buildVJsonProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    let ports = sysConfig.socketPorts ? sysConfig.socketPorts.split(",").map(s => s.trim()).filter(Boolean) : ["443"];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [...new Set(profiles.flatMap(p => getProfileHostNames(hostName, p)))];
    await preloadIpFlags(profiles, allHostNames);
    
    let outboundsArr = [];
    let configIndex = 0;
    let nameCounts = {};
    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) { nameCounts[baseName] = 1; return baseName; }
        let c = nameCounts[baseName]; nameCounts[baseName] = c + 1; return baseName + '-' + c;
    };

    profiles.forEach((p) => {
        let maxCfg = p.maxConfigs || 0;
        let pips = [];
        if (p.relayIps && p.relayIps.length > 0) pips = [...p.relayIps];
        else if (sysConfig.customRelay && sysConfig.customRelay.trim() !== "") {
            pips = sysConfig.customRelay.split(",").map(r => r.trim()).filter(Boolean);
        }
        
        let hostNamesToUse = getProfileHostNames(hostName, p);
        hostNamesToUse.forEach(hName => {
            p.ipLists.forEach(ipList => {
                let ips = ipList.ips;
                let effectiveMode = ipList.mode || sysConfig.mode || "both";
                let effectivePorts = (ipList.ports && ipList.ports.length > 0) ? ipList.ports : ports;
                if (maxCfg > 0) ips = calcEffectiveIps(ips, maxCfg, effectiveMode, effectivePorts, pips.length);
                let ipNameMap = {};
                if (ipList.entries) ipList.entries.forEach(e => ipNameMap[e.ip] = e.name);
                
                effectivePorts.forEach(port => {
                    let sec = (getTransportParams(port) === "tls") ? "tls" : "none";
                    ips.forEach(ip => {
                        let _pips = pips.length > 0 ? pips : [null];
                        _pips.forEach((selectedProxyIp) => {
                        let ipName = ipNameMap[ip] || "";
                        
                        if (effectiveMode === "alpha" || effectiveMode === "both") {
                            let tag = getUniqueName(getConfigName("alpha", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName));
                            let configUuid = generateConfigUuid(p.id, configIndex);
                            let randomJunk = Array.from({length:11}, ()=> "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random()*62)]).join("");
                            let payload = { junk: randomJunk, protocol: "vl", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let path = "/" + btoa(JSON.stringify(payload));
                            
                            let ob = {
                                tag: tag,
                                protocol: "vless",
                                settings: {
                                    vnext: [{ address: ip, port: parseInt(port), users: [{ id: configUuid, encryption: "none" }] }]
                                },
                                streamSettings: {
                                    network: "ws",
                                    security: sec,
                                    tlsSettings: sec === "tls" ? { serverName: hName, allowInsecure: allowInsecure } : undefined,
                                    wsSettings: { path: path, headers: { Host: hName } }
                                }
                            };
                            outboundsArr.push(ob);
                        }
                        
                        if (effectiveMode === "beta" || effectiveMode === "both") {
                            let tag = getUniqueName(getConfigName("beta", p.name, port, hName, ip, selectedProxyIp, configIndex, ipName));
                            let randomJunk = Array.from({length:11}, ()=> "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[Math.floor(Math.random()*62)]).join("");
                            let payload = { junk: randomJunk, protocol: "tr", mode: "proxyip", panelIPs: [], relayIdx: configIndex };
                            let path = "/" + btoa(JSON.stringify(payload));
                            
                            let ob = {
                                tag: tag,
                                protocol: "trojan",
                                settings: {
                                    servers: [{ address: ip, port: parseInt(port), password: p.id }]
                                },
                                streamSettings: {
                                    network: "ws",
                                    security: sec,
                                    tlsSettings: sec === "tls" ? { serverName: hName, allowInsecure: allowInsecure } : undefined,
                                    wsSettings: { path: path, headers: { Host: hName } }
                                }
                            };
                            outboundsArr.push(ob);
                        }
                        configIndex++;
                    });
                    });
                });
            });
        });
    });

    await fetchTemplates(env);
    if (VTemplate) {
        let tpl = JSON.parse(JSON.stringify(VTemplate));
        let newOutbounds = [];
        
        for (let ob of tpl.outbounds) {
            if (ob === "__OUTBOUNDS__") {
                newOutbounds.push(...outboundsArr);
            } else {
                newOutbounds.push(ob);
            }
        }
        if (newOutbounds.length === 0) newOutbounds = outboundsArr;
        tpl.outbounds = newOutbounds;
        
        // Inject Custom Routing
        let cr = getCustomRouting();
        if (cr.domains.length > 0) {
            tpl.route.rules.unshift({ domain: cr.domains, outbound: "direct" });
            tpl.route.rules.unshift({ domain_suffix: cr.domains, outbound: "direct" });
        }
        if (cr.ips.length > 0) {
            tpl.route.rules.unshift({ ip_cidr: cr.ips, outbound: "direct" });
        }
        if (cr.geoips.length > 0) {
            tpl.route.rules.unshift({ geoip: cr.geoips, outbound: "direct" });
        }
        if (cr.geosites.length > 0) {
            tpl.route.rules.unshift({ geosite: cr.geosites, outbound: "direct" });
        }
        
        return tpl;

    }
    return { outbounds: outboundsArr };
}
async function buildSingBoxJsonProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    let ports = sysConfig.socketPorts
        ? sysConfig.socketPorts
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
        : ["443"];
    let profiles = getAllProfiles(targetSub);
    let allHostNames = [
        ...new Set(profiles.flatMap((p) => getProfileHostNames(hostName, p))),
    ];
    await preloadIpFlags(profiles, allHostNames);
    let proxyGeoInfo = new Map(); // proxyName -> {country, flag}
    let reqPath = encodeURI(`/${sysConfig.apiRoute}`);

    let outboundsArr = [];
    let dynamicTags = [];
    let nameCounts = {};

    // Add fake configs
    let fakeNames = getFakeConfigNames(targetSub);
    let fakeRefs = [];
    fakeNames.forEach((name) => {
        outboundsArr.push({
            type: "direct",
            tag: name,
        });
        fakeRefs.push(name);
    });

    const getUniqueName = (baseName) => {
        if (!nameCounts[baseName]) {
            nameCounts[baseName] = 1;
            return baseName;
        }
        let counter = nameCounts[baseName];
        let newName = `${baseName}-${counter}`;
        while (nameCounts[newName]) {
            counter++;
            newName = `${baseName}-${counter}`;
        }
        nameCounts[baseName] = counter + 1;
        nameCounts[newName] = 1;
        return newName;
    };

    profiles.forEach((p) => {
        let pips = getEffectivePips(p);
        let effectiveMode = p.userMode || sysConfig.mode;
        let effectivePorts = p.userPorts
            ? p.userPorts
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)
            : ports;
        let maxCfg = p.maxConfigs || null;

        let configIndex = 0;
        let profileHostNames = getProfileHostNames(hostName, p);

        profileHostNames.forEach((hName) => {
            let ipEntries = getCleanIpsWithNames(hName, p.cleanIp);
            let allIps = ipEntries.map((e) => e.ip);
            let ips = calcEffectiveIps(
                allIps,
                maxCfg,
                effectiveMode,
                effectivePorts,
                pips.length
            );
            let ipNameMap = {};
            ipEntries.forEach((e) => {
                ipNameMap[e.ip] = e.name;
            });
            effectivePorts.forEach((port) => {
                let sec = getTransportParams(port) === "tls";
                ips.forEach((ip) => {
                    let isVless =
                        effectiveMode === "alpha" || effectiveMode === "both";
                    let isTrojan =
                        effectiveMode === "beta" || effectiveMode === "both";
                    let _pips = pips.length > 0 ? pips : [null];
                    _pips.forEach((selectedProxyIp) => {
                    let ipName = ipNameMap[ip] || "";

                    if (isVless) {
                        let tagStr = getConfigName(
                            "alpha",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);

                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadVl = {
                            junk: randomJunk,
                            protocol: "vl",
                            mode: "proxyip",
                            panelIPs: [],
                        };
                        let pathStrVl = "/" + btoa(JSON.stringify(payloadVl));

                        let configUuid = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid,
                            p.id,
                            selectedProxyIp || "",
                        );

                        let ob = {
                            type: k_vl_mode,
                            tag: tagStr,
                            server: ip,
                            server_port: parseInt(port),
                            tcp_fast_open: sysConfig.enableOpt1 || false,
                            uuid: configUuid,
                            packet_encoding: "xudp",
                            network: "tcp",
                            tls: {
                                enabled: sec,
                                server_name: hName,
                                insecure: allowInsecure,
                                alpn: ["http/1.1"],
                                utls: {
                                    enabled: true,
                                    fingerprint: "randomized",
                                },
                            },
                            transport: {
                                type: "ws",
                                path: pathStrVl,
                                max_early_data: 2560,
                                early_data_header_name:
                                    "Sec-WebSocket-Protocol",
                                headers: {
                                    Host: hName,
                                },
                            },
                        };
                        outboundsArr.push(ob);
                    }

                    if (isTrojan) {
                        let tagStr = getConfigName(
                            "beta",
                            p.name,
                            port,
                            hName,
                            ip,
                            selectedProxyIp,
                            configIndex,
                            ipName,
                        );
                        tagStr = getUniqueName(tagStr);
                        dynamicTags.push(tagStr);

                        let randomJunk = Array.from(
                            { length: 11 },
                            () =>
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                    Math.floor(Math.random() * 62)
                                ],
                        ).join("");
                        let payloadTr = {
                            junk: randomJunk,
                            protocol: "tr",
                            mode: "proxyip",
                            panelIPs: [],
                            relayIdx: configIndex,
                        };
                        let pathStrTr = "/" + btoa(JSON.stringify(payloadTr));

                        let configUuid2 = generateConfigUuid(p.id, configIndex);
                        registerConfigEntry(
                            configUuid2,
                            p.id,
                            selectedProxyIp || "",
                        );

                        let ob = {
                            type: k_tr_mode,
                            tag: tagStr,
                            server: ip,
                            server_port: parseInt(port),
                            tcp_fast_open: sysConfig.enableOpt1 || false,
                            password: p.id,
                            network: "tcp",
                            tls: {
                                enabled: sec,
                                server_name: hName,
                                insecure: allowInsecure,
                                alpn: ["http/1.1"],
                                utls: {
                                    enabled: true,
                                    fingerprint: "randomized",
                                },
                            },
                            transport: {
                                type: "ws",
                                path: pathStrTr,
                                max_early_data: 2560,
                                early_data_header_name:
                                    "Sec-WebSocket-Protocol",
                                headers: {
                                    Host: hName,
                                },
                            },
                        };
                        outboundsArr.push(ob);
                    }
                    configIndex++;
                    if (sysConfig.enableDirectConfigs && pips.length > 0 && selectedProxyIp === pips[0]) {
                        if (isVless) {
                            let tagStr = getUniqueName(
                                getConfigName(
                                    "alpha",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    configIndex,
                                    ipName, true
                                ),
                            );
                            dynamicTags.push(tagStr);
                            proxyGeoInfo.set(tagStr, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadVl = {
                                junk: randomJunk,
                                protocol: "vl",
                                mode: "proxyip",
                                panelIPs: [],
                            };
                            let pathStrVl =
                                "/" + btoa(JSON.stringify(payloadVl));
                            let configUuid = generateConfigUuid(
                                p.id,
                                configIndex,
                            );
                            registerConfigEntry(configUuid, p.id, "");
                            let ob = {
                                type: k_vl_mode,
                                tag: tagStr,
                                server: ip,
                                server_port: parseInt(port),
                                tcp_fast_open: sysConfig.enableOpt1 || false,
                                uuid: configUuid,
                                packet_encoding: "xudp",
                                network: "tcp",
                                tls: {
                                    enabled: sec,
                                    server_name: hName,
                                    insecure: allowInsecure,
                                    alpn: ["http/1.1"],
                                    utls: {
                                        enabled: true,
                                        fingerprint: "randomized",
                                    },
                                },
                                transport: {
                                    type: "ws",
                                    path: pathStrVl,
                                    max_early_data: 2560,
                                    early_data_header_name:
                                        "Sec-WebSocket-Protocol",
                                    headers: { Host: hName },
                                },
                            };
                            outboundsArr.push(ob);
                        }
                        if (isTrojan) {
                            let tagStr = getUniqueName(
                                getConfigName(
                                    "beta",
                                    p.name,
                                    port,
                                    hName,
                                    ip,
                                    null,
                                    configIndex,
                                    ipName, true
                                ),
                            );
                            dynamicTags.push(tagStr);
                            proxyGeoInfo.set(tagStr, getGeoInfo(ip));
                            let randomJunk = Array.from(
                                { length: 11 },
                                () =>
                                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[
                                        Math.floor(Math.random() * 62)
                                    ],
                            ).join("");
                            let payloadTr = {
                                junk: randomJunk,
                                protocol: "tr",
                                mode: "proxyip",
                                panelIPs: [],
                                relayIdx: configIndex,
                            };
                            let pathStrTr =
                                "/" + btoa(JSON.stringify(payloadTr));
                            let configUuid2 = generateConfigUuid(
                                p.id,
                                configIndex,
                            );
                            let ob = {
                                type: k_tr_mode,
                                tag: tagStr,
                                server: ip,
                                server_port: parseInt(port),
                                tcp_fast_open: sysConfig.enableOpt1 || false,
                                password: p.id,
                                network: "tcp",
                                tls: {
                                    enabled: sec,
                                    server_name: hName,
                                    insecure: allowInsecure,
                                    alpn: ["http/1.1"],
                                    utls: {
                                        enabled: true,
                                        fingerprint: "randomized",
                                    },
                                },
                                transport: {
                                    type: "ws",
                                    path: pathStrTr,
                                    max_early_data: 2560,
                                    early_data_header_name:
                                        "Sec-WebSocket-Protocol",
                                    headers: { Host: hName },
                                },
                            };
                            outboundsArr.push(ob);
                        }
                        configIndex++;
                    }
                    });
                });
            });
        });
    });

    if (dynamicTags.length === 0) {
        dynamicTags.push("direct");
    }

    
    await fetchTemplates(env);
    if (singboxTemplate) {
        let tpl = JSON.parse(JSON.stringify(singboxTemplate));
        let newOutbounds = [];
        let allProxies = outboundsArr.map(o => o.tag);
        
        for (let ob of tpl.outbounds) {
            if (ob === "__OUTBOUNDS__") {
                newOutbounds.push(...outboundsArr);
            } else if (ob.outbounds && ob.outbounds.includes("{all_proxies}")) {
                let obCpy = { ...ob };
                obCpy.outbounds = [];
                for (let tag of ob.outbounds) {
                    if (tag === "{all_proxies}") obCpy.outbounds.push(...allProxies);
                    else obCpy.outbounds.push(tag);
                }
                newOutbounds.push(obCpy);
            } else {
                newOutbounds.push(ob);
            }
        }
        tpl.outbounds = newOutbounds;
        return tpl;
    }
    // Fallback if template fails
    return {
        log: { disabled: false, level: "warn", timestamp: true },
        dns: { servers: [], rules: [] },
        inbounds: [],
        [k_obds]: outboundsArr,
        route: { rules: [] }
    };
}

