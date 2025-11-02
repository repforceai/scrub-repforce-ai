// server.js — scrub.repforce.ai (production build)
// - Mass endpoint first, fallback to single if 401/forbidden
// - HTTP keep-alive + token-bucket RPS limiter
// - Env knobs: MAX_CONCURRENCY, RATE_LIMIT_RPS, TCPA_FORCE_MODE, TCPA_BASE
// - Portal-style CSVs: Clean Numbers, Bad Numbers, Full Results
// - Summary breakdown: TCPA, DNC Complainers, Federal DNC, State DNC (+per-state)

import express from "express";
import multer from "multer";
import dotenv from "dotenv";
import axios from "axios";
import { Server as IOServer } from "socket.io";
import http from "http";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Papa from "papaparse";
import { stringify } from "csv-stringify";
import https from "https";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new IOServer(server, { cors: { origin: (o, cb) => cb(null, true) } });

// ===== ENV / CONFIG =====
const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",").map(s => s.trim()).filter(Boolean);

const FILES_DIR = process.env.FILES_DIR || path.join(__dirname, "files");
const FILE_TTL_HOURS = parseInt(process.env.FILE_TTL_HOURS || "72", 10);

// API creds/hosts
const TCPA_USER = process.env.TCPA_USER || "";
const TCPA_PASS = process.env.TCPA_PASS || "";

// Primary + backup hosts; dedicated host (if provided) overrides first slot
const PRIMARY_BASE = (process.env.TCPA_BASE || "https://api.tcpalitigatorlist.com").replace(/\/+$/,"");
const TCPA_BASES = [PRIMARY_BASE, "https://api101.tcpalitigatorlist.com"];

// Routes (try both styles; vendor pages sometimes show with/without /api)
const TCPA_MASS_PATHS     = ["/api/scrub/phones/", "/scrub/phones/"];
const TCPA_MASS_GET_PATHS = ["/api/scrub/phones/get/", "/scrub/phones/get/"];
const TCPA_SINGLE_PATHS   = ["/api/scrub/phone/", "/scrub/phone/"];

// Throttle + mode knobs
const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "64", 10); // worker pool for single fallback
const RATE_LIMIT_RPS  = parseInt(process.env.RATE_LIMIT_RPS  || "45", 10); // stay < public 50 rps
const TCPA_FORCE_MODE = (process.env.TCPA_FORCE_MODE || "auto").toLowerCase(); // auto | single | mass

await fs.mkdir(FILES_DIR, { recursive: true });
app.use(express.json());

// ===== CORS =====
app.use((req, res, next) => {
  const origin = req.headers.origin;
  const ok = !ALLOWED_ORIGINS.length || (origin && ALLOWED_ORIGINS.some(o=>{
    if (o.includes("*")) {
      const re = new RegExp("^"+o.replace(/\./g,"\\.").replace(/\*/g,".*").replace(/\//g,"\\/")+"$");
      return re.test(origin);
    }
    return o === origin;
  }));
  if (ok) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  }
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ===== Static UI & files =====
app.use("/ui", express.static(path.join(__dirname, "ui")));
app.get("/", (_req,res)=>res.redirect("/ui"));
app.use("/files", express.static(FILES_DIR));

// Egress IP helper (give this IP to TCPA if they whitelist)
app.get("/debug/egress", async (_req,res)=>{
  try {
    const { data } = await axios.get("https://api.ipify.org?format=json", { timeout: 5000 });
    res.json(data); // { ip: "x.x.x.x" }
  } catch (e) { res.status(500).json({ error: e?.message || "ipify failed" }); }
});

// ===== Upload endpoint =====
const upload = multer({ limits: { fileSize: 25 * 1024 * 1024 } });

app.post("/api/upload", upload.single("file"), async (req,res)=>{
  try{
    const options = req.body.optionsJson ? JSON.parse(req.body.optionsJson)
      : { types: ["tcpa","dnc_complainers"], states: [] };

    if (!req.file) throw new Error("No file uploaded");

    const csvText = req.file.buffer.toString("utf8");
    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors?.length) throw new Error("CSV parse error");

    // accept any header containing "phone"
    const first = parsed.data[0] || {};
    const phoneKey = Object.keys(first).find(h => h?.toLowerCase?.().includes("phone")) || "phone";

    const phones = parsed.data
      .map(r => String(r?.[phoneKey] ?? "").replace(/\D/g,""))
      .filter(p => p.length >= 10);

    if (!phones.length)
      throw new Error(`No phone numbers found (looking for a column like "phone"; detected "${phoneKey}").`);

    const jobId = Date.now().toString(36);
    await fs.writeFile(path.join(FILES_DIR, `upload_${jobId}.json`),
      JSON.stringify({ total: phones.length, at: new Date().toISOString() }));

    // Chunk size only affects mass-path batching/progress noise
    const chunkSize = 10000;
    const chunks = [];
    for (let i=0;i<phones.length;i+=chunkSize) chunks.push(phones.slice(i,i+chunkSize));

    res.json({ job_id: jobId });

    scrubInChunks(jobId, chunks, options).catch(err=>{
      io.emit(`job:${jobId}:error`, { error: err?.message || "Scrub failed" });
    });

  }catch(e){
    res.status(400).json({ error: e?.message || "Upload failed" });
  }
});

// ===== Socket.IO =====
io.on("connection", ()=>{});

// ===== HTTP keep-alive + rate limiter =====
const agent = new https.Agent({ keepAlive: true });
const AX = axios.create({ httpsAgent: agent });

let tokens = RATE_LIMIT_RPS;
const waiters = [];
setInterval(() => {
  tokens = RATE_LIMIT_RPS;
  while (tokens > 0 && waiters.length) { tokens--; waiters.shift()(); }
}, 1000);
function acquireToken() {
  if (tokens > 0) { tokens--; return Promise.resolve(); }
  return new Promise(res => waiters.push(res));
}

// ===== TCPA client helpers =====
function safeJson(v){ try { return typeof v==="string" ? v : JSON.stringify(v); } catch { return String(v); } }

async function tcpapost(routeCandidates, payload) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  const errs = [];
  for (const base of TCPA_BASES) {
    for (const route of routeCandidates) {
      const url = `${base}${route}`;
      try {
        await acquireToken();
        const { data } = await AX.post(url, payload, { auth, timeout: 30000 });
        return data;
      } catch (e) {
        errs.push({ url, status: e?.response?.status, body: e?.response?.data });
      }
    }
  }
  const details = errs.map(x=>`[${x.status}] ${x.url} -> ${safeJson(x.body)}`).join(" | ");
  throw new Error(`TCPA request failed: ${details}`);
}

async function tcpapostSingle(routeCandidates, payload) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  const errs = [];
  for (const base of TCPA_BASES) {
    for (const route of routeCandidates) {
      const url = `${base}${route}`;
      try {
        await acquireToken();
        const { data } = await AX.post(url, payload, { auth, timeout: 12000 });
        return data;
      } catch (e) {
        errs.push({ url, status: e?.response?.status, body: e?.response?.data });
      }
    }
  }
  const details = errs.map(x=>`[${x.status}] ${x.url} -> ${safeJson(x.body)}`).join(" | ");
  throw new Error(`TCPA single failed: ${details}`);
}

// ===== Single-endpoint fallback fan-out =====
async function scrubViaSingleEndpoint(phones, types, states) {
  const results = [];
  const queue = [...phones];
  const workers = [];

  async function worker() {
    while (queue.length) {
      const phone = queue.shift();
      const payload = { phone, type: Array.isArray(types) && types.length === 1 ? String(types[0]) : types };
      if (states?.length) payload.state = states;

      try {
        const data = await tcpapostSingle(TCPA_SINGLE_PATHS, payload);
        const r = data?.result || (Array.isArray(data?.results) ? data.results[0] : null) || {};
        results.push({
          phone_number: r.phone_number || phone,
          clean: Number(r.clean ?? (r.is_bad_number ? 0 : 1)),
          is_bad_number: (r.is_bad_number ?? (r.clean ? 0 : 1)) ? 1 : 0,
          status_array: Array.isArray(r.status_array) ? r.status_array : (Array.isArray(r.flags) ? r.flags : []),
          status: r.status || ""
        });
      } catch {
        // record synthetic error row so job completes visibly
        results.push({
          phone_number: phone, clean: 0, is_bad_number: 1,
          status_array: ["tcpalist_error"], status: "single_endpoint_error"
        });
      }
    }
  }

  for (let i=0; i<MAX_CONCURRENCY; i++) workers.push(worker());
  await Promise.allSettled(workers);
  return results;
}

// ===== Orchestration =====
async function scrubInChunks(jobId, chunks, options) {
  const results = [];
  let processed = 0;
  const total = chunks.reduce((a,c)=>a+c.length,0);

  for (let i=0;i<chunks.length;i++){
    const phones = chunks[i];

    const payload = { phones, type: options.types || ["tcpa","dnc_complainers"] };
    if (options.states?.length) payload.state = options.states;

    // Try MASS unless forced otherwise
    let data;
    try {
      if (TCPA_FORCE_MODE === "single") throw new Error("force_single");
      if (TCPA_FORCE_MODE === "mass") {
        data = await tcpapost(TCPA_MASS_PATHS, payload);
      } else {
        data = await tcpapost(TCPA_MASS_PATHS, payload);
      }
    } catch (e) {
      const msg = String(e?.message || "");
      const forbidden = msg.includes("401") && msg.includes("rest_forbidden");
      const forced = TCPA_FORCE_MODE === "single";
      if (!forbidden && !forced && !msg.includes("force_single")) throw e;

      // Fallback: single fan-out for this chunk
      const single = await scrubViaSingleEndpoint(phones, payload.type, payload.state);
      results.push(...single);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue;
    }

    // MASS handling
    if (Array.isArray(data?.results)) {
      results.push(...data.results);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue;
    }
    if (data?.job_id) {
      const out = await tcpapost(TCPA_MASS_GET_PATHS, { job_id: data.job_id });
      if (!out?.ready || !Array.isArray(out?.results)) throw new Error(`TCPA job not ready or invalid response for job_id ${data.job_id}`);
      results.push(...out.results);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue;
    }

    throw new Error(`Unexpected TCPA response ${safeJson(data)}`);
  }

  const { fullPath, cleanPath, badPath, summary } = await buildCsvs(results);

  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanPath)}`,  // Download Clean Numbers
    badUrl:   `/files/${path.basename(badPath)}`,    // Download Bad Numbers
    fullUrl:  `/files/${path.basename(fullPath)}`,   // Download Full Results
    summary
  });
}

// ===== Portal-style CSV builder =====
async function buildCsvs(apiResults) {
  const rows = apiResults.map(r => {
    const flagsArr = Array.isArray(r.status_array) ? r.status_array : (Array.isArray(r.flags) ? r.flags : []);
    const flagsCsv = flagsArr.join(","); // portal uses comma-separated
    const phone = r.phone_number || r.phone || "";
    const clean = Number(r.clean ?? (r.is_bad_number ? 0 : 1));
    const isBad = (r.is_bad_number ?? (r.clean ? 0 : 1)) ? 1 : 0;
    const status = r.status || "";

    const fset = new Set(flagsArr.map(s => String(s).toLowerCase()));
    const tcpa            = (fset.has("tcpa") || fset.has("troll") || fset.has("litigator")) ? 1 : 0;
    const dnc_complainers = fset.has("dnc_complainers") ? 1 : 0;
    const federal_dnc     = (fset.has("dnc") || fset.has("federal_dnc")) ? 1 : 0;

    let state_dnc = 0, state_code = "";
    for (const f of fset) {
      if (f.startsWith("dnc_state")) {
        state_dnc = 1;
        const m = f.match(/dnc_state[-:_\s]?([a-z]{2})/i);
        if (m) state_code = m[1].toUpperCase();
      }
    }

    return {
      phone_number: phone,
      clean,
      is_bad_number: isBad,
      status_array: flagsCsv,
      status,
      tcpa,
      dnc_complainers,
      federal_dnc,
      state_dnc,
      state_code
    };
  });

  const isDialable = (r) => (r.clean === 1) || (r.is_bad_number === 0);
  const cleanRows = rows.filter(isDialable);
  const badRows   = rows.filter(r => !isDialable(r));

  const COLS = [
    "phone_number",
    "clean",
    "is_bad_number",
    "status_array",
    "status",
    "tcpa",
    "dnc_complainers",
    "federal_dnc",
    "state_dnc",
    "state_code"
  ];

  const ts = Date.now();
  const fullPath  = path.join(FILES_DIR, `full_results_${ts}.csv`);
  const cleanPath = path.join(FILES_DIR, `clean_numbers_${ts}.csv`);
  const badPath   = path.join(FILES_DIR, `bad_numbers_${ts}.csv`);

  await writeCsvWithColumns(fullPath,  rows,      COLS);
  await writeCsvWithColumns(cleanPath, cleanRows, COLS);
  await writeCsvWithColumns(badPath,   badRows,   COLS);

  const sum = {
    total: rows.length,
    clean: cleanRows.length,
    bad:   badRows.length,
    breakdown: {
      tcpa: rows.reduce((a,r)=>a+r.tcpa,0),
      dnc_complainers: rows.reduce((a,r)=>a+r.dnc_complainers,0),
      federal_dnc: rows.reduce((a,r)=>a+r.federal_dnc,0),
      state_dnc: rows.reduce((a,r)=>a+r.state_dnc,0),
      state_dnc_by_state: rows.reduce((acc,r)=>{ if(r.state_code){ acc[r.state_code]=(acc[r.state_code]||0)+1; } return acc; }, {})
    }
  };

  return { fullPath, cleanPath, badPath, summary: sum };
}

async function writeCsvWithColumns(filePath, arr, columns) {
  return new Promise((resolve, reject) => {
    const stringifier = stringify(arr, { header: true, columns });
    const ws = createWriteStream(filePath);
    stringifier.pipe(ws);
    stringifier.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
  });
}

// ===== housekeeping =====
function wait(ms){ return new Promise(r=>setTimeout(r,ms)); }

setInterval(async ()=>{
  try{
    const ttlMs = FILE_TTL_HOURS * 3600 * 1000;
    const now = Date.now();
    for (const n of await fs.readdir(FILES_DIR)) {
      const p = path.join(FILES_DIR, n);
      const st = await fs.stat(p);
      if (now - st.mtimeMs > ttlMs) await fs.rm(p, { force: true });
    }
  }catch{}
}, 60*60*1000);

// ===== start =====
server.listen(PORT, ()=>console.log(`scrub.repforce.ai server listening on :${PORT}`));
