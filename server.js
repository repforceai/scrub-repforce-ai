// server.js — scrub.repforce.ai (stable + /health)
// Uses only the verified endpoints for your tenant:
//   POST {BASE}/scrub/phone/        (payload { phone_number, type, [state] })
//   POST {BASE}/scrub/phones/       (mass)
//   POST {BASE}/scrub/phones/get/   (poll)
// Mass-first with single fallback (per-record progress)
// Keep-alive + token-bucket RPS limiter
// Round-trip CSVs (original columns + scrub fields) + summary
// /health for Railway, /debug/egress for IP, CORS, Socket.IO

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

const TCPA_USER = process.env.TCPA_USER || "";
const TCPA_PASS = process.env.TCPA_PASS || "";

const TCPA_BASE = (process.env.TCPA_BASE || "https://api.tcpalitigatorlist.com").replace(/\/+$/,"");
const MASS_URL        = `${TCPA_BASE}/scrub/phones/`;
const MASS_GET_URL    = `${TCPA_BASE}/scrub/phones/get/`;
const SINGLE_POST_URL = `${TCPA_BASE}/scrub/phone/`;

const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "64", 10);
const RATE_LIMIT_RPS  = parseInt(process.env.RATE_LIMIT_RPS  || "45", 10);    // public ~50; raise on dedicated
const TCPA_FORCE_MODE = (process.env.TCPA_FORCE_MODE || "auto").toLowerCase(); // auto | single | mass

await fs.mkdir(FILES_DIR, { recursive: true });
app.use(express.json());

// ===== CORS =====
app.use((req,res,next)=>{
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
    res.setHeader("Access-Control-Allow-Credentials","true");
    res.setHeader("Access-Control-Allow-Headers","Content-Type, Authorization");
    res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  }
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ===== Health & Utility =====
app.get("/health", (_req,res)=>res.status(200).send("OK"));  // <— Railway health check
app.get("/debug/egress", async (_req,res)=>{
  try { const { data } = await axios.get("https://api.ipify.org?format=json", { timeout: 5000 }); res.json(data); }
  catch(e){ res.status(500).json({ error: e?.message || "ipify failed" }); }
});

// ===== Static UI & files =====
app.use("/ui", express.static(path.join(__dirname, "ui")));
app.get("/", (_req,res)=>res.redirect("/ui"));
app.use("/files", express.static(FILES_DIR));

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

    // Preserve ALL original columns/rows
    const originalRows = parsed.data.map((row, idx) => ({ __idx: idx, __row: row }));
    const headerRow = parsed.meta?.fields || Object.keys(parsed.data[0] || {});
    const phoneKey = headerRow.find(h => String(h).toLowerCase().includes("phone")) || "phone";

    const metaRows = [];
    const phones = [];
    for (const { __idx, __row } of originalRows) {
      const raw = (__row?.[phoneKey] ?? "").toString();
      const norm = raw.replace(/\D/g, "");
      if (norm.length >= 10) {
        metaRows.push({ idx: __idx, phone: norm, orig: __row });
        phones.push(norm);
      } else {
        metaRows.push({ idx: __idx, phone: "", orig: __row, badPhone: true });
      }
    }

    if (!phones.length)
      throw new Error(`No phone numbers found (looking for a column like "phone"; detected "${phoneKey}").`);

    const jobId = Date.now().toString(36);
    await fs.writeFile(path.join(FILES_DIR, `upload_${jobId}.json`),
      JSON.stringify({ total: metaRows.length, at: new Date().toISOString(), phoneKey }));

    // Chunking (only affects MASS batching/progress)
    const chunkSize = 10000;
    const chunks = [];
    for (let i=0;i<phones.length;i+=chunkSize) chunks.push(phones.slice(i,i+chunkSize));

    res.json({ job_id: jobId });

    scrubInChunks(jobId, chunks, options, metaRows, phoneKey).catch(err=>{
      io.emit(`job:${jobId}:error`, { error: err?.message || "Scrub failed" });
    });

  }catch(e){
    res.status(400).json({ error: e?.message || "Upload failed" });
  }
});

// ===== Socket.IO =====
io.on("connection", ()=>{});

// ===== Keep-alive + RPS limiter =====
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

// ===== Utilities =====
function safeJson(v){ try { return typeof v==="string" ? v : JSON.stringify(v); } catch { return String(v); } }
function makeProgress(jobId, total) {
  let processed = 0, lastEmit = 0;
  const minIntervalMs = 60;
  function emit(){ io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round((processed/total)*100) }); }
  return { inc(n=1){ processed+=n; const now=Date.now(); if(now-lastEmit>=minIntervalMs){ lastEmit=now; emit(); } }, add(n=1){ this.inc(n); }, flush(){ emit(); } };
}

// ===== TCPA calls (locked to proven routes) =====
async function massPost(payload) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  await acquireToken();
  const { data } = await AX.post(MASS_URL, payload, { auth, timeout: 30000 });
  return data;
}
async function massGet(job_id) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  await acquireToken();
  const { data } = await AX.post(MASS_GET_URL, { job_id }, { auth, timeout: 30000 });
  return data;
}
async function singlePost(phone, types, states) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  const payload = { phone_number: phone, type: Array.isArray(types)&&types.length===1 ? String(types[0]) : types };
  if (states?.length) payload.state = states;
  await acquireToken();
  const { data } = await AX.post(SINGLE_POST_URL, payload, { auth, timeout: 12000 });
  return data;
}

// ===== SINGLE fallback (per-record progress) =====
async function scrubViaSingleEndpoint(phones, types, states, prog) {
  const results = [];
  const queue = [...phones];
  const workers = [];

  async function worker() {
    while (queue.length) {
      const phone = queue.shift();
      try {
        const data = await singlePost(phone, types, states);
        const r = data?.result || (Array.isArray(data?.results) ? data.results[0] : null) || {};
        results.push({
          phone_number: r.phone_number || phone,
          clean: Number(r.clean ?? (r.is_bad_number ? 0 : 1)),
          is_bad_number: (r.is_bad_number ?? (r.clean ? 0 : 1)) ? 1 : 0,
          status_array: Array.isArray(r.status_array) ? r.status_array : (Array.isArray(r.flags) ? r.flags : []),
          status: r.status || ""
        });
      } catch (e) {
        results.push({ phone_number: phone, clean: 0, is_bad_number: 1, status_array: ["tcpalist_error"], status: "single_endpoint_error" });
      } finally {
        prog.inc(1);
      }
    }
  }

  for (let i=0; i<MAX_CONCURRENCY; i++) workers.push(worker());
  await Promise.allSettled(workers);
  return results;
}

// ===== Merge results into ORIGINAL rows =====
function mergeOriginalWithResults(metaRows, results, phoneKey) {
  const phoneToIdxQ = new Map();
  for (const mr of metaRows) {
    const p = mr.phone;
    if (!phoneToIdxQ.has(p)) phoneToIdxQ.set(p, []);
    phoneToIdxQ.get(p).push(mr.idx);
  }
  const attached = new Map();

  const flagPack = (r) => {
    const flagsArr = Array.isArray(r.status_array) ? r.status_array : (Array.isArray(r.flags) ? r.flags : []);
    const flagsCsv = flagsArr.join(",");
    const fset = new Set(flagsArr.map(s => String(s || "").toLowerCase()));
    const tcpa            = (fset.has("tcpa") || fset.has("troll") || fset.has("litigator")) ? 1 : 0;
    const dnc_complainers = fset.has("dnc_complainers") ? 1 : 0;
    const federal_dnc     = (fset.has("dnc") || fset.has("federal_dnc")) ? 1 : 0;
    let state_dnc = 0, state_code = "";
    for (const f of fset) if (f.startsWith("dnc_state")) {
      state_dnc = 1;
      const m = f.match(/dnc_state[-:_\s]?([a-z]{2})/i);
      if (m) state_code = m[1].toUpperCase();
    }
    return { flagsCsv, tcpa, dnc_complainers, federal_dnc, state_dnc, state_code };
  };

  for (const r of results) {
    const ph = (r.phone_number || "").toString().replace(/\D/g, "");
    const q = phoneToIdxQ.get(ph);
    if (q && q.length) {
      const idx = q.shift();
      const clean = Number(r.clean ?? (r.is_bad_number ? 0 : 1));
      const isBad = (r.is_bad_number ?? (r.clean ? 0 : 1)) ? 1 : 0;
      const status = r.status || "";
      const pack = flagPack(r);
      attached.set(idx, {
        phone_normalized: ph,
        clean, is_bad_number: isBad,
        status_array: pack.flagsCsv, status,
        tcpa: pack.tcpa, dnc_complainers: pack.dnc_complainers,
        federal_dnc: pack.federal_dnc, state_dnc: pack.state_dnc, state_code: pack.state_code
      });
    }
  }

  return metaRows.map(mr => {
    const orig = { ...(mr.orig || {}) };
    const added = attached.get(mr.idx) || {
      phone_normalized: (mr.phone || "").toString(),
      clean: mr.badPhone ? 0 : 0,
      is_bad_number: mr.badPhone ? 1 : 1,
      status_array: mr.badPhone ? "invalid_phone" : "tcpalist_error",
      status: mr.badPhone ? "invalid_phone" : "no_match_or_error",
      tcpa: 0, dnc_complainers: 0, federal_dnc: 0, state_dnc: 0, state_code: ""
    };
    if (!orig.hasOwnProperty("phone_normalized")) orig["phone_normalized"] = added.phone_normalized;
    return { ...orig,
      clean: added.clean, is_bad_number: added.is_bad_number,
      status_array: added.status_array, status: added.status,
      tcpa: added.tcpa, dnc_complainers: added.dnc_complainers,
      federal_dnc: added.federal_dnc, state_dnc: added.state_dnc, state_code: added.state_code
    };
  });
}

// ===== Orchestration =====
async function scrubInChunks(jobId, chunks, options, metaRows, phoneKey) {
  const total = metaRows.filter(m => m.phone && m.phone.length >= 10).length;
  const prog = makeProgress(jobId, total);
  const results = [];

  for (let i=0;i<chunks.length;i++){
    const phones = chunks[i];
    const payload = { phones, type: options.types || ["tcpa","dnc_complainers"] };
    if (options.states?.length) payload.state = options.states;

    let data;
    try {
      if (TCPA_FORCE_MODE === "single") throw new Error("force_single");
      data = await massPost(payload);
    } catch (e) {
      const msg = String(e?.message || "");
      const forbidden = msg.includes("401") && msg.includes("rest_forbidden");
      const forced = TCPA_FORCE_MODE === "single" || msg.includes("force_single");
      if (!forbidden && !forced) throw e;

      const single = await scrubViaSingleEndpoint(phones, payload.type, payload.state, prog);
      results.push(...single);
      continue;
    }

    if (Array.isArray(data?.results)) {
      results.push(...data.results);
      prog.add(phones.length);
      continue;
    }
    if (data?.job_id || data?.job_key) {
      const job_id = data.job_id || data.job_key;
      const out = await massGet(job_id);
      if (!out?.ready || !Array.isArray(out?.results)) throw new Error(`TCPA job not ready or invalid response for job_id ${job_id}`);
      results.push(...out.results);
      prog.add(phones.length);
      continue;
    }

    throw new Error(`Unexpected TCPA response ${safeJson(data)}`);
  }

  const mergedRows = mergeOriginalWithResults(metaRows, results, phoneKey);
  const { fullPath, cleanPath, badPath, summary } = await buildCsvsRoundTrip(mergedRows);

  prog.flush();

  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanPath)}`,
    badUrl:   `/files/${path.basename(badPath)}`,
    fullUrl:  `/files/${path.basename(fullPath)}`,
    summary
  });
}

// ===== Round-trip CSV builder =====
async function buildCsvsRoundTrip(mergedRows) {
  const first = mergedRows[0] || {};
  const appended = ["phone_normalized","clean","is_bad_number","status_array","status","tcpa","dnc_complainers","federal_dnc","state_dnc","state_code"];
  const originalCols = Object.keys(first).filter(k => !appended.includes(k));
  const COLS = [...originalCols, ...appended];

  const isDialable = (r) => (Number(r.clean) === 1) || (Number(r.is_bad_number) === 0);
  const cleanRows = mergedRows.filter(isDialable);
  const badRows   = mergedRows.filter(r => !isDialable(r));

  const ts = Date.now();
  const fullPath  = path.join(FILES_DIR, `full_results_${ts}.csv`);
  const cleanPath = path.join(FILES_DIR, `clean_numbers_${ts}.csv`);
  const badPath   = path.join(FILES_DIR, `bad_numbers_${ts}.csv`);

  await writeCsvWithColumns(fullPath,  mergedRows, COLS);
  await writeCsvWithColumns(cleanPath, cleanRows,  COLS);
  await writeCsvWithColumns(badPath,   badRows,    COLS);

  const sum = {
    total: mergedRows.length,
    clean: cleanRows.length,
    bad:   badRows.length,
    breakdown: {
      tcpa: mergedRows.reduce((a,r)=>a+Number(r.tcpa||0),0),
      dnc_complainers: mergedRows.reduce((a,r)=>a+Number(r.dnc_complainers||0),0),
      federal_dnc: mergedRows.reduce((a,r)=>a+Number(r.federal_dnc||0),0),
      state_dnc: mergedRows.reduce((a,r)=>a+Number(r.state_dnc||0),0),
      state_dnc_by_state: mergedRows.reduce((acc,r)=>{ const s=(r.state_code||"").toString().toUpperCase(); if(s){ acc[s]=(acc[s]||0)+1; } return acc; }, {})
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
      const p = path.join(__dirname, "files", n);
      const st = await fs.stat(p);
      if (now - st.mtimeMs > ttlMs) await fs.rm(p, { force: true });
    }
  }catch{}
}, 60*60*1000);

// ===== start =====
server.listen(PORT, ()=>console.log(`scrub.repforce.ai server listening on :${PORT}`));
