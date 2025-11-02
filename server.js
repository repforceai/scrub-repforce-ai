// server.js — scrub.repforce.ai (mass + single fallback, Node 20 OK)

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

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new IOServer(server, { cors: { origin: (o, cb) => cb(null, true) } });

// ===== ENV =====
const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",").map(s => s.trim()).filter(Boolean);

const FILES_DIR = process.env.FILES_DIR || path.join(__dirname, "files");
const FILE_TTL_HOURS = parseInt(process.env.FILE_TTL_HOURS || "72", 10);

const TCPA_USER = process.env.TCPA_USER || "";
const TCPA_PASS = process.env.TCPA_PASS || "";

// We’ll try all known bases/paths automatically
const TCPA_BASES = [
  (process.env.TCPA_BASE || "https://api.tcpalitigatorlist.com").replace(/\/+$/,""),
  "https://api101.tcpalitigatorlist.com"
];
const TCPA_MASS_PATHS     = ["/api/scrub/phones/", "/scrub/phones/"];
const TCPA_SINGLE_PATHS   = ["/api/scrub/phone/",  "/scrub/phone/"];
const TCPA_MASS_GET_PATHS = ["/api/scrub/phones/get/", "/scrub/phones/get/"];

// reasonable concurrency for single endpoint fallback
const MAX_CONCURRENCY = 12;

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

// Egress IP helper (send this to TCPA if they require IP whitelisting)
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

    // chunk
    const chunkSize = 5000;
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

// sockets for progress
io.on("connection", ()=>{});

// ===== TCPA client helpers =====
function safeJson(v){ try { return typeof v==="string" ? v : JSON.stringify(v); } catch { return String(v); } }

async function tcpapost(routeCandidates, payload) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  const errs = [];
  for (const base of TCPA_BASES) {
    for (const route of routeCandidates) {
      const url = `${base}${route}`;
      try {
        const { data } = await axios.post(url, payload, { auth, timeout: 60000 });
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
        const { data } = await axios.post(url, payload, { auth, timeout: 30000 });
        return data;
      } catch (e) {
        errs.push({ url, status: e?.response?.status, body: e?.response?.data });
      }
    }
  }
  const details = errs.map(x=>`[${x.status}] ${x.url} -> ${safeJson(x.body)}`).join(" | ");
  throw new Error(`TCPA single failed: ${details}`);
}

// ===== Fallback fan-out via single endpoint =====
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
        // be safe: mark as blocked if single call fails
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

// ===== Main scrub orchestration =====
async function scrubInChunks(jobId, chunks, options) {
  const results = [];
  let processed = 0;
  const total = chunks.reduce((a,c)=>a+c.length,0);

  for (let i=0;i<chunks.length;i++){
    const phones = chunks[i];
    if (i>0) await wait(750);

    const payload = { phones, type: options.types || ["tcpa","dnc_complainers"] };
    if (options.states?.length) payload.state = options.states;

    // Try mass first
    let data;
    try {
      data = await tcpapost(TCPA_MASS_PATHS, payload);
    } catch (e) {
      const msg = String(e?.message || "");
      const forbidden = msg.includes("401") && msg.includes("rest_forbidden");

      if (!forbidden) throw e; // real error => bubble out

      // Fallback to single endpoint fan-out for this chunk
      const single = await scrubViaSingleEndpoint(phones, payload.type, payload.state);
      results.push(...single);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue; // next chunk
    }

    // Mass response handling
    if (Array.isArray(data?.results)) {
      results.push(...data.results);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue;
    }
    if (data?.job_id) {
      // poll job
      const out = await tcpapost(TCPA_MASS_GET_PATHS, { job_id: data.job_id });
      if (!out?.ready || !Array.isArray(out?.results)) throw new Error(`TCPA job not ready or invalid response for job_id ${data.job_id}`);
      results.push(...out.results);
      processed += phones.length;
      io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
      continue;
    }

    throw new Error(`Unexpected TCPA response ${safeJson(data)}`);
  }

  const { fullCsvPath, cleanCsvPath, summary } = await buildCsvs(results);
  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanCsvPath)}`,
    fullUrl: `/files/${path.basename(fullCsvPath)}`,
    summary
  });
}

// ===== CSV builders =====
async function buildCsvs(apiResults) {
  const full = apiResults.map(r => ({
    phone: r.phone_number,
    clean: Number(r.clean),
    is_bad_number: r.is_bad_number ? 1 : 0,
    flags: Array.isArray(r.status_array) ? r.status_array.join("|") : "",
    status: r.status || ""
  }));
  const clean = full.filter(r => r.clean === 1 || r.is_bad_number === 0);

  const ts = Date.now();
  const fullCsvPath = path.join(FILES_DIR, `full_results_${ts}.csv`);
  const cleanCsvPath = path.join(FILES_DIR, `clean_${ts}.csv`);

  await writeCsv(fullCsvPath, full);
  await writeCsv(cleanCsvPath, clean);

  return { fullCsvPath, cleanCsvPath, summary: { total: full.length, clean: clean.length, blocked: full.length - clean.length } };
}

async function writeCsv(filePath, rows){
  return new Promise((resolve,reject)=>{
    const columns = Object.keys(rows[0] || { phone:"", clean:0, is_bad_number:0, flags:"", status:"" });
    const stringifier = stringify(rows, { header: true, columns });
    const ws = createWriteStream(filePath);
    stringifier.pipe(ws);
    stringifier.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
  });
}

function wait(ms){ return new Promise(r=>setTimeout(r,ms)); }

// ===== housekeeping =====
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
