// server.js — scrub.repforce.ai (robust TCPA client with multi-host/multi-path fallbacks)

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

const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

const FILES_DIR = process.env.FILES_DIR || path.join(__dirname, "files");
const FILE_TTL_HOURS = parseInt(process.env.FILE_TTL_HOURS || "72", 10);

// --- TCPA config ---
const TCPA_USER = process.env.TCPA_USER || "";
const TCPA_PASS = process.env.TCPA_PASS || "";
// We’ll try all of these automatically
const TCPA_BASES = [
  (process.env.TCPA_BASE || "https://api.tcpalitigatorlist.com").replace(/\/+$/,""),
  "https://api101.tcpalitigatorlist.com"
];
const TCPA_PATHS = ["/api/scrub/phones/", "/scrub/phones/"];
const TCPA_GET_PATHS = ["/api/scrub/phones/get/", "/scrub/phones/get/"];

await fs.mkdir(FILES_DIR, { recursive: true });
app.use(express.json());

// --- CORS ---
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
  if (req.method==="OPTIONS") return res.sendStatus(200);
  next();
});

// --- Static UI & files ---
app.use("/ui", express.static(path.join(__dirname, "ui")));
app.get("/", (_req,res)=>res.redirect("/ui"));
app.use("/files", express.static(FILES_DIR));

// --- Upload endpoint ---
const upload = multer({ limits: { fileSize: 25 * 1024 * 1024 } });

app.post("/api/upload", upload.single("file"), async (req,res)=>{
  try{
    const options = req.body.optionsJson ? JSON.parse(req.body.optionsJson) : { types: ["tcpa","dnc_complainers"], states: [] };
    if (!req.file) throw new Error("No file uploaded");

    const csvText = req.file.buffer.toString("utf8");
    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors?.length) throw new Error("CSV parse error");

    // Accept any column with 'phone' in the header
    const first = parsed.data[0] || {};
    const phoneKey = Object.keys(first).find(h => h.toLowerCase().includes("phone")) || "phone";

    const phones = parsed.data
      .map(r => String((r[phoneKey] ?? "")).replace(/\D/g,""))
      .filter(p => p.length >= 10);

    if (!phones.length) throw new Error(`No phone numbers found (looking for column like "phone"; detected key "${phoneKey}")`);

    const uploadId = Date.now().toString(36);
    await fs.writeFile(path.join(FILES_DIR, `upload_${uploadId}.json`), JSON.stringify({ total: phones.length, at: new Date().toISOString() }));

    // chunk
    const chunkSize = 5000;
    const chunks = [];
    for (let i=0;i<phones.length;i+=chunkSize) chunks.push(phones.slice(i,i+chunkSize));

    res.json({ job_id: uploadId });

    scrubInChunks(uploadId, chunks, options).catch(err=>{
      io.emit(`job:${uploadId}:error`, { error: err?.message || "Scrub failed" });
    });

  }catch(e){ res.status(400).json({ error: e?.message || "Upload failed" }); }
});

// --- Sockets (progress)
io.on("connection", ()=>{});

// --- TCPA robust client ---
async function tcpapost(routeCandidates, payload) {
  const auth = { username: TCPA_USER, password: TCPA_PASS };
  const errs = [];

  for (const base of TCPA_BASES) {
    for (const route of routeCandidates) {
      const url = `${base}${route}`;
      try {
        const { data } = await axios.post(url, payload, { auth, timeout: 60000 });
        return data; // success
      } catch (e) {
        const status = e?.response?.status;
        const body = e?.response?.data;
        errs.push({ url, status, body });
        // If it’s a 401/402, no need to keep hammering the same base/route endlessly—collect and continue
      }
    }
  }
  // Build a concise error with all attempts so support can see it
  const details = errs.map(x => `[${x.status}] ${x.url} -> ${safeJson(x.body)}`).join(" | ");
  const hint = `Auth is Basic with API Username/Secret. If 401 rest_forbidden persists, TCPA must enable API scrubbing on your key or whitelist your server IP.`;
  throw new Error(`TCPA request failed: ${details} :: ${hint}`);
}

function safeJson(v){ try { return typeof v==="string" ? v : JSON.stringify(v); } catch { return String(v); } }

// --- Scrub orchestration ---
async function scrubInChunks(jobId, chunks, options) {
  const results = [];
  let processed = 0;
  const total = chunks.reduce((a,c)=>a+c.length,0);

  for (let i=0;i<chunks.length;i++){
    const phones = chunks[i];
    if (i>0) await wait(750);

    const payload = { phones, type: options.types || ["tcpa","dnc_complainers"] };
    if (options.states?.length) payload.state = options.states;

    // Try mass-scrub on all routes/hosts until one works
    const data = await tcpapost(TCPA_PATHS, payload);

    let chunkResults;
    if (Array.isArray(data?.results)) {
      chunkResults = data.results;
    } else if (data?.job_id) {
      const out = await tcpapost(TCPA_GET_PATHS, { job_id: data.job_id });
      if (!out?.ready || !Array.isArray(out?.results)) throw new Error(`TCPA job not ready or invalid response for job_id ${data.job_id}`);
      chunkResults = out.results;
    } else {
      throw new Error(`Unexpected TCPA response ${safeJson(data)}`);
    }

    results.push(...chunkResults);
    processed += phones.length;
    io.emit(`job:${jobId}:progress`, { processed, total, percent: Math.round(processed/total*100) });
  }

  const { fullCsvPath, cleanCsvPath, summary } = await buildCsvs(results);
  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanCsvPath)}`,
    fullUrl: `/files/${path.basename(fullCsvPath)}`,
    summary
  });
}

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

// --- Cleanup old files ---
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

// --- Start ---
server.listen(PORT, ()=>console.log(`scrub.repforce.ai server listening on :${PORT}`));
