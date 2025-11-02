// server.js — scrub.repforce.ai (stable ESM, Node 20 OK)

import express from "express";
import multer from "multer";
import dotenv from "dotenv";
import axios from "axios";
import { Server as IOServer } from "socket.io";
import http from "http";
import fs from "fs/promises";
import { createWriteStream } from "fs"; // <-- fixed: no dynamic import
import path from "path";
import { fileURLToPath } from "url";
import Papa from "papaparse";
import { stringify } from "csv-stringify";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new IOServer(server, {
  cors: { origin: (origin, cb) => cb(null, true) },
});

// ------------ Env & constants ------------
const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const STORAGE_MODE = process.env.STORAGE_MODE || "local";
const FILES_DIR =
  process.env.FILES_DIR || path.join(__dirname, "files");
const FILE_TTL_HOURS = parseInt(process.env.FILE_TTL_HOURS || "72", 10);

const TCPA_BASE =
  process.env.TCPA_BASE || "https://api.tcpalitigatorlist.com";
const TCPA_USER = process.env.TCPA_USER || "";
const TCPA_PASS = process.env.TCPA_PASS || "";

// Prepare local files dir
await fs.mkdir(FILES_DIR, { recursive: true });

app.use(express.json());

// ------------ CORS ------------
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (
    !ALLOWED_ORIGINS.length ||
    (origin &&
      ALLOWED_ORIGINS.some((o) => {
        if (o.includes("*")) {
          // wildcard like https://*.repforce.ai
          const re = new RegExp(
            "^" +
              o
                .replace(/\./g, "\\.")
                .replace(/\*/g, ".*")
                .replace(/\//g, "\\/") +
              "$"
          );
          return re.test(origin);
        }
        return o === origin;
      }))
  ) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization"
    );
    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET,POST,OPTIONS"
    );
  }
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ------------ Static UI & files ------------
app.use("/ui", express.static(path.join(__dirname, "ui")));
app.get("/", (_req, res) => res.redirect("/ui"));
app.use("/files", express.static(FILES_DIR));

// ------------ Upload endpoint ------------
const upload = multer({
  limits: { fileSize: 25 * 1024 * 1024 }, // 25MB
});

app.post(
  "/api/upload",
  upload.single("file"),
  async (req, res) => {
    try {
      const options = req.body.optionsJson
        ? JSON.parse(req.body.optionsJson)
        : { types: ["tcpa", "dnc_complainers"], states: [] };

      if (!req.file) throw new Error("No file uploaded.");

      const csvText = req.file.buffer.toString("utf8");
      const parsed = Papa.parse(csvText, {
        header: true,
        skipEmptyLines: true,
      });
      if (parsed.errors?.length)
        throw new Error("CSV parse error");

      const allRows = parsed.data;
      const phones = allRows
        .map((r) =>
          String(r.phone || "")
            .replace(/\D/g, "")
            .trim()
        )
        .filter((p) => p.length >= 10);

      if (!phones.length) throw new Error("No valid phones found.");

      // minimal job record
      const uploadId = Date.now().toString(36);
      await fs.writeFile(
        path.join(FILES_DIR, `upload_${uploadId}.json`),
        JSON.stringify({
          total: phones.length,
          at: new Date().toISOString(),
        })
      );

      // Chunk into batches for progress
      const chunkSize = 5000;
      const chunks = [];
      for (let i = 0; i < phones.length; i += chunkSize) {
        chunks.push(phones.slice(i, i + chunkSize));
      }

      // return job id immediately; processing is async
      res.json({ job_id: uploadId });

      scrubInChunks(uploadId, chunks, options, allRows).catch(
        (err) => {
          io.emit(`job:${uploadId}:error`, {
            error: err?.message || "Scrub failed",
          });
        }
      );
    } catch (e) {
      res.status(400).json({ error: e?.message || "Upload failed" });
    }
  }
);

// ------------ WebSockets (progress events) ------------
io.on("connection", () => {
  /* no auth here; CORS already limits origins */
});

// ------------ Scrub logic ------------
async function scrubInChunks(jobId, chunks, options) {
  const results = [];
  let processed = 0;
  const total = chunks.reduce((acc, c) => acc + c.length, 0);

  for (let i = 0; i < chunks.length; i++) {
    const phones = chunks[i];

    if (i > 0) await wait(750); // gentle rate limit

    const payload = {
      phones,
      type: options.types || ["tcpa", "dnc_complainers"],
    };
    if (options.states?.length) payload.state = options.states;

    const { data } = await axios.post(
      `${TCPA_BASE}/scrub/phones/`,
      payload,
      {
        auth: { username: TCPA_USER, password: TCPA_PASS },
        timeout: 60_000,
      }
    );

    let chunkResults;
    if (Array.isArray(data?.results)) {
      chunkResults = data.results;
    } else if (data?.job_id) {
      chunkResults = await pollJobResults(data.job_id);
    } else {
      throw new Error("Unexpected TCPA response");
    }

    results.push(...chunkResults);
    processed += phones.length;
    const percent = Math.round((processed / total) * 100);
    io.emit(`job:${jobId}:progress`, { processed, total, percent });
  }

  const { fullCsvPath, cleanCsvPath, summary } = await buildCsvs(
    results
  );

  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanCsvPath)}`,
    fullUrl: `/files/${path.basename(fullCsvPath)}`,
    summary,
  });
}

async function pollJobResults(jobId) {
  // Poll every 2s up to ~4 minutes
  for (let i = 0; i < 120; i++) {
    const { data } = await axios.post(
      `${TCPA_BASE}/scrub/phones/get/`,
      { job_id: jobId },
      {
        auth: { username: TCPA_USER, password: TCPA_PASS },
        timeout: 60_000,
      }
    );
    if (data?.ready && Array.isArray(data?.results)) {
      return data.results;
    }
    await wait(2000);
  }
  throw new Error("Job timed out");
}

async function buildCsvs(apiResults) {
  // Normalize typical shape returned by vendor
  const full = apiResults.map((r) => ({
    phone: r.phone_number,
    clean: Number(r.clean),
    is_bad_number: r.is_bad_number ? 1 : 0,
    flags: Array.isArray(r.status_array)
      ? r.status_array.join("|")
      : "",
    status: r.status || "",
  }));

  // Keep rows deemed dialable
  const clean = full.filter(
    (r) => r.clean === 1 || r.is_bad_number === 0
  );

  const ts = Date.now();
  const fullCsvPath = path.join(FILES_DIR, `full_results_${ts}.csv`);
  const cleanCsvPath = path.join(FILES_DIR, `clean_${ts}.csv`);

  await writeCsv(fullCsvPath, full);
  await writeCsv(cleanCsvPath, clean);

  return {
    fullCsvPath,
    cleanCsvPath,
    summary: {
      total: full.length,
      clean: clean.length,
      blocked: full.length - clean.length,
    },
  };
}

async function writeCsv(filePath, rows) {
  return new Promise((resolve, reject) => {
    const columns =
      Object.keys(rows[0] || {}) ||
      ["phone", "clean", "is_bad_number", "flags", "status"];

    const stringifier = stringify(rows, { header: true, columns });
    const ws = createWriteStream(filePath); // <-- fixed
    stringifier.pipe(ws);
    stringifier.on("error", reject);
    ws.on("error", reject);
    ws.on("finish", resolve);
  });
}

function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ------------ Housekeeping: purge old files ------------
setInterval(async () => {
  try {
    const ttlMs = FILE_TTL_HOURS * 3600 * 1000;
    const now = Date.now();
    const names = await fs.readdir(FILES_DIR);
    await Promise.all(
      names.map(async (n) => {
        const p = path.join(FILES_DIR, n);
        const st = await fs.stat(p);
        if (now - st.mtimeMs > ttlMs) {
          await fs.rm(p, { force: true });
        }
      })
    );
  } catch {
    /* ignore */
  }
}, 60 * 60 * 1000);

// ------------ Start server ------------
server.listen(PORT, () => {
  console.log(`scrub.repforce.ai server listening on :${PORT}`);
});
