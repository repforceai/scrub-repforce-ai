import express from 'express';
import multer from 'multer';
import dotenv from 'dotenv';
import axios from 'axios';
import { Server as IOServer } from 'socket.io';
import http from 'http';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import Papa from 'papaparse';
import { stringify } from 'csv-stringify';
import mime from 'mime-types';

dotenv.config();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const io = new IOServer(server, { cors: { origin: (origin, cb) => cb(null, true) } });

const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const FILES_DIR = process.env.FILES_DIR || path.join(__dirname, 'files');
const STORAGE_MODE = process.env.STORAGE_MODE || 'local';
const FILE_TTL_HOURS = parseInt(process.env.FILE_TTL_HOURS || '72', 10);

const TCPA_BASE = process.env.TCPA_BASE || 'https://api.tcpalitigatorlist.com';
const TCPA_USER = process.env.TCPA_USER;
const TCPA_PASS = process.env.TCPA_PASS;

await fs.mkdir(FILES_DIR, { recursive: true });
app.use(express.json());

// Basic CORS
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (!ALLOWED_ORIGINS.length || (origin && ALLOWED_ORIGINS.some(o => origin === o || (o.includes('*') && new RegExp(o.replace(/\./g,'\\.').replace('*','.*')).test(origin))))) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  }
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Serve UI
app.use('/ui', express.static(path.join(__dirname, 'ui')));
app.get('/', (req, res) => res.redirect('/ui'));

// Static file serving for result links
app.use('/files', express.static(FILES_DIR));

// Upload handler
const upload = multer({ limits: { fileSize: 25 * 1024 * 1024 } }); // 25MB
app.post('/api/upload', upload.single('file'), async (req, res) => {
  try {
    const { optionsJson } = req.body;
    const options = optionsJson ? JSON.parse(optionsJson) : { types: ['tcpa','dnc_complainers'], states: [] };

    if (!req.file) throw new Error('No file uploaded');
    const csvText = req.file.buffer.toString('utf8');
    const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
    if (parsed.errors?.length) throw new Error('CSV parse error');

    const allRows = parsed.data;
    const phones = allRows.map(r => String(r.phone || '').replace(/\D/g, '')).filter(p => p.length >= 10);
    if (!phones.length) throw new Error('No valid phones found');

    // Save upload snapshot (optional)
    const uploadId = Date.now().toString(36);
    await fs.writeFile(path.join(FILES_DIR, `upload_${uploadId}.json`), JSON.stringify({ total: phones.length, at: new Date().toISOString() }));

    // Chunk strategy for progress (e.g., 5k per chunk)
    const chunkSize = 5000;
    const chunks = [];
    for (let i = 0; i < phones.length; i += chunkSize) chunks.push(phones.slice(i, i + chunkSize));

    // Build progress record
    const progress = { total: phones.length, processed: 0, percent: 0 };
    const jobId = uploadId;
    res.json({ job_id: jobId }); // Frontend will start polling socket events

    // Start async processing
    scrubInChunks(jobId, chunks, options, allRows).catch(async (err) => {
      io.emit(`job:${jobId}:error`, { error: err.message || 'Scrub failed' });
    });

  } catch (e) {
    res.status(400).json({ error: e.message || 'Upload failed' });
  }
});

// Socket connection (no auth, but CORS restricted by domain)
io.on('connection', (socket) => {
  // no-op, we broadcast by job channel
});

async function scrubInChunks(jobId, chunks, options, allRows) {
  const results = [];
  let processed = 0;
  const total = chunks.reduce((acc, c) => acc + c.length, 0);

  for (let idx = 0; idx < chunks.length; idx++) {
    const phones = chunks[idx];

    // Respect vendor throttling: small delay per chunk
    if (idx > 0) await wait(750);

    // POST to TCPA mass endpoint
    const payload = { phones, type: options.types || ['tcpa','dnc_complainers'] };
    if (options.states && options.states.length) payload.state = options.states;

    const { data } = await axios.post(`${TCPA_BASE}/scrub/phones/`, payload, {
      auth: { username: TCPA_USER, password: TCPA_PASS },
      timeout: 60000
    });

    let chunkResults = [];
    if (Array.isArray(data?.results)) {
      chunkResults = data.results;
    } else if (data?.job_id) {
      // Poll for this chunk job
      chunkResults = await pollJobResults(data.job_id);
    } else {
      throw new Error('Unexpected TCPA response');
    }

    results.push(...chunkResults);
    processed += phones.length;
    const percent = Math.round((processed / total) * 100);
    io.emit(`job:${jobId}:progress`, { processed, total, percent });
  }

  // Build CSVs
  const { fullCsvPath, cleanCsvPath, summary } = await buildCsvs(results);
  io.emit(`job:${jobId}:done`, {
    cleanUrl: `/files/${path.basename(cleanCsvPath)}`,
    fullUrl: `/files/${path.basename(fullCsvPath)}`,
    summary
  });
}

async function pollJobResults(jobId) {
  // Poll every 2s until ready
  for (let i = 0; i < 120; i++) {
    const { data } = await axios.post(`${process.env.TCPA_BASE || 'https://api.tcpalitigatorlist.com'}/scrub/phones/get/`, { job_id: jobId }, {
      auth: { username: TCPA_USER, password: TCPA_PASS },
      timeout: 60000
    });
    if (Array.isArray(data?.results) && data.ready) return data.results;
    await wait(2000);
  }
  throw new Error('Job timed out');
}

async function buildCsvs(apiResults) {
  // Normalize: each result expected:
  // { phone_number, clean, is_bad_number, status_array, status }
  const full = apiResults.map(r => ({
    phone: r.phone_number,
    clean: Number(r.clean),
    is_bad_number: r.is_bad_number ? 1 : 0,
    flags: Array.isArray(r.status_array) ? r.status_array.join('|') : '',
    status: r.status || ''
  }));

  const clean = full.filter(r => r.clean === 1 || r.is_bad_number === 0);

  const ts = Date.now();
  const fullCsvPath = path.join(FILES_DIR, `full_results_${ts}.csv`);
  const cleanCsvPath = path.join(FILES_DIR, `clean_${ts}.csv`);

  await writeCsv(fullCsvPath, full);
  await writeCsv(cleanCsvPath, clean);

  const summary = {
    total: full.length,
    clean: clean.length,
    blocked: full.length - clean.length
  };
  return { fullCsvPath, cleanCsvPath, summary };
}

async function writeCsv(filePath, rows) {
  return new Promise((resolve, reject) => {
    const columns = Object.keys(rows[0] || { phone: '', clean: 0, is_bad_number: 0, flags: '', status: '' });
    const stringifier = stringify(rows, { header: true, columns });
    const ws = (await import('fs')).createWriteStream(filePath);
    stringifier.pipe(ws);
    stringifier.on('error', reject);
    ws.on('finish', resolve);
  });
}

function wait(ms){ return new Promise(r => setTimeout(r, ms)); }

// Cleanup old files periodically
setInterval(async () => {
  const ttlMs = FILE_TTL_HOURS * 3600 * 1000;
  try {
    const items = await fs.readdir(FILES_DIR);
    const now = Date.now();
    await Promise.all(items.map(async name => {
      const p = path.join(FILES_DIR, name);
      const st = await fs.stat(p);
      if (now - st.mtimeMs > ttlMs) {
        await fs.rm(p, { force: true });
      }
    }));
  } catch(e){ /* noop */ }
}, 60 * 60 * 1000);

// Start
server.listen(PORT, () => console.log(`scrub.repforce.ai server on :${PORT}`));
