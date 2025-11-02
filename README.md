# scrub.repforce.ai — One-Box Deploy

Production-ready starter: Node/Express API + Socket.IO + static frontend. 
Deploy on **Railway** or **Render** with a single service. 
Express serves both the API and the built frontend. 

## Features
- CSV upload (Multer) with 25MB cap (tune as needed)
- Chunked mass-scrub to TCPA (batches with progress)
- Live progress via WebSocket (% complete and counts)
- Outputs two CSVs: clean.csv and full_results.csv
- Temporary storage in local `/files` (swap to S3/R2 for prod)
- CORS locked by env (`ALLOWED_ORIGINS`)

## Quick Start (local)
```bash
npm i
cp .env.example .env   # fill creds
npm run dev
# Open http://localhost:8080
```

## Deploy (Railway — simplest)
1. Create Railway project → New Service → Deploy from Repo/Zip
2. Add Environment Variables (see .env.example)
3. Set Start Command: `node server.js`
4. Add domain `scrub.repforce.ai` (via Cloudflare CNAME or Railway domains)
5. Done

## Deploy (Render — alternative)
1. New Web Service → Node
2. Build command: `npm i`
3. Start command: `node server.js`
4. Add env vars → Save → wait for green
5. Map `scrub.repforce.ai` to Render URL via Cloudflare CNAME

## AutoCalls Integration
Use the script in `autocalls-tab-snippet.html` inside AutoCalls Custom Scripts.
Change IFRAME_SRC to your deployed URL (`https://scrub.repforce.ai/ui`).

## S3/R2 (optional, recommended)
- Set `STORAGE_MODE=s3`
- Fill AWS-style creds (`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_REGION`)
- Files auto-expire based on `FILE_TTL_HOURS` (default 72)

## Notes
- TCPA API rate-limit: respect backoff (the code batches and throttles).
