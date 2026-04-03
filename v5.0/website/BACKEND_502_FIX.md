# 502 Backend Gateway Error - Fix Guide

## Current Status ✅
- **Backend:** Running locally on `http://localhost:8000` (Process PID: 776787)
- **Cloudflare Tunnels:** Active (Multiple tunnels available)
- **Frontend:** Deployed to `https://wnbp.vercel.app`
- **Issue:** Frontend can't reach backend - default fallback is localhost, which is unreachable from Vercel

## Root Cause
The `PLATFORM_BACKEND_URL` environment variable is not set in Vercel, so the frontend defaults to `http://127.0.0.1:8000` which:
- Works locally ✓ 
- Fails on Vercel ✗ (returns 502)

## Solution: Configure Vercel Environment Variable

### Step 1: Get the Backend Public URL

**Option A: Use existing Cloudflare tunnel** (Recommended for testing)
- Run command to get tunnel URL:
```bash
ps aux | grep cloudflared | grep -v grep | head -1
```
- Look for URLs in format: `https://xxxxx.trycloudflare.com`

**Option B: Create new tunnel** (If existing ones are expired)
```bash
/home/derek/.local/bin/cloudflared tunnel --url http://localhost:8000
# Outputs: https://xxxx-xxxx-xxxx.trycloudflare.com
```

**Option C: Deploy backend permanently**
- Railway: https://railway.app
- Render: https://render.com
- Fly.io: https://fly.io

### Step 2: Configure Vercel via CLI

```bash
cd /home/derek/Documents/stock/v5.0/website

# Set environment variable
vercel env add PLATFORM_BACKEND_URL https://your-tunnel-url-here

# Or manually via Vercel dashboard:
# 1. Go to https://vercel.com/dashboard
# 2. Select "wnbp" project
# 3. Settings → Environment Variables
# 4. Add: PLATFORM_BACKEND_URL = https://your-url
# 5. Production environment
```

### Step 3: Redeploy

```bash
vercel deploy --prod
```

### Step 4: Verify Fix

```bash
# Test auth endpoint
curl https://wnbp.vercel.app/api/auth/me

# Should return JSON, not 502 error
```

## Environment Variables Chain (from code)
Priority order in `src/lib/api-base.ts`:
1. `PLATFORM_BACKEND_URL` ← **SET THIS**
2. `BACKEND_URL`
3. `API_URL`
4. `NEXT_PUBLIC_API_URL`
5. `http://127.0.0.1:8000` (fallback - fails on Vercel)

## Testing Locally

The local setup works because Next.js dev server (`localhost:3000`) can reach backend (`localhost:8000`):
```bash
# Terminal 1: Start backend
cd /home/derek/Documents/stock/v5.0/backend
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000

# Terminal 2: Start frontend
cd /home/derek/Documents/stock/v5.0/website
npm run dev

# Browser: http://localhost:3000 (works - APIs resolve to localhost:8000)
```

## Production Setup

For production (`https://wnbp.vercel.app`), Vercel needs:
- `PLATFORM_BACKEND_URL` = publicly accessible URL (not localhost)
- Backend running at that URL
- CORS headers properly configured

## Files Involved

| File | Purpose |
|------|---------|
| `src/lib/api-base.ts` | Resolves backend URL from env vars |
| `src/app/api/auth/me/route.ts` | Uses resolved URL to proxy requests |
| `src/app/api/sports/[sport]/route.ts` | Same pattern for other endpoints |
| `next.config.ts` | Rewrites `/api/proxy/*` to backend |

## Quick Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `502 Bad Gateway` | Backend URL not accessible | Set `PLATFORM_BACKEND_URL` |
| API works locally | Default localhost:8000 works | Deploy backend or use tunnel |
| Auth shows undefined | Backend returning error | Check `/api/auth/me` response |
| 401 Unauthorized | Valid auth issue | Check JWT tokens |

## Next Steps

1. **Recommended:** Use existing Cloudflare tunnel with the running backend
2. Get the tunnel URL from running process
3. Set `PLATFORM_BACKEND_URL` in Vercel
4. Redeploy
5. Verify API calls work

The backend is ready - just needs public URL configured in Vercel!
