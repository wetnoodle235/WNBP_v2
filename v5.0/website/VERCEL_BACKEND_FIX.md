# Fixing 502 Backend Gateway Errors on Vercel

## Problem
The Vercel deployment at wnbp.vercel.app is returning 502 errors for API endpoints like `/api/auth/me`, `/api/sports/nba`, etc.

**Root Cause:** The backend service URL (`PLATFORM_BACKEND_URL`) is not set in Vercel environment variables, so the frontend defaults to `http://127.0.0.1:8000` (localhost), which is unreachable from Vercel's servers.

## Quick Fix (Choose One)

### Option 1: Local Backend + Cloudflare Tunnel (Development/Testing)

1. **Start the backend locally:**
```bash
cd /home/derek/Documents/stock/v5.0/backend
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

2. **Expose via Cloudflare tunnel:**
```bash
cloudflared tunnel create wnbp-backend
# Get the public URL (e.g., https://something.trycloudflare.com)
```

3. **Configure Vercel:**
   - Go to https://vercel.com/dashboard
   - Select project "wnbp"
   - Settings → Environment Variables
   - Add: `PLATFORM_BACKEND_URL` = `https://your-cloudflare-url`
   - Redeploy

### Option 2: Deploy Backend to Production Service

Use one of:
- Railway.app
- Render.com
- Fly.io
- AWS/GCP/Azure

Then add the deployed URL to Vercel as above.

### Option 3: API Proxy Mode (Quick Workaround)

If you want to test without external deployment:
1. Run backend on port 8000 locally
2. Keep Next.js dev server running on port 3000
3. Vercel will use localhost (broken in prod) but local dev works

## Verification

After configuration:
```bash
# Test the API is accessible
curl https://wnbp.vercel.app/api/auth/me -H "Authorization: Bearer <token>"
# Should return JSON, not 502
```

## Files Involved
- Frontend expects: `PLATFORM_BACKEND_URL` environment variable
- Falls back to: `http://127.0.0.1:8000` (localhost - fails in Vercel)
- Location: `v5.0/website/src/lib/api-base.ts`

## Status
- ✅ Frontend deployed to Vercel
- ❌ Backend URL not configured
- ❌ API endpoints returning 502

Next: Choose deployment method above and configure the URL in Vercel dashboard
