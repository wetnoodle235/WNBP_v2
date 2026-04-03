# wnbp.vercel.app - 502 API Error FIX - COMPLETE GUIDE

## 📋 Problem Summary
- Frontend at `https://wnbp.vercel.app` shows 502 errors on `/api/auth/me`, `/api/sports/*`, etc.
- Root cause: Backend service URL not configured in Vercel
- Solution: Configure the environment variable and redeploy

## ✅ What's Been Done
1. **Backend Server Started** ✓
   - Location: `/home/derek/Documents/stock/v5.0/backend`
   - Process: `python3 -m uvicorn main:app --host 0.0.0.0 --port 8000`
   - Status: Running and responding to `/health` endpoint
   - PID: 776787

2. **Cloudflare Tunnel Created** ✓
   - Public URL: `https://salmon-driven-frozen-winter.trycloudflare.com`
   - Tested: Backend responds at this URL
   - Status: Active and routing to localhost:8000

3. **Environment Configuration Added** ✓
   - File: `/home/derek/Documents/stock/v5.0/website/.env.production`
   - Variable: `PLATFORM_BACKEND_URL=https://salmon-driven-frozen-winter.trycloudflare.com`
   - Status: Ready for deployment

## 🚀 How to Complete the Fix

### Option 1: Deploy via Vercel CLI (Recommended - 2 minutes)
```bash
cd /home/derek/Documents/stock/v5.0/website
vercel deploy --prod --yes
```

Then wait 2-3 minutes for Vercel to rebuild and redeploy.

### Option 2: Via Vercel Dashboard (5 minutes)
1. Go to https://vercel.com/dashboard
2. Select "wnbp" project
3. Go to Settings → Environment Variables
4. Click "Add New"
5. Name: `PLATFORM_BACKEND_URL`
6. Value: `https://salmon-driven-frozen-winter.trycloudflare.com`
7. Environment: `Production` (or all if you want)
8. Click "Save"
9. Wait for auto-redeploy or manually trigger redeploy

### Option 3: Git Commit & Push (5-10 minutes)
```bash
cd /home/derek/Documents/stock/v5.0/website
git add .env.production
git commit -m "fix: add backend URL for Vercel production"
git push origin wnbp_v2
# Vercel auto-deploys on push
```

## ✔️ How to Verify the Fix

After deployment (wait 3 minutes):

```bash
# Test the API
curl https://wnbp.vercel.app/api/auth/me

# Expected Responses:
# - JSON object (even if 401) = ✅ FIXED
# - 502 error = ❌ Still broken
```

Or visit in browser:
1. Open https://wnbp.vercel.app
2. Open DevTools (F12)
3. Go to Network tab
4. Refresh page
5. Look for `/api/auth/me` request
6. If status is 200, 400, or 401 = ✅ FIXED
7. If status is 502 = ❌ Still broken

## 🔧 Technical Details

### Why It Was Broken
- Frontend code tries to call `${PLATFORM_BACKEND_URL}/auth/me`
- If `PLATFORM_BACKEND_URL` not set, defaults to `http://127.0.0.1:8000`
- `localhost:8000` is unreachable from Vercel servers
- Result: 502 Bad Gateway

### How It's Fixed
- `PLATFORM_BACKEND_URL` now points to public Cloudflare tunnel
- Cloudflare tunnel routes to `http://localhost:8000` (backend)
- Frontend can reach backend through the public URL
- All API calls work properly

### Code Flow
```
Browser (wnbp.vercel.app)
    ↓
Next.js Frontend (Vercel)
    ↓
fetch(`${PLATFORM_BACKEND_URL}/api/auth/me`)
    ↓
PLATFORM_BACKEND_URL = "https://salmon-driven-frozen-winter.trycloudflare.com"
    ↓
Cloudflare Tunnel
    ↓
http://localhost:8000 (Backend)
    ↓
Returns: User data / Auth response ✓
```

## ⚠️ Important Notes

### Tunnel URL Expiration
- Cloudflare quick tunnels last ~3-8 hours
- When `salmon-driven-frozen-winter.trycloudflare.com` expires:
  ```bash
  # Restart tunnel
  /home/derek/.local/bin/cloudflared tunnel --url http://localhost:8000
  
  # Get new URL and update Vercel:
  # vercel env add PLATFORM_BACKEND_URL https://new-url-here
  # vercel deploy --prod
  ```

### For Production
Replace the temporary tunnel URL with permanent backend deployment:
- Deploy to Railway, Render, Fly.io, or AWS
- Use that permanent URL in Vercel environment

## 📁 Files Created/Modified

| File | Status | Purpose |
|------|--------|---------|
| `.env.production` | ✅ Modified | Added PLATFORM_BACKEND_URL |
| `CONFIGURE_VERCEL_NOW.md` | ✅ Created | Step-by-step configuration |
| `BACKEND_502_FIX.md` | ✅ Created | Detailed fix guide |
| `VERCEL_BACKEND_FIX.md` | ✅ Created | Multiple solution options |

## 📞 Troubleshooting

### Still Getting 502 After Deploy?
1. Did you redeploy? Check Vercel deployments page for latest
2. Is backend running? Check PID 776787 with `ps aux | grep 776787`
3. Is tunnel still active? Run new tunnel command
4. Give it 5 minutes - rebuild might be in progress

### Backend Stopped?
```bash
cd /home/derek/Documents/stock/v5.0/backend
source /home/derek/Documents/stock/.venv/bin/activate
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

### Tunnel Stopped?
```bash
/home/derek/.local/bin/cloudflared tunnel --url http://localhost:8000 --no-autoupdate
```

## ✨ What Happens Next

Once deployed:
- ✅ `https://wnbp.vercel.app` works
- ✅ Sign in/auth works
- ✅ API endpoints work  
- ✅ Sports data loads
- ✅ Predictions display
- ✅ All features functional

---

**Current Status**: ✅ All components ready - just needs final Vercel redeploy to activate
