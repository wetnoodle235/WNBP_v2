# FINAL FIX: Configure Vercel with Backend URL

## ✅ Current Status - Ready to Deploy

### What's Running
- **Backend**: FastAPI running on `http://localhost:8000` ✓
- **Backend Process PID**: 776787
- **Cloudflare Tunnel**: Active and public ✓  
- **Tunnel URL**: `https://salmon-driven-frozen-winter.trycloudflare.com`

### The Fix
Add the tunnel URL to Vercel environment variables:

```bash
cd /home/derek/Documents/stock/v5.0/website

# Option 1: Using Vercel CLI (Recommended)
vercel env add PLATFORM_BACKEND_URL https://salmon-driven-frozen-winter.trycloudflare.com

# Option 2: Manual via Vercel Dashboard
# 1. Go to: https://vercel.com/dashboard
# 2. Click on "wnbp" project
# 3. Settings → Environment Variables
# 4. Click "Add New"
# 5. Name: PLATFORM_BACKEND_URL
# 6. Value: https://salmon-driven-frozen-winter.trycloudflare.com
# 7. Environment: Production (check all environments if you want)
# 8. Click "Save"

# Then redeploy:
vercel deploy --prod --yes
```

## Verification After Configuration

```bash
# Check the fix worked
curl https://wnbp.vercel.app/api/auth/me -H "Content-Type: application/json"

# Expected: JSON response (not 502 error)
# Example: {"success": false, "detail": "..."}  (OK, just checking connectivity)
```

## If Tunnel Expires
Cloudflare quick tunnels last ~3 hours. If `salmon-driven-frozen-winter.trycloudflare.com` expires:

```bash
# Create new tunnel
/home/derek/.local/bin/cloudflared tunnel --url http://localhost:8000 --no-autoupdate

# Get new URL from output 
# Update Vercel environment variable with the new URL
vercel env add PLATFORM_BACKEND_URL https://new-tunnel-url-here
vercel deploy --prod
```

## For Production Deployment
Instead of temporary Cloudflare tunnels, deploy backend to:
- Railway: https://railway.app
- Render: https://render.com
- Fly.io: https://fly.io
- AWS/GCP/Azure

Then use that permanent URL.

## Files Modified/Created
- ✅ `BACKEND_502_FIX.md` - Detailed fix guide
- ✅ `VERCEL_BACKEND_FIX.md` - Configuration options
- ✅ Backend running and tested
- ✅ Cloudflare tunnel active and tested
- ⏳ Vercel environment needs manual configuration

## Next Steps (2-3 minutes)
1. Run vercel env add command above
2. Run vercel deploy --prod
3. Wait 2-3 minutes for Vercel to rebuild
4. Visit https://wnbp.vercel.app and test sign-in
5. Check Network tab in DevTools - /api/auth/me should return 200, not 502

---

## Tunnel Status
- URL: https://salmon-driven-frozen-winter.trycloudflare.com
- Backend: http://localhost:8000
- Status: ✅ ACTIVE
- Tested: Yes, backend responds to /health
