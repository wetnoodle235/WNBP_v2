# Vercel Environment Configuration for Backend API

## Problem Identified
API endpoints are returning **502 Bad Gateway** errors because the backend service URL is not properly configured on Vercel deployment.

**Current Issue:**
- `/api/sports/nba` → 502 error
- `/api/predictions/nba` → 502 error
- Falls back to `http://127.0.0.1:8000` (local development) when env vars missing

## Solution: Configure Backend URL on Vercel

The application uses this environment variable resolution chain:
1. `PLATFORM_BACKEND_URL` (highest priority)
2. `BACKEND_URL`
3. `API_URL`
4. `NEXT_PUBLIC_API_URL`
5. `http://127.0.0.1:8000` (fallback - development only)

### Steps to Fix

1. **Log into Vercel Dashboard**
   - Go to [vercel.com/dashboard](https://vercel.com/dashboard)
   - Select your project (e.g., "wnbp")

2. **Navigate to Environment Variables**
   - Click "Settings" → "Environment Variables"

3. **Add the Backend URL**
   Choose the most appropriate option for your deployment:

   **Option A: Using Cloudflare Worker (Recommended)**
   ```
   Name: PLATFORM_BACKEND_URL
   Value: https://sportstock-api.<your-cloudflare-domain>.workers.dev
   Environment: Production, Preview, Development
   ```

   **Option B: Using Direct Backend URL**
   ```
   Name: PLATFORM_BACKEND_URL
   Value: https://your-backend-service.example.com
   Environment: Production, Preview, Development
   ```

4. **Redeploy On Vercel**
   - Go to "Deployments"
   - Select the latest deployment
   - Click "Redeploy"
   - Wait for rebuild to complete

5. **Verify the Fix**
   - Navigate to `/api/sports/nba` on your deployed site
   - Should return JSON data instead of 502 error
   - Check browser DevTools → Network tab for successful responses

## Environment Variables Reference

| Variable | Priority | Scope | Purpose |
|----------|----------|-------|---------|
| `PLATFORM_BACKEND_URL` | 1 (Highest) | Server-side only | Primary backend service URL |
| `BACKEND_URL` | 2 | Server-side only | Fallback backend URL |
| `API_URL` | 3 | Server-side only | Alternative API endpoint |
| `NEXT_PUBLIC_API_URL` | 4 | Public (visible in client) | Last resort, not recommended for secrets |

## File References

- **Configuration**: [next.config.ts](./next.config.ts#L86-L100) - Rewrites configuration
- **API Base Resolver**: [src/lib/api-base.ts](./src/lib/api-base.ts) - Resolution logic
- **API Route Example**: [src/app/api/sports/[sport]/route.ts](./src/app/api/sports/%5Bsport%5D/route.ts#L1-L10)

## Testing

After configuration:

```bash
# Test from development with env var set
PLATFORM_BACKEND_URL=https://your-backend.com npm run dev

# Test production deployment
curl https://wnbp.vercel.app/api/sports/nba
# Should return valid sports data, not 502 error
```

## Performance Impact

✅ **Expected improvement after fix:**
- API endpoints will respond with actual data
- Pages dependent on `/api/sports/*` endpoints will render correctly
- Predictions and live scores will load data properly
- Overall page load time may improve as backend queries complete successfully

## Troubleshooting

### Still getting 502 errors?
1. Verify the backend URL is publicly accessible
2. Check backend service is running
3. Confirm network allows connections from Vercel IPs
4. Check backend logs for connection/permission errors

### Backend URL shows in client JavaScript?
- Only set `NEXT_PUBLIC_API_URL` if necessary
- Prefer `PLATFORM_BACKEND_URL` (server-side only, more secure)
- Check CSP headers in [next.config.ts](./next.config.ts#L60-L70) allow connections

### Environment variable not taking effect?
- Ensure "All Environments" are selected when adding variable
- Redeploy after adding/changing environment variables
- Clear browser cache and hard refresh
- Check `process.env` values in API route logs
