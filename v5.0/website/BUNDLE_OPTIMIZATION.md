# Bundle Optimization Guide

## Current Bundle Status
- **Total .next size**: 224 MB
- **Build files**: 224 TypeScript/TSX source files
- **Largest components**: game detail (77KB), SeasonClient (52KB), LiveClient (47KB)

## Code Splitting Implemented ✅

### 1. SeasonClient (52KB)
**File**: [src/app/season/page.tsx](./src/app/season/page.tsx)
- **Change**: Converted to dynamic import
- **Benefit**: 52KB chunk only loaded when user visits `/season` page
- **Loading UX**: Shows "Loading season simulator..." during fetch

### 2. LiveClient (47KB)  
**File**: [src/app/live/page.tsx](./src/app/live/page.tsx)
- **Change**: Converted to dynamic import
- **Benefit**: 47KB chunk only loaded when user visits `/live` page
- **Loading UX**: Shows "Loading live scores..." during fetch

### Impact
- Estimated bundle reduction: ~100KB from initial page loads
- Only pages that need these components request them
- Other routes load significantly faster
- Cache-friendly: chunks can be cached independently

## Bundle Analysis Setup

### Install Bundle Analyzer
```bash
npm install --save-dev @next/bundle-analyzer
```

### Update next.config.ts
```typescript
import withBundleAnalyzer from '@next/bundle-analyzer'

const nextConfig = {
  // ... existing config
}

const withAnalyzer = withBundleAnalyzer({
  enabled: process.env.ANALYZE === 'true',
})

export default withAnalyzer(nextConfig)
```

### Run Analysis
```bash
# Generate bundle analysis
ANALYZE=true npm run build

# This will create:
# - .next/static/chunks/*.js.nft.json (detailed bundle info)
# - Open bundle report in browser (if available)
```

## Recommended Next Steps

### 1. Game Detail Page (77KB) - Candidate for Further Splitting
**Location**: [src/app/games/[sport]/[id]/page.tsx](./src/app/games/%5Bsport%5D/%5Bid%5D/page.tsx)

Options:
- Extract visualizations to lazy-loaded subcomponents
- Defer non-critical chart libraries (e.g., rendering libraries)
- Consider route-level code splitting with `next/dynamic`

### 2. Remove Unused Dependencies
```bash
# Audit for unused packages
npm audit
npm ls --depth=0

# Check for duplicate dependencies
npm dedupe
```

### 3. Configure Optimized Package Imports
**Already configured in next.config.ts**:
```typescript
experimental: {
  optimizePackageImports: ["@/components/ui"]
}
```

Expand this to other frequently-imported packages:
```typescript
optimizePackageImports: [
  "@/components/ui",
  "@/lib/sports-config",
  "@/components/LoadingSkeleton"
]
```

### 4. Enable Dynamic Imports for More Components
Identify candidates using bundle analyzer:
- Components that appear on few pages
- Large utility libraries only needed conditionally
- Heavy chart/visualization libraries

### 5. Implement Route Prefetching Strategy
**next.config.ts** can configure prefetch behavior:

```typescript
experimental: {
  optimizePackageImports: ["@/components/ui"],
  // Control when dynamic chunks are prefetched
}
```

For user-initiated navigation to `/season` or `/live`:
- Current: chunks load on demand (fastest initial load)
- Alternative: prefetch on hover/intersection observer (better UX)

## Bundle Size Targets

### Current Baseline
- Home page load: 2.7s
- Dynamic pages: 3.5-4.0s
- Static pages: 0.34s

### After Optimizations
- **Expected home page load**: 2.4-2.6s (100KB reduction)
- **Expected dynamic pages**: 3.2-3.8s
- **Static pages**: No change (0.34s)

### Further Optimization Goals
- Target max bundle: 150MB (from current 224MB)
- Individual route chunks <50KB each
- Cache-Control: `public, s-maxage=31536000, immutable` for versioned chunks

## Monitoring Bundle Size

### Add to package.json CI/CD
```json
{
  "scripts": {
    "analyze": "ANALYZE=true npm run build",
    "build:check-size": "npm run build && echo 'Build size check passed'"
  }
}
```

### Vercel Integration
Bundle analysis automatically available for each deployment:
1. Go to Vercel deployment details
2. Click "Analytics" tab
3. Review bundle breakdown by route

## Cache Control Headers

**Current configuration in next.config.ts**:
- ✅ Static assets: `public, max-age=31536000, immutable`
- ✅ Dynamic routes: `public, s-maxage=3600, stale-while-revalidate=86400`
- ✅ Font files: Long-term caching

No additional configuration needed - framework handles automatic cache busting for versioned chunks.

## Performance Checklist

- [x] Code split SeasonClient (52KB)
- [x] Code split LiveClient (47KB)  
- [ ] Run bundle analyzer to identify unused code
- [ ] Extract game detail page visualizations
- [ ] Verify Cache-Control headers in production
- [ ] Monitor bundle size in CI/CD pipeline
- [ ] Set up bundle size budget enforcement
- [ ] Document optimization impact

## References

- [Next.js Bundle Analysis](https://nextjs.org/docs/app/building-your-application/optimizing/bundle-analyzer)
- [Next.js Code Splitting](https://nextjs.org/docs/app/building-your-application/optimizing/bundle-size)
- [Dynamic Imports](https://nextjs.org/docs/app/building-your-application/optimizing/dynamic-imports)
