# Backend deploy (ZIP upload)

## 1) Upload this folder as ZIP to your host (Render)
- Build command: `npm install`
- Start command: `npm start`

## 2) Set Environment Variables (Render -> Environment)
- STRIPE_SECRET_KEY = (your Stripe secret key, sk_live_...)
- (optional) STRIPE_WEBHOOK_SECRET = (whsec_...)
- PUBLIC_URL = https://YOUR_BACKEND.onrender.com
- PUBLIC_PORTAL_URL = https://YOUR_NETLIFY_SITE.netlify.app

## 3) Test
- https://YOUR_BACKEND.onrender.com/health  -> { ok:true }
- GET  /api/stripe/packs
- POST /api/stripe/create-checkout-session   { credits: 100, userId: "test" }

## Notes
- The secret key is NOT stored in files. It must be set in ENV.
- Price IDs are in server.js in CREDIT_PACKS.


Email verification (SMTP):
- SMTP_HOST
- SMTP_PORT
- SMTP_USER (Titkosvagy@titkosvagy.com)
- SMTP_PASS
- SMTP_SECURE (true for 465)
- MAIL_FROM (Titkosvagy@titkosvagy.com)
- PUBLIC_PORTAL_URL=https://titkosvagy.netlify.app
