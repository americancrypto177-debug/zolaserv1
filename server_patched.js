import 'dotenv/config';
import http from 'http';
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import multer from 'multer';
import { Server as SocketIOServer } from 'socket.io';
import { nanoid } from 'nanoid';
import Stripe from 'stripe';
import path from 'path';
import fs from 'fs/promises';
import crypto from 'crypto';
// Storage helpers are inlined to avoid missing-file deploy issues on Render.
// (Keeping lib/storage.js in the repo is still fine, but server.js does not depend on it.)

function dataPath(rel = '') {
  return path.join(process.cwd(), 'data', rel);
}

async function readJson(filename, fallback) {
  try {
    const file = dataPath(filename);
    const raw = await fs.readFile(file, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    if (e && e.code === 'ENOENT') return fallback;
    throw e;
  }
}

async function writeJson(filename, obj) {
  const file = dataPath(filename);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, JSON.stringify(obj, null, 2), 'utf8');
  return obj;
}

async function updateJson(filename, fallback, updater) {
  const cur = await readJson(filename, fallback);
  const next = await updater(cur);
  await writeJson(filename, next);
  return next;
}


const PORT = Number(process.env.PORT || 3001);
const PUBLIC_URL = process.env.PUBLIC_URL || '';

const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

// Credit packs (Stripe PRICE IDs)
const CREDIT_PACKS = [
  { credits: 100,  priceId: 'price_1SlsEBEAcUmosVgk7FDZPejh' },
  { credits: 200,  priceId: 'price_1SlsM4EAcUmosVgktdb14FOA' },
  { credits: 400,  priceId: 'price_1SlsSjEAcUmosVgkUXLg6F2f' },
  { credits: 600,  priceId: 'price_1SlsVOEAcUmosVgk9rYPohCW' },
  { credits: 1000, priceId: 'price_1SlsX6EAcUmosVgkZBVOsck7' },
  { credits: 1500, priceId: 'price_1SlsZ5EAcUmosVgkLsqfGPPc' }
];

const CREDITS_FILE = 'credits.json';
const CREDITS_LEDGER_FILE = 'credits_ledger.json';
const STRIPE_APPLIED_FILE = 'stripe_applied.json';
// Portal users store (must be defined before top-level admin seeding runs)
const PORTAL_USERS_FILE = 'portal_users.json';
async function getCredits(userId){
  const store = await readJson(CREDITS_FILE, {});
  return Number(store[userId] || 0);
}
async function addCredits(userId, amount){
  if(!userId) return;
  const store = await readJson(CREDITS_FILE, {});
  store[userId] = Number(store[userId] || 0) + Number(amount || 0);
  await writeJson(CREDITS_FILE, store);
}

async function _loadLedger(){
  const cur = await readJson(CREDITS_LEDGER_FILE, { used: {} });
  cur.used = (cur && typeof cur.used === 'object' && cur.used) ? cur.used : {};
  return cur;
}
async function _markLedger(key){
  const cur = await _loadLedger();
  cur.used[key] = now();
  const keys = Object.keys(cur.used);
  if(keys.length > 50000){
    keys.sort((a,b)=> (cur.used[a]||0) - (cur.used[b]||0));
    for(let i=0;i<keys.length-40000;i++) delete cur.used[keys[i]];
  }
  await writeJson(CREDITS_LEDGER_FILE, cur);
}
async function debitCredits(userId, amount, reasonId){
  const uid = lower(userId);
  const amt = Number(amount||0);
  const rid = safeStr(reasonId||'').trim();
  if(!uid || !amt) return { ok:false, err:'missing' };
  const key = sha256hex('D|' + uid + '|' + rid);
  if(rid){
    const ledger = await _loadLedger();
    if(ledger.used && ledger.used[key]) return { ok:true, already:true, credits: await getCredits(uid) };
  }
  const store = await readJson(CREDITS_FILE, {});
  const have = Number(store[uid] || 0);
  if(have < amt) return { ok:false, err:'insufficient', credits: have };
  store[uid] = have - amt;
  await writeJson(CREDITS_FILE, store);
  if(rid) await _markLedger(key);
  return { ok:true, debited: amt, credits: Number(store[uid]||0) };
}
async function creditOnce(userId, amount, reasonId){
  const uid = lower(userId);
  const amt = Number(amount||0);
  const rid = safeStr(reasonId||'').trim();
  if(!uid || !amt) return { ok:false, err:'missing' };
  const key = sha256hex('C|' + uid + '|' + rid);
  if(rid){
    const ledger = await _loadLedger();
    if(ledger.used && ledger.used[key]) return { ok:true, already:true, credits: await getCredits(uid) };
  }
  await addCredits(uid, amt);
  if(rid) await _markLedger(key);
  return { ok:true, credited: amt, credits: await getCredits(uid) };
}
async function _loadStripeApplied(){
  const cur = await readJson(STRIPE_APPLIED_FILE, { applied: {} });
  cur.applied = (cur && typeof cur.applied === 'object' && cur.applied) ? cur.applied : {};
  return cur;
}
async function _markStripeApplied(sessionId){
  const cur = await _loadStripeApplied();
  cur.applied[String(sessionId)] = now();
  await writeJson(STRIPE_APPLIED_FILE, cur);
}
async function stripeCreditOnce(userId, credits, sessionId){
  const sid = String(sessionId||'').trim();
  if(!sid) return { ok:false, err:'missing_session' };
  const applied = await _loadStripeApplied();
  if(applied.applied && applied.applied[sid]) return { ok:true, alreadyApplied:true, credits: await getCredits(userId) };
  const r = await creditOnce(userId, credits, 'stripe:'+sid);
  await _markStripeApplied(sid);
  return Object.assign({}, r, { alreadyApplied:false });
}


const UPLOAD_DIR = path.join(process.cwd(), 'uploads');
const HEARTBEAT_MS = 8000; // clients already ping every 8s
// 15s TTL can be too aggressive in production (mobile / Wi‑Fi jitter) and may cause
// false "offline" -> reassignment -> "elugró / vibráló" üzenetek. Give a wider grace.
const ONLINE_TTL_MS = 45000; // UI considers online if lastSeen < 45s

function now(){ return Date.now(); }
function lower(s){ return String(s||'').trim().toLowerCase(); }
function safeStr(s){ return String(s||''); }
function clampStr(s, max=2000){
  const x = safeStr(s);
  return x.length > max ? x.slice(0, max) : x;
}
function sha256hex(s){
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}
function publicAbs(p){
  if(!p) return '';
  if(/^https?:\/\//i.test(p)) return p;
  return PUBLIC_URL ? (PUBLIC_URL.replace(/\/$/, '') + p) : p;
}

async function ensureFiles(){
  await fs.mkdir(dataPath(''), { recursive: true });
  await fs.mkdir(UPLOAD_DIR, { recursive: true });

  // Profiles: if profiles.json is missing or empty, bootstrap from profiles_seed.json
  // so supervisor trigger dropdown works even when the portal and moderáció are on
  // different origins (localStorage is not shared).
  let seededProfiles = { profiles: [] };
  try{
    const seedRaw = await fs.readFile(dataPath('profiles_seed.json'), 'utf8');
    const parsed = JSON.parse(seedRaw);
    if(parsed && Array.isArray(parsed.profiles)) seededProfiles = { profiles: parsed.profiles };
  }catch(e){ /* optional */ }

  const seeds = [
    ['threads.json', { threads: {}, assignments: {}, queue: [] }],
    ['audit.json', { entries: [] }],
    ['extra_users.json', { users: [] }],
    ['bans.json', { operators: {} }],
    ['internal_chat.json', { messages: [] }],
    ['users.json', { users: [] }],
    ['portal_users.json', { users: [] }],
    ['credits.json', {}],
    ['credits_ledger.json', { used: {} }],
    ['stripe_applied.json', { applied: {} }],
    ['profiles.json', seededProfiles]
  ];
  for(const [f, v] of seeds){
    const cur = await readJson(f, null);
    if(cur === null) await writeJson(f, v);
  }

  // If profiles.json exists but empty, also seed it.
  try{
    const cur = await readJson('profiles.json', { profiles: [] });
    if(cur && Array.isArray(cur.profiles) && cur.profiles.length === 0 && seededProfiles.profiles.length){
      await writeJson('profiles.json', seededProfiles);
    }
  }catch(e){ /* ignore */ }
}

await ensureFiles();

// Seed portal admin account (for testing/admin use on the portal)
async function ensurePortalAdmin(){
  const email = 'admin@forrovagy.hu';
  const password = 'ForroVagy2000!';
  const cur = await loadPortalUsers().catch(()=>({ users: [] }));
  cur.users = Array.isArray(cur.users) ? cur.users : [];
  let u = cur.users.find(x => normEmail(x.email) === normEmail(email));
  if(!u){
    const id = 'u_' + nanoid(10);
    const { salt, hash } = hashPassword(password);
    u = {
      id,
      email,
      nick: 'Admin',
      city: '',
      county: '',
      birthYear: '',
      gender: '',
      profilePic: '',
      transport: '',
      hobbies: '',
      work: '',
      about: '',
      relationship: '',
      passSalt: salt,
      passHash: hash,
      createdAt: now()
    };
    cur.users.push(u);
    await savePortalUsers(cur);
  }
  const currentCredits = await getCredits(u.id);
  if(Number(currentCredits||0) < 2000){
    await addCredits(u.id, 2000 - Number(currentCredits||0));
  }
}
await ensurePortalAdmin();

function buildPublicUrl(p){
  if(!p) return '';
  if(/^https?:\/\//i.test(p)) return p;
  if(PUBLIC_URL){
    return PUBLIC_URL.replace(/\/$/,'') + (p.startsWith('/')?p:'/'+p);
  }
  return p;
}


const app = express();
app.set('trust proxy', 1);
app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(cors({ origin: true, credentials: true }));
app.use((req, res, next) => {
  if (req.originalUrl === '/api/stripe/webhook') return next();
  return express.json({ limit: '5mb' })(req, res, next);
});
app.use((req, res, next) => {
  if (req.originalUrl === '/api/stripe/webhook') return next();
  return express.urlencoded({ extended: true })(req, res, next);
});

app.get('/health', (_req, res) => res.json({ ok: true, ts: now() }));

// ---------------- Stripe API ----------------
app.get('/api/stripe/packs', async (_req, res) => {
  try{
    if(!stripe) return res.json({ ok:false, err:'stripe_not_configured' });
    const packs = [];
    for(const p of CREDIT_PACKS){
      const price = await stripe.prices.retrieve(p.priceId);
      packs.push({
        credits: p.credits,
        priceId: p.priceId,
        currency: price.currency,
        unit_amount: price.unit_amount,
        amount: price.unit_amount
      });
    }
    res.json({ ok: true, packs });
  }catch(e){
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/stripe/create-checkout-session', async (req, res) => {
  try{
    if(!stripe) return res.json({ ok:false, err:'stripe_not_configured' });
    const { credits, userId } = req.body || {};
    const pack = CREDIT_PACKS.find(x => x.credits === Number(credits));
    if(!pack) return res.status(400).json({ ok: false, error: 'Invalid credits pack' });

    const origin = safeStr(req.body?.returnBase).trim() || req.headers.origin || process.env.PUBLIC_PORTAL_URL || 'http://localhost:7707';
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      line_items: [{ price: pack.priceId, quantity: 1 }],
      client_reference_id: String(userId || ''),
      metadata: { userId: String(userId || ''), credits: String(pack.credits) },
      success_url: `${origin}/payment_success.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url:  `${origin}/payment_cancel.html`
    });

    res.json({ ok: true, url: session.url });
  }catch(e){
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/api/stripe/checkout-session', async (req, res) => {
  try{
    if(!stripe) return res.json({ ok:false, err:'stripe_not_configured' });
    const sid = req.query.session_id;
    if(!sid) return res.status(400).json({ ok: false, error: 'Missing session_id' });

    const session = await stripe.checkout.sessions.retrieve(String(sid));
    const userId = lower(session?.metadata?.userId || session?.client_reference_id || '');
    const credits = Number(session?.metadata?.credits || 0);

    let creditsAdded = 0;
    let creditsTotal = 0;
    let alreadyApplied = false;

    const paid = (session?.payment_status === 'paid') || (session?.status === 'complete');
    if(userId && credits > 0 && paid){
      const r = await stripeCreditOnce(userId, credits, session.id);
      creditsTotal = Number(r.credits || 0);
      alreadyApplied = !!r.alreadyApplied || !!r.already || false;
      creditsAdded = alreadyApplied ? 0 : Number(r.credited || credits);
      io.emit('credits:update', { userId, credits: creditsTotal });
    }else if(userId){
      creditsTotal = await getCredits(userId);
    }

    res.json({ ok: true, session, creditsAdded, creditsTotal, alreadyApplied });
  }catch(e){
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Webhook (optional). If STRIPE_WEBHOOK_SECRET is set, signature is verified.
// Stripe Terminal (optional)
app.post('/api/stripe/terminal/connection-token', async (_req, res) => {
  try{
    if(!stripe) return res.status(400).json({ ok:false, error:'Stripe nincs beállítva.' });
    const token = await stripe.terminal.connectionTokens.create();
    res.json({ ok:true, secret: token.secret });
  }catch(e){
    res.status(500).json({ ok:false, error: e.message || 'Szerver hiba' });
  }
});

app.get('/api/stripe/terminal/locations', async (_req, res) => {
  try{
    if(!stripe) return res.status(400).json({ ok:false, error:'Stripe nincs beállítva.' });
    const locations = await stripe.terminal.locations.list({ limit: 100 });
    res.json({ ok:true, locations: locations.data || [] });
  }catch(e){
    res.status(500).json({ ok:false, error: e.message || 'Szerver hiba' });
  }
});

app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  try{
    if(!stripe) return res.status(200).json({ received: true });
    const sig = req.headers['stripe-signature'];
    let event = null;

    if(STRIPE_WEBHOOK_SECRET){
      event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
    } else {
      // No signature check (not recommended). Use only for quick local tests.
      event = JSON.parse(req.body.toString('utf8'));
    }

    if(event.type === 'checkout.session.completed'){
      const session = event.data.object;
      const userId = lower(session?.metadata?.userId || session?.client_reference_id || '');
      const credits = Number(session?.metadata?.credits || 0);
      const paid = (session?.payment_status === 'paid') || true; // completed event implies paid
      if(userId && credits > 0 && paid){
        const r = await stripeCreditOnce(userId, credits, session.id);
        io.emit('credits:update', { userId, credits: Number(r.credits||0) });
      }
    }

    res.json({ received: true });
  }catch(e){
    res.status(400).send(`Webhook Error: ${e.message}`);
  }
});

// Credits API
app.get('/api/credits', async (req, res) => {
  const userId = req.query.userId || '';
  res.json({ ok: true, userId, credits: await getCredits(String(userId)) });
});


// Static
app.use('/uploads', express.static(UPLOAD_DIR));
app.use(express.static(path.join(process.cwd(), 'public')));

const upload = multer({
  storage: multer.diskStorage({
    destination: async (_req, _file, cb) => {
      try{ await fs.mkdir(UPLOAD_DIR, { recursive: true }); }catch(e){}
      cb(null, UPLOAD_DIR);
    },
    filename: (_req, file, cb) => {
      const ext = (path.extname(file.originalname || '') || '').slice(0, 12);
      cb(null, `${Date.now()}_${nanoid(10)}${ext || ''}`);
    }
  }),
  limits: {
    fileSize: Number(process.env.UPLOAD_MAX_BYTES || (200 * 1024 * 1024)) // 200MB default
  }
});

app.post('/api/upload', upload.single('file'), (req, res) => {
  try{
    if(!req.file) return res.status(400).json({ ok: false, err: 'missing_file' });
    const rel = '/uploads/' + req.file.filename;
    const url = buildPublicUrl(rel);
    res.json({ ok: true, url, path: url, rel });
  }catch(e){
    res.status(500).json({ ok: false, err: 'upload_failed' });
  }
});

// ---- Data helpers ----
async function loadThreads(){
  return readJson('threads.json', { threads: {}, assignments: {}, queue: [] });
}
async function saveThreads(next){
  await writeJson('threads.json', next);
}
async function loadAudit(){
  return readJson('audit.json', { entries: [] });
}
async function appendAudit(entry){
  await updateJson('audit.json', { entries: [] }, (cur) => {
    cur.entries = Array.isArray(cur.entries) ? cur.entries : [];
    cur.entries.push(entry);
    // keep last 200k entries
    if(cur.entries.length > 200000) cur.entries = cur.entries.slice(-200000);
    return cur;
  });
}
async function loadExtraUsers(){
  return readJson('extra_users.json', { users: [] });
}
async function loadBans(){
  return readJson('bans.json', { operators: {} });
}
async function saveBans(next){
  await writeJson('bans.json', next);
}
async function loadInternalChat(){
  return readJson('internal_chat.json', { messages: [] });
}
async function saveInternalChat(next){
  await writeJson('internal_chat.json', next);
}
async function loadUsers(){
  return readJson('users.json', { users: [] });
}
async function saveUsers(next){
  await writeJson('users.json', next);
}
async function loadProfiles(){
  return readJson('profiles.json', { profiles: [] });
}
async function saveProfiles(next){
  await writeJson('profiles.json', next);
}

function splitThreadId(threadId){
  const parts = String(threadId||'').split('__');
  const userId = parts.shift() || '';
  const profileNick = parts.join('__') || '';
  return { userId, profileNick };
}

function dayKey(ts){
  const d = new Date(ts);
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,'0');
  const da = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${da}`;
}

const presence = { users: new Map(), operators: new Map() };

// ---- Portal auth (email verification) ----

function normEmail(s){ return String(s||'').trim().toLowerCase(); }
function token64(n=32){ return crypto.randomBytes(n).toString('base64url'); }

function hashPassword(password, salt){
  const s = salt || crypto.randomBytes(16).toString('base64url');
  const h = crypto.pbkdf2Sync(String(password||''), s, 120000, 32, 'sha256').toString('base64url');
  return { salt:s, hash:h };
}
function verifyPassword(password, salt, hash){
  try{
    const h = crypto.pbkdf2Sync(String(password||''), String(salt||''), 120000, 32, 'sha256').toString('base64url');
    return crypto.timingSafeEqual(Buffer.from(h), Buffer.from(String(hash||'')));
  }catch(e){ return false; }
}

async function loadPortalUsers(){
  const cur = await readJson(PORTAL_USERS_FILE, { users: [] });
  cur.users = Array.isArray(cur.users) ? cur.users : [];
  return cur;
}
async function savePortalUsers(next){
  next = next || { users: [] };
  next.users = Array.isArray(next.users) ? next.users : [];
  await writeJson(PORTAL_USERS_FILE, next);
  return next;
}

function getPortalBase(req){
  const envBase = String(process.env.PUBLIC_PORTAL_URL || process.env.PORTAL_URL || '').trim().replace(/\/+$/,'');
  const o = String(req.headers?.origin || '').trim().replace(/\/+$/,'');
  if(o && /^https?:\/\//i.test(o)) return o;
  if(envBase && /^https?:\/\//i.test(envBase)) return envBase;
  try{
    const proto = (req.headers['x-forwarded-proto']||'').toString().split(',')[0].trim() || 'https';
    const host  = (req.headers['x-forwarded-host']||req.headers.host||'').toString().split(',')[0].trim();
    if(host) return `${proto}://${host}`;
  }catch(e){}
  return '';
}

app.post('/api/auth/register', async (req, res) => {
  try{
    const email = normEmail(req.body?.email);
    const nick  = safeStr(req.body?.nick).trim();
    const password = safeStr(req.body?.password).trim();
    const city  = safeStr(req.body?.city).trim();
    const county= safeStr(req.body?.county).trim();
    const birthYear = safeStr(req.body?.birthYear).trim();
    const gender = safeStr(req.body?.gender).trim();
    const profilePic = safeStr(req.body?.profilePic).trim();

    const transport = safeStr(req.body?.transport).trim();
    const hobbies   = safeStr(req.body?.hobbies).trim();
    const work      = safeStr(req.body?.work).trim();
    const about     = safeStr(req.body?.about).trim();
    const relationship = safeStr(req.body?.relationship).trim();

    if(!email || !email.includes('@')) return res.status(400).json({ ok:false, error:'Érvénytelen email.' });
    if(!nick) return res.status(400).json({ ok:false, error:'Kérlek add meg a beceneved.' });
    if(String(password||'').length < 4) return res.status(400).json({ ok:false, error:'A jelszó túl rövid.' });

    const cur = await loadPortalUsers();
    const exists = cur.users.find(u => normEmail(u.email) === email);
    if(exists) return res.status(409).json({ ok:false, error:'Ezzel az email címmel már regisztráltak.' });

    const id = 'u_' + nanoid(10);
    const { salt, hash } = hashPassword(password);

    // Email küldés kikapcsolva: azonnal aktivált felhasználó
    const user = {
      id, email, nick,
      passSalt: salt, passHash: hash,
      city, county, birthYear, gender,
      profilePic,
      transport, hobbies, work, about, relationship,
      verified:true,
      verifyTokenHash: '',
      verifyTokenCreatedAt: 0,
      createdAt: now()
    };

    cur.users.push(user);
    await savePortalUsers(cur);

    const session = {
      id: user.id,
      email: user.email,
      nick: user.nick,
      city: user.city || '',
      county: user.county || '',
      birthYear: user.birthYear || '',
      gender: user.gender || '',
      profilePic: user.profilePic || '',
      transport: user.transport || '',
      hobbies: user.hobbies || '',
      work: user.work || '',
      about: user.about || '',
      relationship: user.relationship || '',
      role: 'user',
      ts: now()
    };

    return res.json({ ok:true, registered:true, session, credits: await getCredits(user.id) });
  }catch(e){
    res.status(500).json({ ok:false, error:'Szerver hiba.' });
  }
});

// Email aktiválás / újraküldés kikapcsolva
app.get('/api/auth/verify', async (_req, res) => {
  res.json({ ok:true, verified:true, note:'email_disabled' });
});

app.post('/api/auth/resend', async (_req, res) => {
  res.json({ ok:true, note:'email_disabled' });
});

app.post('/api/auth/login', async (req, res) => {
  try{
    const email = normEmail(req.body?.email);
    const password = safeStr(req.body?.password).trim();
    if(!email || !email.includes('@')) return res.status(400).json({ ok:false, error:'Érvénytelen email.' });

    const cur = await loadPortalUsers();
    const u = cur.users.find(x => normEmail(x.email) === email);
    if(!u) return res.status(401).json({ ok:false, error:'Hibás email vagy jelszó.' });
    if(!verifyPassword(password, u.passSalt || u.salt, u.passHash || u.hash)) return res.status(401).json({ ok:false, error:'Hibás email vagy jelszó.' });

    const session = {
      id: u.id,
      email: u.email,
      nick: u.nick,
      city: u.city || '',
      county: u.county || '',
      birthYear: u.birthYear || '',
      gender: u.gender || '',
      profilePic: u.profilePic || '',
      transport: u.transport || '',
      hobbies: u.hobbies || '',
      work: u.work || '',
      about: u.about || '',
      relationship: u.relationship || '',
      role: 'user',
      ts: now()
    };

    return res.json({ ok:true, session, credits: await getCredits(u.id) });
  }catch(e){
    res.status(500).json({ ok:false, error:'Szerver hiba.' });
  }
});

// ---- REST APIs used by existing UIs ----
app.get('/api/extra-users', async (_req, res) => {
  const eu = await loadExtraUsers();
  res.json({ ok: true, users: Array.isArray(eu.users) ? eu.users : [] });
});

app.post('/api/extra-users', async (req, res) => {
  const email = safeStr(req.body?.email).trim();
  const id = safeStr(req.body?.id).trim();
  const password = safeStr(req.body?.password);
  if(!email || !id || !password) return res.status(400).json({ ok: false, err: 'missing_fields' });

  await updateJson('extra_users.json', { users: [] }, (cur) => {
    cur.users = Array.isArray(cur.users) ? cur.users : [];
    const key = lower(email);
    if(cur.users.some(u => lower(u?.email) === key)){
      return cur; // already exists
    }
    cur.users.push({ email, id, role: 'operator', pwdPlain: password, createdAt: now() });
    return cur;
  });

  res.json({ ok: true });
});

// Operator ban check (login preflight)
app.get('/api/operator/is-banned', async (req, res) => {
  const email = lower(req.query?.email || '');
  if(!email) return res.json({ ok: true, banned: false });
  try{
    const bans = await loadBans();
    const it = bans.operators?.[email] || null;
    const permanent = !!(it && it.permanent);
    const until = (it && it.until) ? Number(it.until) : null;
    const banned = permanent || (!!until && now() < until);
    res.json({ ok: true, banned, permanent, until });
  }catch(e){
    res.status(500).json({ ok:false, error:'ban_check_failed' });
  }
});

// Operator password change (self-service)
app.post('/api/operator/change-password', async (req, res) => {
  const email = lower(req.body?.email || '');
  const oldPassword = safeStr(req.body?.oldPassword || '');
  const newPassword = safeStr(req.body?.newPassword || '');
  if(!email || !oldPassword || !newPassword) return res.status(400).json({ ok:false, error:'missing_fields' });
  if(newPassword.length < 4) return res.status(400).json({ ok:false, error:'weak_password' });

  const eu = await loadExtraUsers();
  const users = Array.isArray(eu.users) ? eu.users : [];
  const idx = users.findIndex(u => lower(u?.email) === email);
  if(idx < 0) return res.status(404).json({ ok:false, error:'not_found' });
  const u = users[idx];
  const cur = safeStr(u.pwdPlain || '');
  if(cur && cur !== oldPassword){
    return res.status(401).json({ ok:false, error:'wrong_password' });
  }
  users[idx] = Object.assign({}, u, { pwdPlain: newPassword, updatedAt: now() });
  await writeJson('extra_users.json', { users });
  res.json({ ok:true });
});

// Összes regisztrált felhasználó (online + offline) – moderáció trigger oldalhoz
app.get('/api/portal-users', async (_req, res) => {
  try{
    const cur = await loadPortalUsers().catch(()=>({ users: [] }));
    const users = Array.isArray(cur.users) ? cur.users : [];
    const onlineSet = new Set(
      Array.from(presence.users.values())
        .filter(u => now() - (u.lastSeen||0) < ONLINE_TTL_MS)
        .map(u => String(u.userId||'').toLowerCase())
    );

    const out = [];
    for(const u of users){
      const id = u.id || '';
      let credits = 0;
      try{ credits = await getCredits(id); }catch(e){}
      out.push({
        id,
        email: u.email || '',
        userId: String(u.email || u.nick || '').toLowerCase(),
        nick: u.nick || '',
        city: u.city || '',
        county: u.county || '',
        gender: u.gender || '',
        birthYear: u.birthYear || '',
        transport: u.transport || '',
        profilePic: u.profilePic || '',
        credits: credits || 0,
        purchased: (credits||0) > 0,
        online: onlineSet.has(String(u.email||u.nick||'').toLowerCase())
      });
    }
    res.json({ ok:true, users: out });
  }catch(e){
    res.status(500).json({ ok:false, error:'Szerver hiba.' });
  }
});

// Fiktív profilok listája (moderáció supervisor trigger dropdown-hoz)
app.get('/api/profiles', async (_req, res) => {
  try{
    const p = await loadProfiles();
    const arr = Array.isArray(p.profiles) ? p.profiles : [];
    // Keep payload small: only what UI needs.
    const out = arr.map(x => ({
      nick: x.nick || x.nickname || '',
      county: x.county || x.megye || '',
      city: x.city || x.varos || '',
      gender: x.gender || x.nem || '',
      age: x.age ?? x.kor ?? '',
      image: Array.isArray(x.images) && x.images.length ? x.images[0] : (x.image || '')
    })).filter(x => x.nick);
    res.json({ ok: true, profiles: out });
  }catch(e){
    res.status(500).json({ ok: false, err: 'profiles_failed' });
  }
});

// Optional: add/update a profile from the portal admin/supervisor UI
app.post('/api/profiles', async (req, res) => {
  try{
    const body = req.body || {};
    const nick = safeStr(body.nick || body.nickname).trim();
    if(!nick) return res.status(400).json({ ok:false, err:'missing_nick' });
    const item = {
      nick,
      gender: safeStr(body.gender || ''),
      age: body.age ?? '',
      county: safeStr(body.county || body.megye || ''),
      city: safeStr(body.city || body.varos || ''),
      about: safeStr(body.about || ''),
      images: Array.isArray(body.images) ? body.images.slice(0,10) : (body.image ? [String(body.image)] : [`image/${nick}/01.jpg`]),
      updatedAt: now()
    };
    await updateJson('profiles.json', { profiles: [] }, (cur) => {
      cur.profiles = Array.isArray(cur.profiles) ? cur.profiles : [];
      const key = lower(nick);
      const idx = cur.profiles.findIndex(p => lower(p?.nick) === key);
      if(idx >= 0) cur.profiles[idx] = Object.assign({}, cur.profiles[idx], item);
      else cur.profiles.unshift(Object.assign({ createdAt: now() }, item));
      // cap size
      if(cur.profiles.length > 5000) cur.profiles = cur.profiles.slice(0,5000);
      return cur;
    });
    res.json({ ok:true });
  }catch(e){
    res.status(500).json({ ok:false, err:'save_failed' });
  }
});

app.get('/api/audit/operators', async (_req, res) => {
  const a = await loadAudit();
  const set = new Set();
  for(const e of (a.entries||[])){
    if(e && e.operatorId) set.add(String(e.operatorId));
  }
  res.json({ ok: true, operators: Array.from(set).sort() });
});

app.get('/api/audit', async (req, res) => {
  const operatorId = safeStr(req.query?.operatorId).trim();
  const a = await loadAudit();
  let entries = Array.isArray(a.entries) ? a.entries : [];
  if(operatorId){
    const key = lower(operatorId);
    entries = entries.filter(x => lower(x.operatorId) === key);
  }
  res.json({ ok: true, entries: entries.slice(-5000) });
});

app.get('/api/audit/all', async (_req, res) => {
  const a = await loadAudit();
  const entries = Array.isArray(a.entries) ? a.entries : [];
  res.json({ ok: true, entries: entries.slice(-8000) });
});

app.get('/api/thread', async (req, res) => {
  const threadId = safeStr(req.query?.threadId).trim();
  if(!threadId) return res.status(400).json({ ok: false, err: 'missing_threadId' });
  const store = await loadThreads();
  const t = store.threads?.[threadId];
  if(!t) return res.json({ ok: false, err: 'not_found' });
  res.json({ ok: true, thread: t });
});

// Active presence snapshot (users + operators)
// Used by supervisor trigger list + admin/supervisor operator presence panels.
app.get('/api/active-users', async (_req, res) => {
  try{
    const users = [];
    const ops = [];
    for(const [_id, u] of presence.users.entries()){
      if(!u) continue;
      users.push({
        userId: u.userId || u.email || _id,
        email: u.email || '',
        nick: u.nick || '',
        city: u.city || '',
        county: u.county || '',
        transport: u.transport || '',
        age: u.age ?? '',
        profilePic: u.profilePic || '',
        lastSeen: u.lastSeen || 0,
        online: !!(u.lastSeen && (now() - u.lastSeen < ONLINE_TTL_MS))
      });
    }
    for(const [opId, o] of presence.operators.entries()){
      if(!o) continue;
      ops.push({
        operatorId: o.operatorId || opId,
        email: o.operatorId || opId,
        lastSeen: o.lastSeen || 0,
        online: !!(o.lastSeen && (now() - o.lastSeen < ONLINE_TTL_MS))
      });
    }
    users.sort((a,b)=> (b.lastSeen||0) - (a.lastSeen||0));
    ops.sort((a,b)=> (b.lastSeen||0) - (a.lastSeen||0));
    res.json({ ok:true, users, operators: ops });
  }catch(e){
    res.json({ ok:false, err:'active_users_failed' });
  }
});

app.get('/api/stats/operators', async (_req, res) => {
  const store = await loadThreads();
  const threads = store.threads || {};
  const sevenDaysAgo = now() - (7*24*60*60*1000);
  const stats = {};

  for(const tid of Object.keys(threads)){
    const t = threads[tid];
    const op = t?.operatorId ? String(t.operatorId) : '';
    if(!op) continue;
    stats[op] = stats[op] || { total7: 0, conv7: 0, weekTotal: 0, received7: 0, replied7: 0 };

    const msgs = Array.isArray(t.messages) ? t.messages : [];
    let has7 = false;
    for(const m of msgs){
      if(!m || !m.ts) continue;
      if(m.ts >= sevenDaysAgo){
        has7 = true;
        stats[op].total7 += 1;
        if(String(m.from||'') === 'client' || String(m.from||'') === 'user') stats[op].received7 += 1;
        if(String(m.from||'') === 'operator') stats[op].replied7 += 1;
      }
    }
    if(has7) stats[op].conv7 += 1;

    // Week total = current ISO week total messages (operator+client)
    // We'll approximate: last 7 days is fine; UI expects a number.
    stats[op].weekTotal = stats[op].total7;
  }

  res.json({ ok: true, stats });
});

app.get('/api/stats/operator', async (req, res) => {
  const operatorId = safeStr(req.query?.operatorId).trim();
  if(!operatorId) return res.status(400).json({ ok: false, err: 'missing_operatorId' });
  const all = (await fetchLocal('/api/stats/operators')).stats || {};
  const st = all[operatorId] || all[operatorId.toLowerCase()] || { total7: 0, conv7: 0, weekTotal: 0, received7: 0, replied7: 0 };
  res.json({ ok: true, stats: st });
});

// helper: reuse logic without making actual HTTP request
async function fetchLocal(_dummy){
  const store = await loadThreads();
  const threads = store.threads || {};
  const sevenDaysAgo = now() - (7*24*60*60*1000);
  const stats = {};
  for(const tid of Object.keys(threads)){
    const t = threads[tid];
    const op = t?.operatorId ? String(t.operatorId) : '';
    if(!op) continue;
    stats[op] = stats[op] || { total7: 0, conv7: 0, weekTotal: 0, received7: 0, replied7: 0 };
    const msgs = Array.isArray(t.messages) ? t.messages : [];
    let has7 = false;
    for(const m of msgs){
      if(m?.ts >= sevenDaysAgo){
        has7 = true;
        stats[op].total7 += 1;
        if(String(m.from||'') === 'client' || String(m.from||'') === 'user') stats[op].received7 += 1;
        if(String(m.from||'') === 'operator') stats[op].replied7 += 1;
      }
    }
    if(has7) stats[op].conv7 += 1;
    stats[op].weekTotal = stats[op].total7;
  }
  return { ok: true, stats };
}

// ---- Socket.IO (real-time chat + triggers + internal chat) ----
const httpServer = http.createServer(app);
const io = new SocketIOServer(httpServer, {
  cors: { origin: true, credentials: true }
});

// In-memory socket maps
const socketsByUser = new Map(); // userId -> Set(socket)
const socketsByOperator = new Map(); // operatorEmail -> Set(socket)
const socketsByStaff = new Set(); // admin+supervisor sockets

function addSock(map, key, sock){
  if(!map.has(key)) map.set(key, new Set());
  map.get(key).add(sock);
}
function delSock(map, key, sock){
  const set = map.get(key);
  if(!set) return;
  set.delete(sock);
  if(set.size===0) map.delete(key);
}

function isOnlineOperator(operatorId){
  const p = presence.operators.get(operatorId);
  return !!(p && (now() - (p.lastSeen||0) < ONLINE_TTL_MS));
}

function touchOperator(operatorId){
  if(!operatorId) return;
  presence.operators.set(String(operatorId), { operatorId: String(operatorId), lastSeen: now() });
}

function touchUser(userId, patch = null){
  if(!userId) return;
  const prev = presence.users.get(userId) || { userId, lastSeen: 0 };
  presence.users.set(userId, Object.assign({}, prev, patch||{}, { userId, lastSeen: now() }));
}

async function isBanned(operatorId){
  const bans = await loadBans();
  const key = lower(operatorId);
  const it = bans.operators?.[key] || null;
  if(!it) return false;
  if(it.permanent) return true;
  if(it.until && now() < it.until) return true;
  return false;
}

async function pickOperator(){
  // pick least-loaded online operator
  const store = await loadThreads();
  const assignments = store.assignments || {};
  const load = {};
  for(const tid of Object.keys(assignments)){
    const op = assignments[tid];
    if(!op) continue;
    load[op] = (load[op]||0) + 1;
  }
  const online = Array.from(presence.operators.keys()).filter(isOnlineOperator);
  const eligible = [];
  for(const op of online){
    if(await isBanned(op)) continue;
    // Only operators with ZERO active assignments are eligible (1 thread at a time)
    if((load[op]||0) > 0) continue;
    eligible.push(op);
  }
  eligible.sort((a,b)=> a.localeCompare(b));
  return eligible[0] || null;
}

async function ensureAssignedServer(threadId){
  const store = await loadThreads();
  store.assignments = store.assignments || {};
  let op = store.assignments[threadId] || store.threads?.[threadId]?.operatorId || null;
  if(op && isOnlineOperator(op) && !(await isBanned(op))){
    return op;
  }
  // try pick new
  const picked = await pickOperator();
  if(picked){
    store.assignments[threadId] = picked;
    if(store.threads?.[threadId]) store.threads[threadId].operatorId = picked;
    await saveThreads(store);
    return picked;
  }
  // queue
  store.queue = Array.isArray(store.queue) ? store.queue : [];
  if(!store.queue.includes(threadId)) store.queue.push(threadId);
  await saveThreads(store);
  return null;
}

function emitToOperator(operatorId, evt, payload){
  const set = socketsByOperator.get(operatorId);
  if(!set) return;
  for(const s of set){
    try{ s.emit(evt, payload); }catch(e){}
  }
}

function emitToOnlineOperators(evt, payload){
  for(const [opId, set] of socketsByOperator.entries()){
    if(!isOnlineOperator(opId)) continue;
    for(const s of set){
      try{ s.emit(evt, payload); }catch(e){}
    }
  }
}


function emitToUser(userId, evt, payload){
  const set = socketsByUser.get(userId);
  if(!set) return;
  for(const s of set){
    try{ s.emit(evt, payload); }catch(e){}
  }
}

async function sendOperatorThreads(operatorId){
  const store = await loadThreads();
  const threads = store.threads || {};
  const out = [];
  for(const tid of Object.keys(threads)){
    const t = threads[tid];
    if(!t || lower(t.operatorId) !== lower(operatorId)) continue;
    out.push({
      threadId: tid,
      userId: t.userId,
      profileNick: t.profileNick,
      userMeta: t.userMeta || {},
      profileMeta: t.profileMeta || {}
    });
  }
  emitToOperator(operatorId, 'operator:threads', { threads: out });
}

async function unassignFromOperator(operatorId, reason='reassign'){
  const store = await loadThreads();
  store.assignments = store.assignments || {};
  for(const tid of Object.keys(store.assignments)){
    if(lower(store.assignments[tid]) === lower(operatorId)){
      delete store.assignments[tid];
      if(store.threads?.[tid]) store.threads[tid].operatorId = '';
      emitToOperator(operatorId, 'operator:unassignThread', { threadId: tid, reason });
      // queue for reassignment
      store.queue = Array.isArray(store.queue) ? store.queue : [];
      if(!store.queue.includes(tid)) store.queue.push(tid);
    }
  }
  await saveThreads(store);
}

async function flushQueue(){
  const store = await loadThreads();
  store.queue = Array.isArray(store.queue) ? store.queue : [];
  const pending = store.queue.slice();
  store.queue = [];
  for(const tid of pending){
    const op = await ensureAssignedServer(tid);
    if(!op){
      store.queue.push(tid);
      continue;
    }
    const t = store.threads?.[tid];
    if(t){
      emitToOperator(op, "chat:syncThread", { threadId: tid, messages: t.messages || [], userMeta: t.userMeta || {}, profileNick: t.profileNick });
      await sendOperatorThreads(op);
    }
  }
  await saveThreads(store);
}

async function persistMessage({ threadId, userId, profileNick, operatorId, from, text, img, meta, id }){
  const msgId = (id ? String(id) : ((meta && meta.id) ? String(meta.id) : nanoid(10)));
  const msg = {
    id: msgId,
    from,
    text: clampStr(text, 5000),
    img: img ? String(img) : '',
    ts: now(),
    meta: meta || null
  };
  await updateJson('threads.json', { threads: {}, assignments: {}, queue: [] }, (cur) => {
    cur.threads = cur.threads || {};
    cur.assignments = cur.assignments || {};
    const t = cur.threads[threadId] || {
      threadId,
      userId,
      profileNick,
      operatorId: operatorId || '',
      userMeta: {},
      profileMeta: {},
      messages: []
    };
    t.userId = userId;
    t.profileNick = profileNick;
    if(operatorId) t.operatorId = operatorId;
    t.messages = Array.isArray(t.messages) ? t.messages : [];
    // Dedupe: if client resends the same message id (reconnect / retry), don't append again.
    if(!t.messages.some(m => m && String(m.id) === msgId)){
      t.messages.push(msg);
    }
    if(t.messages.length > 2000) t.messages = t.messages.slice(-2000);
    cur.threads[threadId] = t;
    if(operatorId) cur.assignments[threadId] = operatorId;
    return cur;
  });
  return msg;
}

async function getThread(threadId){
  const store = await loadThreads();
  return store.threads?.[threadId] || null;
}

io.on('connection', (socket) => {
  socket.on('auth:login', async (p) => {
    try{
      p = Object.assign({}, (p||{}), (p && p.data ? p.data : {}));
      const role = safeStr(p?.role).trim();
      socket.data.role = role;

      if(role === 'user'){
        const userId = lower(p?.userId || p?.email);
        if(!userId){ socket.emit('auth:denied', { reason:'missing_userId' }); return; }
        socket.data.userId = userId;
        // Keep email for special rules (admin free messages, etc.)
        socket.data.email = lower(p?.email || (String(userId).includes('@') ? userId : ''));
        socket.data.nick = safeStr(p?.nick || '');
        socket.data.city = safeStr(p?.city || '');
        socket.data.county = safeStr(p?.county || '');
        socket.data.transport = safeStr(p?.transport || '');
                socket.data.hobbies = safeStr(p?.hobbies || '');
        socket.data.work = safeStr(p?.work || '');
        socket.data.about = safeStr(p?.about || '');
        socket.data.relationship = safeStr(p?.relationship || '');
        socket.data.birthYear = safeStr(p?.birthYear || '');
        socket.data.gender = safeStr(p?.gender || '');
socket.data.age = p?.age ?? '';
        socket.data.profilePic = safeStr(p?.profilePic || '');
        addSock(socketsByUser, userId, socket);
        presence.users.set(userId, { userId, lastSeen: now(), nick: socket.data.nick, city: socket.data.city, county: socket.data.county, transport: socket.data.transport, age: socket.data.age, profilePic: socket.data.profilePic });

        // Keep user online status truly real-time even when they are only browsing.
        // (The portal does not send an explicit ping, so we touch presence while the socket is connected.)
        try{
          socket.data.__hb && clearInterval(socket.data.__hb);
          socket.data.__hb = setInterval(()=>{
            try{
              if(socket.connected){
                touchUser(userId, {
                  nick: socket.data.nick||'',
                  city: socket.data.city||'',
                  county: socket.data.county||'',
                  transport: socket.data.transport||'',
                  hobbies: socket.data.hobbies||'',
                  work: socket.data.work||'',
                  about: socket.data.about||'',
                  relationship: socket.data.relationship||'',
                  birthYear: socket.data.birthYear||'',
                  gender: socket.data.gender||'',
                  age: socket.data.age||'',
                  profilePic: socket.data.profilePic||''
                });
              }
            }catch(e){}
          }, 12000);
        }catch(e){}
        return;
      }

      if(role === 'operator'){
        // Operator UI sends { operatorId: email } (historic), while some pages send { email }.
        const email = lower(p?.email || p?.operatorId || p?.operatorID || p?.operator || p?.operator_id);
        if(!email){ socket.emit('auth:denied', { reason:'missing_email' }); return; }
        if(await isBanned(email)){
          socket.emit('operator:banned', { operatorId: email });
          socket.disconnect(true);
          return;
        }
        socket.data.email = email;
        socket.data.id = safeStr(p?.id || '');
        addSock(socketsByOperator, email, socket);
        presence.operators.set(email, { operatorId: email, lastSeen: now() });
        await sendOperatorThreads(email);
        await flushQueue();
        return;
      }

      if(role === 'admin' || role === 'supervisor'){
        socket.data.email = lower(p?.email || '');
        socket.data.id = safeStr(p?.id || '');
        socketsByStaff.add(socket);
        return;
      }

      socket.emit('auth:denied', { reason:'unknown_role' });
    }catch(e){
      socket.emit('auth:denied', { reason:'error' });
    }
  });

  socket.on('operator:ping', async (_p) => {
    const role = socket.data.role;
    if(role !== 'operator') return;
    const op = socket.data.email;
    if(!op) return;
    if(await isBanned(op)){
      socket.emit('operator:banned', { operatorId: op });
      socket.disconnect(true);
      return;
    }
    touchOperator(op);
    // keep queue moving
    await flushQueue();
  });

  socket.on('operator:fetchThread', async (p) => {
    const role = socket.data.role;
    if(role !== 'operator') return;
    const threadId = safeStr(p?.threadId).trim();
    if(!threadId) return;
    touchOperator(socket.data.email);
    const t = await getThread(threadId);
    if(!t) return;
    socket.emit('chat:syncThread', { threadId, messages: t.messages || [], userMeta: t.userMeta || {}, profileNick: t.profileNick });
  });

  socket.on('operator:releaseThread', async (p) => {
    const role = socket.data.role;
    if(role !== 'operator') return;
    const op = socket.data.email;
    const threadId = safeStr(p?.threadId).trim();
    if(!op || !threadId) return;
    touchOperator(op);
    await updateJson('threads.json', { threads: {}, assignments: {}, queue: [] }, (cur) => {
      cur.assignments = cur.assignments || {};
      if(lower(cur.assignments[threadId]) === lower(op)) delete cur.assignments[threadId];
      if(cur.threads?.[threadId]) cur.threads[threadId].operatorId = '';
      cur.queue = Array.isArray(cur.queue) ? cur.queue : [];
      if(!cur.queue.includes(threadId)) cur.queue.push(threadId);
      return cur;
    });
    socket.emit('operator:backToWaiting', { threadId });
    await flushQueue();
  });

  socket.on('operator:handled', async (_p) => {
    // operator finished current; just nudge waiting status
    if(socket.data.role !== 'operator') return;
    touchOperator(socket.data.email);
    socket.emit('operator:backToWaiting', { ok:true });
    await flushQueue();
  });

  socket.on('chat:userMessage', async (p) => {
    try{
      const userId = lower(p?.userId);
      const threadId = safeStr(p?.threadId).trim();
      const profileNick = safeStr(p?.profileNick).trim();
      if(!userId || !threadId || !profileNick) return;

      // Update presence + persist a stable userMeta snapshot into the thread so operator UI can show
      // nick/age/county/city/transport/profilePic reliably.
      const userMetaPatch = {
        nick: safeStr(p?.nick || p?.userMeta?.nick || ''),
        city: safeStr(p?.city || p?.userMeta?.city || ''),
        county: safeStr(p?.county || p?.userMeta?.county || ''),
        transport: safeStr(p?.transport || p?.userMeta?.transport || ''),
        age: p?.age ?? (p?.userMeta?.age ?? ''),
        profilePic: safeStr(p?.profilePic || p?.userMeta?.profilePic || '')
      };
const profileMetaPatch = (p && p.profileMeta && typeof p.profileMeta === 'object') ? p.profileMeta : {
  nick: profileNick,
  city: safeStr(p?.profileCity || ''),
  county: safeStr(p?.profileCounty || ''),
  gender: safeStr(p?.profileGender || ''),
  age: p?.profileAge ?? '',
  avatar: safeStr(p?.profileAvatar || '')
};
      touchUser(userId, userMetaPatch);
      await updateJson('threads.json', { threads: {}, assignments: {}, queue: [] }, (cur) => {
        cur.threads = cur.threads || {};
        const t = cur.threads[threadId] || { threadId, userId, profileNick, operatorId: '', userMeta: {}, profileMeta: {}, messages: [] };
        t.userId = userId;
        t.profileNick = profileNick;
        t.userMeta = Object.assign({}, t.userMeta || {}, userMetaPatch);
        t.profileMeta = Object.assign({}, t.profileMeta || {}, profileMetaPatch || {});
        cur.threads[threadId] = t;
        return cur;
      });

      // Credits: 10 per message (image is also 10). Debit when the USER sends.
// Admin user is exempt (0 credit per message) as requested.
const isAdmin = (lower(socket.data?.email) === 'admin@forrovagy.hu') || (lower(userId) === 'admin@forrovagy.hu');
const cost = isAdmin ? 0 : 10;

if(cost > 0){
  const rid = safeStr((p && (p.id || (p.meta && p.meta.id))) || '').trim() || (threadId + '|' + String(now()));
  const debit = await debitCredits(userId, cost, rid);
  if(!debit.ok){
    socket.emit('credits:insufficient', { required: cost, credits: Number(debit.credits||0) });
    return;
  }
  emitToUser(userId, 'credits:update', { credits: Number(debit.credits||0) });
} else {
  // keep UI in sync
  emitToUser(userId, 'credits:update', { credits: await getCredits(userId) });
}

      const op = await ensureAssignedServer(threadId);
      const msg = await persistMessage({
        threadId,
        userId,
        profileNick,
        operatorId: op || '',
        from: 'client',
        text: p?.text || '',
        img: p?.img || '',
        meta: Object.assign({}, (p?.meta || null), (p?.id ? { id: String(p.id) } : null)),
        id: p?.id
      });

      await appendAudit({
        operatorId: op || '',
        threadId,
        profileNick,
        userId,
        direction: 'in',
        text: msg.text,
        img: msg.img,
        ts: msg.ts,
        meta: msg.meta || null
      });

      
if(op){
  const t = await getThread(threadId);
  const payload = {
    threadId,
    userId,
    profileNick,
    text: msg.text,
    img: msg.img,
    meta: msg.meta,
    // For operator UI (avatars + labels)
    profilePic: t?.userMeta?.profilePic || '',
    profileMeta: t?.profileMeta || {},
    userMeta: t?.userMeta || presence.users.get(userId) || {}
  };
  emitToOperator(op, 'chat:incoming', payload);
  // Also allow supervisor/admin to see incoming instantly in their UIs if they are connected
  for(const s of socketsByStaff){
    try{ s.emit('chat:incoming', Object.assign({ operatorId: op || '' }, payload)); }catch(e){}
  }
} else {
  // No operator online right now; message is queued. Notify any online operators so they can refresh.
  emitToOnlineOperators('operator:queueChanged', { threadId });
}
await sendOperatorThreads(op);
    }catch(e){}
  });

  socket.on('chat:operatorMessage', async (p) => {
    try{
      const op = socket.data.role === 'operator' ? socket.data.email : lower(p?.operatorId);
      const threadId = safeStr(p?.threadId).trim();
      // Some older operator UIs only send threadId, so derive missing fields.
      const derived = splitThreadId(threadId);
      const userId = lower(p?.userId || derived.userId);
      const profileNick = safeStr(p?.profileNick || derived.profileNick).trim();
      if(!op || !threadId || !userId || !profileNick) return;

      touchOperator(op);

      const msg = await persistMessage({
        threadId,
        userId,
        profileNick,
        operatorId: op,
        from: 'operator',
        text: p?.text || '',
        img: p?.img || '',
        meta: Object.assign({}, (p?.meta || null), (p?.id ? { id: String(p.id) } : null)),
        id: p?.id
      });

      await appendAudit({
        operatorId: op,
        threadId,
        profileNick,
        userId,
        direction: 'out',
        text: msg.text,
        img: msg.img,
        ts: msg.ts,
        meta: msg.meta || null
      });

      emitToUser(userId, 'chat:message', {
        threadId,
        from: 'operator',
        text: msg.text,
        img: msg.img,
        ts: msg.ts,
        meta: msg.meta || null,
        profileNick
      });

      // let operator UI refresh
      emitToOperator(op, 'chat:syncThread', { threadId, messages: (await getThread(threadId))?.messages || [], userMeta: (await getThread(threadId))?.userMeta || {}, profileNick });
      await sendOperatorThreads(op);
    }catch(e){}
  });

  socket.on('chat:systemMessage', async (p) => {
    // trigger / poke / favorite like messages
    try{
      const threadId = safeStr(p?.threadId).trim();
      const userId = lower(p?.userId);
      const profileNick = safeStr(p?.profileNick).trim();
      if(!threadId || !userId || !profileNick) return;

      // Prevent "beragadt / vibráló" poke: if the same thread already got a poke recently,
      // ignore duplicates (clients sometimes resend on reconnect).
      const msgType = String(p?.meta?.type || '').toLowerCase();
      if(msgType === 'poke'){
        const t0 = await getThread(threadId);
        const msgs0 = Array.isArray(t0?.messages) ? t0.messages : [];
        const lastPoke = [...msgs0].reverse().find(m => String(m?.meta?.type||'').toLowerCase() === 'poke');
        if(lastPoke && (now() - (lastPoke.ts||0)) < 2*60*1000){
          return;
        }
      }

      const op = await ensureAssignedServer(threadId);
      const msg = await persistMessage({
        threadId,
        userId,
        profileNick,
        operatorId: op || '',
        from: 'operator',
        text: p?.text || '',
        img: p?.img || '',
        meta: Object.assign({}, (p?.meta || null), (p?.id ? { id: String(p.id) } : null)),
        id: p?.id
      });

      await appendAudit({
        operatorId: op || '',
        threadId,
        profileNick,
        userId,
        direction: 'out',
        text: msg.text,
        img: msg.img,
        ts: msg.ts,
        meta: msg.meta || null
      });

      emitToUser(userId, 'chat:message', {
        threadId,
        from: 'operator',
        text: msg.text,
        img: msg.img,
        ts: msg.ts,
        meta: msg.meta || null,
        profileNick
      });

      if(op){
        await sendOperatorThreads(op);
      }
    }catch(e){}
  });

  socket.on('internal:history', async () => {
    if(!(socket.data.role === 'admin' || socket.data.role === 'supervisor')) return;
    const ic = await loadInternalChat();
    socket.emit('internal:history', { messages: (ic.messages||[]).slice(-500) });
  });

  socket.on('internal:send', async (p) => {
    if(!(socket.data.role === 'admin' || socket.data.role === 'supervisor')) return;
    const text = clampStr(p?.text || '', 2000);
    const img = p?.img ? String(p.img) : '';
    if(!text && !img) return;
    const msg = { id: nanoid(10), from: socket.data.id || socket.data.email || 'staff', text, img, ts: now() };
    const ic = await loadInternalChat();
    ic.messages = Array.isArray(ic.messages) ? ic.messages : [];
    ic.messages.push(msg);
    if(ic.messages.length > 2000) ic.messages = ic.messages.slice(-2000);
    await saveInternalChat(ic);
    for(const s of socketsByStaff){
      try{ s.emit('internal:message', msg); }catch(e){}
    }
  });

  socket.on('admin:banOperator', async (p) => {
    if(socket.data.role !== 'admin' && socket.data.role !== 'supervisor') return;
    const operatorId = lower(p?.operatorId);
    const duration = safeStr(p?.duration).trim();
    if(!operatorId) return;
    const bans = await loadBans();
    bans.operators = bans.operators || {};
    if(duration === 'permanent'){
      bans.operators[operatorId] = { permanent: true, until: null, by: socket.data.email || socket.data.id || '', ts: now() };
    } else if(duration === '24h'){
      bans.operators[operatorId] = { permanent: false, until: now() + 24*60*60*1000, by: socket.data.email || socket.data.id || '', ts: now() };
    } else {
      bans.operators[operatorId] = { permanent: false, until: now() + 60*60*1000, by: socket.data.email || socket.data.id || '', ts: now() };
    }
    await saveBans(bans);

    // Kick + unassign
    emitToOperator(operatorId, 'operator:banned', { operatorId });
    await unassignFromOperator(operatorId, 'banned');
    await flushQueue();
  });

  socket.on('disconnect', () => {
    try{
      if(socket.data.role === 'user'){
        try{ socket.data.__hb && clearInterval(socket.data.__hb); }catch(e){}
        const userId = socket.data.userId;
        if(userId) delSock(socketsByUser, userId, socket);
      } else if(socket.data.role === 'operator'){
        const op = socket.data.email;
        if(op) delSock(socketsByOperator, op, socket);
        // If this was the last socket of the operator, immediately requeue their active thread(s)
        // so another online operator can pick them up.
        if(op && !socketsByOperator.has(op)){
          try{ presence.operators.delete(op); }catch(e){}
          (async()=>{
            try{ await unassignFromOperator(op, 'disconnect'); }catch(e){}
            try{ await flushQueue(); }catch(e){}
          })();
        }
      } else if(socket.data.role === 'admin' || socket.data.role === 'supervisor'){
        socketsByStaff.delete(socket);
      }
    }catch(e){}
  });
});



// --- Auto seed test portal account (for live testing) ---
const TEST_PORTAL_EMAIL = normEmail(process.env.TEST_PORTAL_EMAIL || 'amin@titkosvagy.com');
const TEST_PORTAL_PASSWORD = safeStr(process.env.TEST_PORTAL_PASSWORD || 'Amin1234!').trim();
const TEST_PORTAL_NICK = safeStr(process.env.TEST_PORTAL_NICK || 'Amin').trim() || 'Amin';
const TEST_PORTAL_CREDITS = Number(process.env.TEST_PORTAL_CREDITS || 1000);

async function ensureCreditsAtLeast(userId, amount){
  if(!userId) return;
  const store = await readJson(CREDITS_FILE, {});
  const cur = Number(store[userId] || 0);
  if(cur < Number(amount || 0)){
    store[userId] = Number(amount || 0);
    await writeJson(CREDITS_FILE, store);
  }
}

async function ensureTestPortalUser(){
  try{
    const cur = await loadPortalUsers();
    let user = cur.users.find(u => normEmail(u.email) === TEST_PORTAL_EMAIL);
    if(!user){
      const id = 'u_' + nanoid(10);
      const { salt, hash } = hashPassword(TEST_PORTAL_PASSWORD);
      user = {
        id,
        email: TEST_PORTAL_EMAIL,
        nick: TEST_PORTAL_NICK,
        passSalt: salt,
        passHash: hash,
        city: 'Budapest',
        county: 'Budapest',
        birthYear: '1998',
        gender: 'férfi',
        profilePic: '',
        transport: '',
        hobbies: '',
        work: '',
        about: '',
        relationship: '',
        verified: true,
        verifyTokenHash: '',
        verifyTokenCreatedAt: 0,
        createdAt: now()
      };
      cur.users.push(user);
      await savePortalUsers(cur);
      console.log('[seed] Test portal user created:', TEST_PORTAL_EMAIL);
    }else{
      // Ensure verified so login works even if older data existed
      if(user.verified !== true){
        user.verified = true;
        await savePortalUsers(cur);
      }
    }
    await ensureCreditsAtLeast(user.id, TEST_PORTAL_CREDITS);
  }catch(e){
    console.warn('[seed] Failed to ensure test portal user:', e?.message || e);
  }
}

// Seed on startup (non-blocking)
await ensureTestPortalUser();

httpServer.listen(PORT, () => {
  console.log(`Titkosvagy backend listening on :${PORT}`);
});
