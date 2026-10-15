/*
 * CUTLOOM - optional YouTube link import (one Vercel Node.js function)
 * ====================================================================
 *
 *   GET /api/yt?id=VIDEO_ID            -> JSON { id, title, author, duration, thumbnail, client,
 *                                               options: [ { kind, h, label, itag, hasAudio, proxy, ... } ] }
 *                                         (also accepts ?url=<any youtube link>)
 *   GET /api/yt?id=VIDEO_ID&itag=18    -> streams that format with Range support + CORS
 *                                         (add &dl=1 to force a file download)
 *
 * Why the stream goes through here: googlevideo URLs are locked to the IP that asked for them,
 * so the browser cannot read them itself. Each ranged answer is capped (4 MB by default) to stay
 * under serverless response limits; index.html reads the file in 4 MB chunks automatically.
 *
 * DEPLOY
 *   1. Folder must contain: index.html, api/yt.js, package.json  (nothing else is needed).
 *   2. `npx vercel --prod` inside the folder, or import the Git repo in Vercel.
 *      Framework preset "Other", no build command, output directory = project root.
 *      Vercel installs youtubei.js from package.json (Node 20+).
 *   3. Check: https://YOUR-SITE.vercel.app/api/yt?id=jNQXAC9IVRw  should return JSON.
 *   The page works without this function too (file import, recording, editing, export);
 *   only "paste a YouTube link" needs it.
 *
 * OPTIONAL ENVIRONMENT VARIABLES (Vercel > Project > Settings > Environment Variables)
 *   YT_COOKIE          Cookie header of a logged-in YouTube session. Use it when /api/yt answers
 *                      code "bot_check" ("Sign in to confirm you're not a bot"), which happens often
 *                      from datacenter IPs. How to get it: open youtube.com in a private window,
 *                      sign in with a THROWAWAY account, open DevTools > Network, reload, click the
 *                      first "www.youtube.com" request, copy the whole "cookie:" request header value
 *                      and paste it as YT_COOKIE. Redeploy. Cookies expire; repeat when bot_check returns.
 *   YT_PO_TOKEN + YT_VISITOR_DATA   Proof-of-origin token and its visitor data (advanced, see youtubei.js docs).
 *   YT_CLIENTS         Client order. Default: ANDROID_VR,IOS,TV,WEB_EMBEDDED,MWEB,WEB
 *   STREAM_CHUNK_BYTES Max bytes per ranged answer. Default 4194304 (4 MB).
 *
 * Use only videos you own or have permission to edit, and respect YouTube's Terms of Service.
 */
import { Readable } from 'node:stream';
import { Innertube, UniversalCache, Constants, Log } from 'youtubei.js';

// Vercel reads this for the function's time limit (no vercel.json needed).
export const config = { maxDuration: 60 };

try { Log.setLevel(Log.Level.ERROR); } catch (_) {}

const ID_RE = /^[\w-]{11}$/;
const TTL = 4 * 60 * 60 * 1000;                 // stream URLs live ~6 h, keep 4 h
const CACHE = new Map();                         // id -> { at, data, urls: Map(itag -> {url, ua, size, mime}) }
const CLIENTS = (process.env.YT_CLIENTS || 'ANDROID_VR,IOS,TV,WEB_EMBEDDED,MWEB,WEB').split(',').map(s => s.trim()).filter(Boolean);
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36';
const MAX = Math.max(256 * 1024, Number(process.env.STREAM_CHUNK_BYTES) || 4 * 1024 * 1024);

class YTError extends Error {
  constructor(code, message, status = 502) { super(message); this.code = code; this.status = status; }
}

let ytPromise = null;
function getYT() {
  if (!ytPromise) {
    ytPromise = Innertube.create({
      cache: new UniversalCache(false),
      retrieve_player: true,
      generate_session_locally: !process.env.YT_VISITOR_DATA,
      cookie: process.env.YT_COOKIE || undefined,
      po_token: process.env.YT_PO_TOKEN || undefined,
      visitor_data: process.env.YT_VISITOR_DATA || undefined
    }).catch(e => { ytPromise = null; throw e; });
  }
  return ytPromise;
}

const uaFor = client => (Constants.CLIENTS[client] && Constants.CLIENTS[client].USER_AGENT) || BROWSER_UA;
const isMp4 = f => /^video\/mp4/.test(f.mime_type || '');
const isAvc = f => /avc1/.test(f.mime_type || '');
const height = f => f.height || parseInt(f.quality_label, 10) || 0;

async function urlOf(f, yt) {
  if (f.url) return f.url;
  if (typeof f.decipher === 'function') {
    try { const u = await f.decipher(yt.session.player); if (u) return u; } catch (_) {}
  }
  return null;
}

async function resolve(id) {
  if (!ID_RE.test(id || '')) throw new YTError('bad_id', 'Missing or invalid YouTube video id', 400);
  const hit = CACHE.get(id);
  if (hit && Date.now() - hit.at < TTL) return hit;
  let yt;
  try { yt = await getYT(); } catch (e) { throw new YTError('init_failed', 'Could not start the YouTube client: ' + (e && e.message || e)); }

  const reasons = [];
  for (const client of CLIENTS) {
    let info;
    try { info = await yt.getBasicInfo(id, { client }); }
    catch (e) { reasons.push(client + ': ' + String(e && e.message || e).split('\n')[0].slice(0, 140)); continue; }
    const ps = info.playability_status || {};
    const sd = info.streaming_data;
    if (ps.status && ps.status !== 'OK') { reasons.push(client + ': ' + ps.status + (ps.reason ? ' (' + ps.reason + ')' : '')); continue; }
    if (!sd) { reasons.push(client + ': no streaming data'); continue; }

    const ua = uaFor(client);
    const urls = new Map();
    const options = [];
    const add = async (f, kind) => {
      const url = await urlOf(f, yt);
      if (!url) return;
      const h = height(f);
      urls.set(String(f.itag), { url, ua, size: Number(f.content_length) || 0, mime: (f.mime_type || 'video/mp4').split(';')[0] });
      options.push({
        kind, h,
        label: f.quality_label || (h ? h + 'p' : 'video'),
        quality: f.quality_label || f.quality || '',
        itag: f.itag,
        mime: f.mime_type || '',
        hasAudio: kind === 'muxed',
        fps: f.fps || 0,
        size: Number(f.content_length) || 0,
        host: 'your server',
        proxy: '/api/yt?id=' + id + '&itag=' + f.itag
      });
    };
    for (const f of (sd.formats || []).filter(f => isMp4(f) && f.has_video !== false).sort((a, b) => height(b) - height(a))) await add(f, 'muxed');
    const vo = new Map();
    for (const f of (sd.adaptive_formats || [])) {
      if (!isMp4(f) || !isAvc(f) || f.has_audio) continue;
      const h = height(f); if (!h || h > 1080) continue;
      const cur = vo.get(h); if (!cur || (f.bitrate || 0) > (cur.bitrate || 0)) vo.set(h, f);
    }
    for (const f of [...vo.values()].sort((a, b) => height(b) - height(a)).slice(0, 4)) await add(f, 'video');

    if (!options.length) { reasons.push(client + ': no MP4 stream with a readable URL'); continue; }
    const bi = info.basic_info || {};
    const data = {
      id,
      title: bi.title || ('youtube-' + id),
      author: bi.author || '',
      duration: bi.duration || 0,
      thumbnail: (bi.thumbnail && bi.thumbnail[0] && bi.thumbnail[0].url) || ('https://i.ytimg.com/vi/' + id + '/hqdefault.jpg'),
      client,
      options
    };
    const entry = { at: Date.now(), data, urls };
    CACHE.set(id, entry);
    if (CACHE.size > 200) CACHE.delete(CACHE.keys().next().value);
    return entry;
  }
  const all = reasons.join(' | ');
  if (/LOGIN_REQUIRED|not a bot|Sign in/i.test(all)) throw new YTError('bot_check', 'YouTube asked this server to sign in (bot check). ' + all);
  if (/UNPLAYABLE|ERROR|unavailable|private|age/i.test(all)) throw new YTError('unavailable', 'YouTube says this video cannot be played from a server. ' + all);
  throw new YTError('no_streams', 'No downloadable MP4 stream was found. ' + all);
}

function parseRange(h) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(h || '').trim());
  if (!m || (m[1] === '' && m[2] === '')) return null;
  if (m[1] === '') return { suffix: Number(m[2]) };
  return { start: Number(m[1]), end: m[2] === '' ? null : Number(m[2]) };
}

async function pipeUpstream(req, res, src, filename) {
  const range = parseRange(req.headers.range);
  const headers = { 'User-Agent': src.ua || 'Mozilla/5.0', Accept: '*/*', 'Accept-Encoding': 'identity' };
  let capped = false;
  if (range) {
    let { start, end, suffix } = range;
    if (suffix != null && src.size) { start = Math.max(0, src.size - suffix); end = src.size - 1; }
    if (start == null) start = 0;
    if (end == null || end - start + 1 > MAX) { end = start + MAX - 1; capped = true; }
    if (src.size && end > src.size - 1) end = src.size - 1;
    headers.Range = 'bytes=' + start + '-' + end;
  }
  const up = await fetch(src.url, { headers, redirect: 'follow' });
  if (!up.ok && up.status !== 206) {
    const e = new YTError('upstream_' + up.status, 'YouTube refused the stream (HTTP ' + up.status + ')', up.status === 416 ? 416 : 502);
    e.upstream = up.status; throw e;
  }
  res.statusCode = up.status;
  for (const k of ['content-type', 'content-length', 'content-range', 'accept-ranges', 'last-modified', 'etag']) {
    const v = up.headers.get(k); if (v) res.setHeader(k, v);
  }
  if (!up.headers.get('content-type')) res.setHeader('Content-Type', src.mime || 'video/mp4');
  res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Cache-Control', 'private, max-age=600');
  if (capped) res.setHeader('X-Range-Capped', String(MAX));
  if (filename) res.setHeader('Content-Disposition', 'attachment; filename="' + filename.replace(/[^\w.\- ]+/g, '_') + '"');
  if (req.method === 'HEAD' || !up.body) return res.end();
  await new Promise((ok, no) => {
    const body = Readable.fromWeb(up.body);
    body.on('error', no); res.on('close', ok); res.on('finish', ok);
    body.pipe(res);
  });
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Range, Content-Type');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges, Content-Type, Content-Disposition, X-Range-Capped');
}

function sendError(res, e) {
  const err = e instanceof YTError ? e : new YTError('error', String(e && e.message || e));
  res.statusCode = err.status || 502;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.end(JSON.stringify({ code: err.code, error: err.message, options: [] }));
}

export default async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.statusCode = 204; return res.end(); }
  const q = new URL(req.url, 'http://localhost').searchParams;
  let id = q.get('id') || '';
  const link = q.get('url');
  if (!id && link) { const m = link.match(/(?:v=|youtu\.be\/|shorts\/|embed\/|live\/)([\w-]{11})/); if (m) id = m[1]; }
  const itag = q.get('itag');

  // 1) lookup: list the stream options
  if (!itag) {
    try {
      const { data } = await resolve(id);
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('Cache-Control', 'public, s-maxage=1800, max-age=0');
      res.statusCode = 200;
      return res.end(JSON.stringify(data));
    } catch (e) { return sendError(res, e); }
  }

  // 2) stream one format through this server (Range supported)
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      const entry = await resolve(id);
      const src = entry.urls.get(String(itag));
      if (!src) throw new YTError('no_itag', 'This quality (itag ' + itag + ') is not available for this video', 404);
      const name = q.get('dl') ? (entry.data.title || id) + '.mp4' : '';
      try { return await pipeUpstream(req, res, src, name); }
      catch (e) {
        if (attempt === 0 && e.upstream && [403, 404, 410].includes(e.upstream) && !res.headersSent) { CACHE.delete(id); continue; }
        throw e;
      }
    }
  } catch (e) {
    if (res.headersSent) { try { res.end(); } catch (_) {} return; }
    sendError(res, e);
  }
}
