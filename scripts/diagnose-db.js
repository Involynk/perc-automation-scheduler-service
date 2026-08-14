/**
 * Deep connectivity diagnostic for Supabase PostgreSQL.
 * Runs multiple probes to determine the actual failure mode.
 * Does NOT print credentials.
 */
const net = require('net');
const tls = require('tls');
const fs = require('fs');
const path = require('path');

// Load DATABASE_URL from .env
const envContent = fs.readFileSync(path.resolve(__dirname, '../.env'), 'utf8');
const envLine = envContent.split('\n').find(l => l.startsWith('DATABASE_URL='));
const rawUrl = envLine.replace(/^DATABASE_URL="?/, '').replace(/"?\r?$/, '');
const u = new URL(rawUrl);

const HOST = u.hostname;
const PORT = parseInt(u.port || '5432', 10);
const PASSWORD = decodeURIComponent(u.password);
const USER = u.username;
const DB = u.pathname.replace('/', '');

console.log(`\n=== Supabase Connectivity Diagnostic ===`);
console.log(`Target  : ${HOST}:${PORT}`);
console.log(`User    : ${USER}`);
console.log(`DB      : ${DB}`);
console.log(`SSL     : required\n`);

// --- Test 1: Raw TCP ---
function testTCP() {
  return new Promise((resolve) => {
    const start = Date.now();
    const sock = net.createConnection({ host: HOST, port: PORT, family: 0 }, () => {
      console.log(`[1] TCP connect (dual-stack)  : OK (${Date.now() - start}ms)`);
      sock.end();
      resolve('ok');
    });
    sock.setTimeout(5000);
    sock.on('timeout', () => { console.log(`[1] TCP connect               : TIMEOUT (5s)`); sock.destroy(); resolve('timeout'); });
    sock.on('error', (e) => { console.log(`[1] TCP connect               : FAILED - ${e.message}`); resolve('error'); });
  });
}

// --- Test 2: Raw TCP IPv4 forced ---
function testTCPv4() {
  return new Promise((resolve) => {
    const start = Date.now();
    const sock = net.createConnection({ host: HOST, port: PORT, family: 4 }, () => {
      console.log(`[2] TCP connect (IPv4 forced) : OK (${Date.now() - start}ms)`);
      sock.end();
      resolve('ok');
    });
    sock.setTimeout(5000);
    sock.on('timeout', () => { console.log(`[2] TCP connect (IPv4 forced) : TIMEOUT (5s)`); sock.destroy(); resolve('timeout'); });
    sock.on('error', (e) => { console.log(`[2] TCP connect (IPv4 forced) : FAILED - ${e.message}`); resolve('error'); });
  });
}

// --- Test 3: Raw TCP IPv6 forced ---
function testTCPv6() {
  return new Promise((resolve) => {
    const start = Date.now();
    const sock = net.createConnection({ host: HOST, port: PORT, family: 6 }, () => {
      console.log(`[3] TCP connect (IPv6 forced) : OK (${Date.now() - start}ms)`);
      sock.end();
      resolve('ok');
    });
    sock.setTimeout(5000);
    sock.on('timeout', () => { console.log(`[3] TCP connect (IPv6 forced) : TIMEOUT (5s)`); sock.destroy(); resolve('timeout'); });
    sock.on('error', (e) => { console.log(`[3] TCP connect (IPv6 forced) : FAILED - ${e.message}`); resolve('error'); });
  });
}

// --- Test 4: TLS handshake ---
function testTLS() {
  return new Promise((resolve) => {
    const start = Date.now();
    const sock = tls.connect({ host: HOST, port: PORT, rejectUnauthorized: false }, () => {
      const cipher = sock.getCipher();
      const proto  = sock.getProtocol();
      console.log(`[4] TLS handshake             : OK (${Date.now() - start}ms) cipher=${cipher?.name} proto=${proto}`);
      sock.end();
      resolve('ok');
    });
    sock.setTimeout(8000);
    sock.on('timeout', () => { console.log(`[4] TLS handshake             : TIMEOUT (8s)`); sock.destroy(); resolve('timeout'); });
    sock.on('error', (e) => { console.log(`[4] TLS handshake             : FAILED - ${e.message}`); resolve('error'); });
  });
}

// --- Test 5: PostgreSQL startup packet (wire protocol) ---
// Sends a real PG startup message and reads the server response (auth request or error)
function testPGStartup() {
  return new Promise((resolve) => {
    const start = Date.now();

    // Build PostgreSQL startup message (protocol 3.0)
    const params = { user: USER, database: DB, application_name: 'diag' };
    const paramStr = Object.entries(params)
      .map(([k, v]) => `${k}\0${v}\0`).join('') + '\0';
    const msgLen = 4 + 4 + Buffer.byteLength(paramStr);
    const buf = Buffer.alloc(msgLen);
    buf.writeInt32BE(msgLen, 0);
    buf.writeInt32BE(196608, 4);   // Protocol 3.0 = 0x00030000
    Buffer.from(paramStr).copy(buf, 8);

    const sock = tls.connect({ host: HOST, port: PORT, rejectUnauthorized: false }, () => {
      sock.write(buf);
    });
    sock.setTimeout(8000);

    sock.on('data', (data) => {
      const type = String.fromCharCode(data[0]);
      // R=AuthRequest E=Error N=Notice
      if (type === 'R') {
        const authType = data.readInt32BE(5);
        const authNames = { 0:'OK',2:'Kerberos',3:'CleartextPassword',5:'MD5',10:'SASL' };
        console.log(`[5] PG startup                : OK (${Date.now()-start}ms) → auth=${authNames[authType]||authType}`);
        resolve('ok');
      } else if (type === 'E') {
        // Parse error message fields
        let msg = '';
        let i = 5;
        while (i < data.length - 1) {
          const field = String.fromCharCode(data[i++]);
          const end = data.indexOf(0, i);
          const val = data.slice(i, end).toString();
          i = end + 1;
          if (field === 'M') msg = val;
          if (field === '\0') break;
        }
        console.log(`[5] PG startup                : SERVER ERROR (${Date.now()-start}ms) → ${msg}`);
        resolve('server_error');
      } else {
        console.log(`[5] PG startup                : Unexpected response type '${type}'`);
        resolve('unexpected');
      }
      sock.destroy();
    });
    sock.on('timeout', () => { console.log(`[5] PG startup                : TIMEOUT (8s) - no server response`); sock.destroy(); resolve('timeout'); });
    sock.on('error', (e) => { console.log(`[5] PG startup                : FAILED - ${e.message}`); resolve('error'); });
  });
}

// --- DNS resolution check ---
function testDNS() {
  const dns = require('dns');
  return new Promise((resolve) => {
    dns.resolve(HOST, 'A', (err4, v4) => {
      dns.resolve(HOST, 'AAAA', (err6, v6) => {
        console.log(`[0] DNS A records (IPv4)      : ${err4 ? 'NONE (' + err4.code + ')' : v4.join(', ')}`);
        console.log(`[0] DNS AAAA records (IPv6)   : ${err6 ? 'NONE (' + err6.code + ')' : v6.join(', ')}`);
        resolve({ v4: v4 || [], v6: v6 || [] });
      });
    });
  });
}

async function run() {
  await testDNS();
  await testTCP();
  await testTCPv4();
  await testTCPv6();
  await testTLS();
  await testPGStartup();
  console.log('\n=== Diagnostic complete ===\n');
}
run().catch(console.error);
