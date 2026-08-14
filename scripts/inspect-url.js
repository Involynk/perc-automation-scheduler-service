const fs = require('fs');
const path = require('path');

const envContent = fs.readFileSync(path.resolve(__dirname, '../.env'), 'utf8');
const line = envContent.split('\n').find(l => l.startsWith('DATABASE_URL='));
const raw = line.replace(/^DATABASE_URL="?/, '').replace(/"?\r?$/, '');
const u = new URL(raw);

console.log('--- Sanitized DATABASE_URL breakdown ---');
console.log('protocol  :', u.protocol);
console.log('username  :', u.username);       // safe - no password
console.log('hostname  :', u.hostname);
console.log('port      :', u.port);
console.log('database  :', u.pathname);
console.log('params    :', Object.fromEntries(u.searchParams));
console.log('');
console.log('Password present?', u.password ? 'YES (redacted)' : 'NO - MISSING!');
