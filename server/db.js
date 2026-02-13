import pg from 'pg';
import { readFileSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';

// Parse ~/.pgpass to get password
function getPgPassword() {
  try {
    const pgpassPath = join(homedir(), '.pgpass');
    const content = readFileSync(pgpassPath, 'utf-8');
    // pgpass can have multiple lines; find the matching one
    const lines = content.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
    for (const line of lines) {
      // Format: hostname:port:database:username:password
      // Password may contain colons, so split only first 4
      const parts = line.split(':', 4);
      if (parts.length < 4) continue;
      const [host, port, db, user] = parts;
      // Check if this line matches our connection
      if ((host === '*' || host === '13.201.42.25') &&
          (port === '*' || port === '5432') &&
          (db === '*' || db === 'masterdata') &&
          (user === '*' || user === 'rohit')) {
        const password = line.slice(parts.join(':').length + 1);
        return password;
      }
    }
    console.error('No matching entry found in ~/.pgpass');
    return process.env.DB_PASSWORD || '';
  } catch (err) {
    console.error('Failed to read ~/.pgpass:', err.message);
    return process.env.DB_PASSWORD || '';
  }
}

const pool = new pg.Pool({
  host: '13.201.42.25',
  port: 5432,
  database: 'masterdata',
  user: 'rohit',
  password: getPgPassword(),
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 60000,
});

pool.on('error', (err) => {
  console.error('Unexpected PG pool error:', err);
});

export async function query(sql, params = []) {
  const start = Date.now();
  const result = await pool.query(sql, params);
  const duration = Date.now() - start;
  if (duration > 5000) {
    console.warn(`Slow query (${duration}ms):`, sql.substring(0, 80));
  }
  return result;
}

export async function testConnection() {
  try {
    const res = await pool.query('SELECT NOW()');
    console.log('DB connected:', res.rows[0].now);
    return true;
  } catch (err) {
    console.error('DB connection failed:', err.message);
    return false;
  }
}

export default pool;
