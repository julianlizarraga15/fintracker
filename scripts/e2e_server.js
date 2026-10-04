const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const root = path.resolve(__dirname, '..');
const venvPython = path.join(root, '.venv', 'bin', 'python');
fs.mkdirSync(path.join(root, 'artifacts', 'e2e'), { recursive: true });
const backendLog = fs.createWriteStream(path.join(root, 'artifacts', 'e2e', 'backend.log'));
const backend = spawn(process.env.PYTHON || (fs.existsSync(venvPython) ? venvPython : 'python3'), ['scripts/e2e_backend.py'], {
  cwd: root,
  env: { ...process.env, PYTHONPATH: root, PYTHON_DOTENV_DISABLED: '1' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
backend.stdout.pipe(backendLog);
backend.stderr.pipe(backendLog);
const types = { '.html': 'text/html; charset=utf-8' };

function proxy(request, response) {
  const upstream = http.request({ hostname: '127.0.0.1', port: 4174, path: request.url.replace(/^\/api/, '') || '/', method: request.method, headers: { ...request.headers, host: '127.0.0.1:4174' } }, (result) => {
    response.writeHead(result.statusCode, result.headers);
    result.pipe(response);
  });
  upstream.on('error', () => { response.writeHead(502); response.end('Test API unavailable'); });
  request.pipe(upstream);
}

const server = http.createServer((request, response) => {
  if (request.url === '/health') { proxy(request, response); return; }
  if (request.url.startsWith('/api/')) { proxy(request, response); return; }
  const filename = request.url.split('?')[0] === '/' ? 'index.html' : path.basename(request.url.split('?')[0]);
  fs.readFile(path.join(root, 'frontend', filename), (error, contents) => {
    if (error) { response.writeHead(404); response.end('Not found'); return; }
    response.writeHead(200, { 'Content-Type': types[path.extname(filename)] || 'application/octet-stream' });
    response.end(contents);
  });
});
server.listen(4173, process.env.E2E_BIND_HOST || '127.0.0.1');
function stop() { server.close(); backend.kill('SIGTERM'); }
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
