import express from 'express';
import path from 'path';
import fs from "node:fs";
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// API ROUTES
// ============================================================

// example: app.use('/api/books', require('./routes/books'));

const navbar = fs.readFileSync(path.join(__dirname, 'views', 'navbar.html'), 'utf-8');

function sendPage(res, file) {
    let html = fs.readFileSync(path.join(__dirname, 'views', file), 'utf-8');
    html = html.replace('<!-- NAVBAR -->', navbar);
    res.type('html').send(html);
}

app.get('/', (req, res) => sendPage(res, 'index.html'));


app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
