import express from 'express';
import path from 'path';
import fs from "node:fs";
import { fileURLToPath } from 'node:url';
import 'dotenv/config';
import { setupSessionStore, setupPassport, setupAuthRoutes } from './authentication/auth.js';

function sendPage(res, file) {
    let html = fs.readFileSync(path.join(__dirname, 'views', file), 'utf-8');
    html = html.replace('<!-- NAVBAR -->', navbar);
    res.type('html').send(html);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT;

setupSessionStore(app);
setupPassport(app);
setupAuthRoutes(app, sendPage, __dirname);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// #region API Routes

import meRoute from './apiRoutes/me.js';
import booksRoute from './apiRoutes/books.js';

app.use('/api/books', booksRoute);

app.use('/api/me', meRoute);

// #endregion

// add navbar to the top
const navbar = fs.readFileSync(path.join(__dirname, 'views', 'navbar.html'), 'utf-8');

// #region Page Routes

app.get('/', (req, res) => sendPage(res, 'index.html'));

app.get('/settings', (req, res) => sendPage(res, 'settings.html'));

// #endregion

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
