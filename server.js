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
const PORT = process.env.PORT || 3000;
const IP_ADDRESS = process.env.IP_ADDRESS || 'localhost';

setupSessionStore(app);
setupPassport(app);
setupAuthRoutes(app, sendPage, __dirname);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.set('trust proxy', 1);

// #region API Routes

import meRoute from './apiRoutes/me.js';
import booksRoute from './apiRoutes/books.js';
import usersRoute from './apiRoutes/users.js';
import studentsRoute from './apiRoutes/students.js';
import classesRoute from './apiRoutes/classes.js';

app.use('/api/books', booksRoute);

app.use('/api/me', meRoute);

app.use('/api/users', usersRoute);

app.use('/api/students', studentsRoute);

app.use('/api/classes', classesRoute);

// #endregion

// add navbar to the top
const navbar = fs.readFileSync(path.join(__dirname, 'views', 'navbar.html'), 'utf-8');

// #region Page Routes

app.get('/', (req, res) => sendPage(res, 'index.html'));

app.get('/settings', (req, res) => {
    if (!req.isAuthenticated()) {
        return res.redirect('/?denied=settings');
    }
    sendPage(res, 'settings.html');
});

app.get('/dashboard', (req, res) => sendPage(res, 'dashboard.html'));

app.get('/search', (req, res) => sendPage(res, 'search.html'));

app.get('/books/:id', (req, res) => sendPage(res, 'bookView.html'));

app.get('/classes/:id', (req, res) => sendPage(res, 'classView.html'));

app.get('/students/:id', (req, res) => sendPage(res, 'studentView.html'));

// #endregion

app.listen(PORT, IP_ADDRESS, () => {
    console.log(`Server running on http://${IP_ADDRESS}:${PORT}`);
});
