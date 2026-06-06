import express from 'express';
import { db } from '../database/index.js';
import { eq } from 'drizzle-orm';
import { allowedEmails } from '../database/schema.js';

const router = express.Router();

function requireAuth(req, res, next) {
    if (!req.isAuthenticated()) {
        return res.status(401).json({ loggedIn: false, message: 'User not authenticated' });
    }
    next();
}

router.get('/', (req, res) => {
    if (!req.isAuthenticated()) {
        return res.status(401).json({ loggedIn: false, message: 'User not authenticated' });
    }
    const users = db.select().from(allowedEmails).all();
    res.json({ loggedIn: true, users });
});

router.post('/', requireAuth, express.json(), (req, res) => {
    const rawEmail = (req.body && req.body.email) || '';
    const email = String(rawEmail).trim().toLowerCase();

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        return res.status(400).json({ message: 'Invalid email' });
    }

    try {
        db.insert(allowedEmails)
            .values({ email })
            .onConflictDoNothing()
            .run();
        const [user] = db.select().from(allowedEmails).where(eq(allowedEmails.email, email)).all();
        res.status(201).json({ user });
    } catch (err) {
        console.error('Failed to add allowed email:', err);
        res.status(500).json({ message: 'Failed to add user' });
    }
});

router.delete('/:email', requireAuth, (req, res) => {
    const email = String(req.params.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ message: 'Invalid email' });

    try {
        const result = db.delete(allowedEmails).where(eq(allowedEmails.email, email)).run();
        if (!result.changes) {
            return res.status(404).json({ message: 'User not found' });
        }
        res.json({ message: 'User removed' });
    } catch (err) {
        console.error('Failed to remove allowed email:', err);
        res.status(500).json({ message: 'Failed to remove user' });
    }
});

export default router;
