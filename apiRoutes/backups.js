import express from 'express';
import { listBackups, createBackup, loadBackup } from '../services/backupService.js';

const router = express.Router();

function requireAuth(req, res, next) {
    if (!req.isAuthenticated()) {
        return res.status(401).json({ loggedIn: false, error: 'User not authenticated' });
    }
    next();
}

router.get('/', requireAuth, (req, res) => {
    try {
        const data = listBackups();
        res.json(data);
    } catch (err) {
        console.error('Failed to list backups:', err);
        res.status(500).json({ error: 'Failed to list backups' });
    }
});

router.post('/', requireAuth, async (req, res) => {
    try {
        const backup = await createBackup('manual');
        const data = listBackups();
        res.status(201).json({
            backup,
            ...data
        });
    } catch (err) {
        console.error('Failed to create manual backup:', err);
        res.status(500).json({ error: err.message || 'Failed to create backup' });
    }
});

router.post('/load', requireAuth, express.json(), async (req, res) => {
    const filename = req.body?.filename;
    if (!filename) {
        return res.status(400).json({ error: 'Backup filename is required' });
    }

    try {
        const restored = await loadBackup(filename);
        const data = listBackups();
        res.json({
            message: 'Backup loaded successfully',
            restored,
            ...data
        });
    } catch (err) {
        console.error('Failed to load backup:', err);
        res.status(400).json({ error: err.message || 'Failed to load backup' });
    }
});

export default router;
