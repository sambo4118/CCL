import { importBooks } from '../services/importBooks.js';
import multer from 'multer';

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 } // limit file size to 10MB
});

import express from 'express';

const importRouter = express.Router();

router.post('/import', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    try {
        const result = await importBooks(req.file.buffer);
        res.json({ success: true, result });
    } catch (error) {
        console.error('Error importing books:', error);
        res.status(500).json({ error: 'Failed to import books', details: error.message });
    }
});

export default { importRouter };
