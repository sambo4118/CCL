import express from 'express';
import multer from 'multer';
import { eq, count } from 'drizzle-orm';
import { db } from '../database/index.js';
import { books } from '../database/schema.js';
import { importBooks } from '../services/importBooks.js';

const booksRoute = express.Router();

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
});

// POST /api/books/import — bulk import from CSV/XLSX/XML
booksRoute.post('/import', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    try {
        const result = await importBooks(req.file);
        res.json({ success: true, result });
    } catch (error) {
        console.error('Error importing books:', error);
        res.status(500).json({ error: 'Failed to import books', details: error.message });
    }
});

// GET /api/books — list all books
booksRoute.get('/', async (req, res) => {
    try {
        const all = await db.select().from(books);
        res.json(all);
    } catch (error) {
        console.error('Error listing books:', error);
        res.status(500).json({ error: 'Failed to list books' });
    }
});

// GET /api/books/count — count all books
booksRoute.get('/count', async (req, res) => {
    try {
        const [result] = await db.select({ count: count() }).from(books);
        res.json({ count: Number(result?.count ?? 0) });
    } catch (error) {
        console.error('Error counting books:', error);
        res.status(500).json({ error: 'Failed to count books' });
    }
});

// GET /api/books/:id — fetch a single book
booksRoute.get('/:id', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    try {
        const [book] = await db.select().from(books).where(eq(books.id, id));
        if (!book) return res.status(404).json({ error: 'Book not found' });
        res.json(book);
    } catch (error) {
        console.error('Error fetching book:', error);
        res.status(500).json({ error: 'Failed to fetch book' });
    }
});

// DELETE /api/books/:id — remove a book
booksRoute.delete('/:id', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    try {
        await db.delete(books).where(eq(books.id, id));
        res.json({ success: true });
    } catch (error) {
        console.error('Error deleting book:', error);
        res.status(500).json({ error: 'Failed to delete book' });
    }
});

export default booksRoute;
