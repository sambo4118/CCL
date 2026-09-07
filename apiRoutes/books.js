import express from 'express';
import multer from 'multer';
import { eq, count, like, or } from 'drizzle-orm';
import { db } from '../database/index.js';
import { books } from '../database/schema.js';
import { importBooks } from '../services/importBooks.js';
import { getBookCover } from '../services/getBookCovers.js';
import { fetchBookInfoExternal } from '../services/fetchBookInfoExternal.js';

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

// Columns to return in JSON responses (everything except the cover blob).
// The blob is served separately via /api/books/:id/cover.
const bookListColumns = {
    id: books.id,
    localNumber: books.localNumber,
    title: books.title,
    subtitle: books.subtitle,
    authorId: books.authorId,
    author: books.author,
    call1: books.call1,
    call2: books.call2,
    publisher: books.publisher,
    published: books.published,
    isbn: books.isbn,
    bookLocation: books.bookLocation,
    blurb: books.blurb,
};

const withCoverUrl = (book) => book && { ...book, coverUrl: `/api/books/${book.id}/cover` };

// GET /api/books — list all books
booksRoute.get('/', async (req, res) => {
    try {
        const all = await db.select(bookListColumns).from(books);
        res.json(all.map(withCoverUrl));
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

// SEARCH /api/books/search?q=... — search books
booksRoute.get('/search', async (req, res) => {
    const q = req.query.q?.toString().trim();
    if (!q) return res.status(400).json({ error: 'Missing search query' });
    try {
        const pattern = `%${q}%`;
        const results = await db.select(bookListColumns).from(books).where(
            or(
                like(books.title, pattern),
                like(books.author, pattern),
                like(books.isbn, pattern),
                like(books.publisher, pattern),
                like(books.localNumber, pattern),
            )
        );
        res.json(results.map(withCoverUrl));
    } catch (error) {
        console.error('Error searching books:', error);
        res.status(500).json({ error: 'Failed to search books', details: error.message });
    }
});

// GET /api/books/:id/cover — serve book cover image bytes
booksRoute.get('/:id/cover', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).end();
    try {
        const [book] = await db.select().from(books).where(eq(books.id, id));
        if (!book) return res.status(404).end();

        // Use cached blob, or fetch + store on first request.
        let buffer = book.coverImage;
        if (!buffer) {
            buffer = await getBookCover(book);
        }
        if (!buffer) return res.status(404).end();

        res.set('Content-Type', 'image/jpeg');
        res.set('Cache-Control', 'public, max-age=86400');
        res.send(buffer);
    } catch (error) {
        console.error('Error serving book cover:', error);
        res.status(500).end();
    }
});

//GET /api/books/external/:isbn — fetch book info from external API (Google Books)
booksRoute.get('/external/:isbn', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const isbn = req.params.isbn.trim();
    if (!isbn) return res.status(400).json({ error: 'Missing ISBN' });

    try {
        const data = await fetchBookInfoExternal(isbn);
        if (!data || !data.items || data.items.length === 0) return res.status(404).json({ error: 'Book not found in external API' });
        
        const volumeInfo = data.items[0].volumeInfo;
       
        return res.json({
            title: volumeInfo.title ?? null,
            subtitle: volumeInfo.subtitle ?? null,
            authors: volumeInfo.authors ?? [],
            publisher: volumeInfo.publisher ?? null,
            publishedDate: volumeInfo.publishedDate ?? null,
            description: volumeInfo.description ?? null,
            pageCount: volumeInfo.pageCount ?? null,
            categories: volumeInfo.categories ?? [],
            coverUrl: volumeInfo.imageLinks?.thumbnail || volumeInfo.imageLinks?.smallThumbnail || null,
            raw: volumeInfo
        });


    } catch (error) {
        console.error('Error fetching external book info:', error);
        res.status(500).json({ error: 'Failed to fetch external book info', details: error.message });
    }

});

// GET /api/books/:id — fetch a single book
booksRoute.get('/:id', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    try {
        const [book] = await db.select(bookListColumns).from(books).where(eq(books.id, id));
        if (!book) return res.status(404).json({ error: 'Book not found' });
        res.json(withCoverUrl(book));
    } catch (error) {
        console.error('Error fetching book:', error);
        res.status(500).json({ error: 'Failed to fetch book' });
    }
});



export default booksRoute;
