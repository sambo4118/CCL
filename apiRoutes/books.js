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

// POST /api/books — create a new book
booksRoute.post('/', upload.single('cover'), async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    try {
        const { title, subtitle, author, authorId, localNumber, publisher, published, isbn, bookLocation, blurb, call1, call2 } = req.body;
        if (!title || !localNumber) {
            return res.status(400).json({ error: 'Title and local number are required' });
        }

        const values = {
            title: title.trim(),
            subtitle: subtitle?.trim() || null,
            author: author?.trim() || null,
            authorId: authorId ? Number(authorId) : null,
            localNumber: localNumber.trim(),
            publisher: publisher?.trim() || null,
            published: published ? Number(published) : null,
            isbn: isbn?.trim() || null,
            bookLocation: bookLocation?.trim() || null,
            blurb: blurb?.trim() || null,
            call1: call1?.trim() || null,
            call2: call2?.trim() || null,
        };

        if (req.file) {
            values.coverImage = req.file.buffer;
        }

        const [newBook] = await db.insert(books).values(values).returning();
        return res.status(201).json(withCoverUrl(newBook));
    } catch (error) {
        console.error('Error creating book:', error);
        return res.status(500).json({ error: 'Failed to create book' });
    }
});

// PUT /api/books/:id — update a book
booksRoute.put('/:id', upload.single('cover'), async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });

    try {
        const { title, subtitle, author, authorId, localNumber, publisher, published, isbn, bookLocation, blurb, call1, call2 } = req.body;
        const updates = {};
        if (title !== undefined) updates.title = title.trim();
        if (subtitle !== undefined) updates.subtitle = subtitle?.trim() || null;
        if (author !== undefined) updates.author = author?.trim() || null;
        if (authorId !== undefined) updates.authorId = authorId ? Number(authorId) : null;
        if (localNumber !== undefined) updates.localNumber = localNumber.trim();
        if (publisher !== undefined) updates.publisher = publisher?.trim() || null;
        if (published !== undefined) updates.published = published ? Number(published) : null;
        if (isbn !== undefined) updates.isbn = isbn?.trim() || null;
        if (bookLocation !== undefined) updates.bookLocation = bookLocation?.trim() || null;
        if (blurb !== undefined) updates.blurb = blurb?.trim() || null;
        if (call1 !== undefined) updates.call1 = call1?.trim() || null;
        if (call2 !== undefined) updates.call2 = call2?.trim() || null;

        if (req.file) {
            updates.coverImage = req.file.buffer;
        }

        const [updated] = await db.update(books).set(updates).where(eq(books.id, id)).returning();
        if (!updated) return res.status(404).json({ error: 'Book not found' });
        return res.json(withCoverUrl(updated));
    } catch (error) {
        console.error('Error updating book:', error);
        return res.status(500).json({ error: 'Failed to update book' });
    }
});

export default booksRoute;
