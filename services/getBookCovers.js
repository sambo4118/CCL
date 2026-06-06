import 'dotenv/config';
import { db } from '../database/index.js';
import { books } from '../database/schema.js';
import { eq } from 'drizzle-orm';

// ---------- helpers ----------

const cleanIsbn = (raw) => String(raw ?? '').replace(/[^0-9Xx]/gi, '');

async function downloadImage(url) {
    if (!url) return null;
    try {
        const res = await fetch(url.replace(/^http:\/\//, 'https://'));
        if (!res.ok) return null;
        const contentType = res.headers.get('content-type') ?? '';
        if (!contentType.startsWith('image/')) return null;
        const buffer = Buffer.from(await res.arrayBuffer());
        if (buffer.length < 1024) return null;
        return buffer;
    } catch (err) {
        console.warn(`downloadImage failed for ${url}: ${err.message}`);
        return null;
    }
}

async function fromGoogleBooksIsbn(book) {
    const isbn = cleanIsbn(book.isbn);
    if (!isbn) return null;

    const key = process.env.GOOGLE_BOOKS_API_KEY;
    const params = new URLSearchParams({
        q: `isbn:${isbn}`,
        maxResults: '1',
        fields: 'items(volumeInfo/imageLinks)',
    });
    if (key) params.set('key', key);

    try {
        const res = await fetch(`https://www.googleapis.com/books/v1/volumes?${params}`);
        if (!res.ok) return null;
        const data = await res.json();
        const links = data?.items?.[0]?.volumeInfo?.imageLinks;
        const url = links?.extraLarge || links?.large || links?.medium || links?.thumbnail || links?.smallThumbnail;
        return await downloadImage(url);
    } catch (err) {
        console.warn(`Google Books ISBN lookup failed: ${err.message}`);
        return null;
    }
}

async function fromOpenLibraryIsbn(book) {
    const isbn = cleanIsbn(book.isbn);
    if (!isbn) return null;
    // -M (medium, ~180px) matches Google Books thumbnail size; ?default=false → 404 instead of gif placeholder.
    return downloadImage(`https://covers.openlibrary.org/b/isbn/${isbn}-M.jpg?default=false`);
}

async function fromGoogleBooksTitle(book) {
    if (!book.title) return null;

    const key = process.env.GOOGLE_BOOKS_API_KEY;
    let q = `intitle:${book.title}`;
    if (book.author) q += `+inauthor:${book.author}`;
    const params = new URLSearchParams({
        q,
        maxResults: '1',
        fields: 'items(volumeInfo/imageLinks)',
    });
    if (key) params.set('key', key);

    try {
        const res = await fetch(`https://www.googleapis.com/books/v1/volumes?${params}`);
        if (!res.ok) return null;
        const data = await res.json();
        const links = data?.items?.[0]?.volumeInfo?.imageLinks;
        const url = links?.extraLarge || links?.large || links?.medium || links?.thumbnail || links?.smallThumbnail;
        return await downloadImage(url);
    } catch (err) {
        console.warn(`Google Books title search failed: ${err.message}`);
        return null;
    }
}

async function fromOpenLibrarySearch(book) {
    if (!book.title) return null;
    const params = new URLSearchParams({ title: book.title, limit: '1' });
    if (book.author) params.set('author', book.author);

    try {
        const res = await fetch(`https://openlibrary.org/search.json?${params}`);
        if (!res.ok) return null;
        const data = await res.json();
        const coverId = data?.docs?.[0]?.cover_i;
        if (!coverId) return null;
        return downloadImage(`https://covers.openlibrary.org/b/id/${coverId}-M.jpg?default=false`);
    } catch (err) {
        console.warn(`Open Library search failed: ${err.message}`);
        return null;
    }
}

// iTunes returns a small thumbnail by default. Bumping the path component
// from 100x100bb.jpg to a larger size gives a usable cover.
const upscaleItunesArtwork = (url) =>
    url ? url.replace(/\/\d+x\d+bb\./, '/600x600bb.') : null;

async function fromItunesIsbn(book) {
    const isbn = cleanIsbn(book.isbn);
    if (!isbn) return null;
    const params = new URLSearchParams({
        term: isbn,
        media: 'ebook',
        limit: '1',
    });
    try {
        const res = await fetch(`https://itunes.apple.com/search?${params}`);
        if (!res.ok) return null;
        const data = await res.json();
        const url = upscaleItunesArtwork(data?.results?.[0]?.artworkUrl100);
        return await downloadImage(url);
    } catch (err) {
        console.warn(`iTunes ISBN lookup failed: ${err.message}`);
        return null;
    }
}

async function fromItunesTitle(book) {
    if (!book.title) return null;
    const term = book.author ? `${book.title} ${book.author}` : book.title;
    const params = new URLSearchParams({
        term,
        media: 'ebook',
        limit: '1',
    });
    try {
        const res = await fetch(`https://itunes.apple.com/search?${params}`);
        if (!res.ok) return null;
        const data = await res.json();
        const url = upscaleItunesArtwork(data?.results?.[0]?.artworkUrl100);
        return await downloadImage(url);
    } catch (err) {
        console.warn(`iTunes title search failed: ${err.message}`);
        return null;
    }
}

// Order matters: most accurate first, fuzzier fallbacks last.
const providers = [
    { name: 'google-isbn',        fn: fromGoogleBooksIsbn },
    { name: 'openlibrary-isbn',   fn: fromOpenLibraryIsbn },
    { name: 'itunes-isbn',        fn: fromItunesIsbn },
    { name: 'google-title',       fn: fromGoogleBooksTitle },
    { name: 'openlibrary-search', fn: fromOpenLibrarySearch },
    { name: 'itunes-title',       fn: fromItunesTitle },
];

// ---------- public API ----------

export async function getBookCover(book) {
    if (book.coverImage) return book.coverImage;

    const attempts = [];
    for (const { name, fn } of providers) {
        const buffer = await fn(book);
        if (buffer) {
            attempts.push(`${name}=hit`);
            db.update(books)
                .set({ coverImage: buffer })
                .where(eq(books.id, book.id))
                .run();
            console.log(`✓ "${book.title}" cover via ${name} [tried: ${attempts.join(', ')}]`);
            return buffer;
        }
        attempts.push(`${name}=miss`);
    }

    console.warn(`✗ no cover for "${book.title}" (isbn=${book.isbn ?? '—'}) [tried: ${attempts.join(', ')}]`);
    return false;
}
