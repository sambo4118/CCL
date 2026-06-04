import * as XLSX from 'xlsx'; 
import { db } from '../database/index.js';
import { books, authors } from '../database/schema.js';

export async function importBooks(file) {
    const normalizeKey = (key) => String(key).toLowerCase().replace(/[^a-z0-9]+/g, '');
    const normalizeRow = (row) => Object.fromEntries(Object.entries(row).map(([key, value]) => [normalizeKey(key), value]));
    const pick = (row, keys) => { for (const key of keys) if (row[key] != null && String(row[key]).trim() !== '') return row[key]; return null; };
    const extension = file.originalname.split('.').pop().toLowerCase();
    let rows;

    if (extension === 'csv' || extension === 'xls' || extension === 'xlsx') {
        const workbook = XLSX.read(file.buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        rows = XLSX.utils.sheet_to_json(sheet);
    } else {
        throw new Error(`Unsupported file type: ${extension}`);
    }

    const normalizedRows = rows.map(normalizeRow);
    const mappedBooks = [];
    const skippedRows = [];

    normalizedRows.forEach((row, index) => {
        const sourceRow = index + 2;
        const localnumber = String(pick(row, ['localnumber', 'id']) ?? '').trim();
        const title = String(pick(row, ['title']) ?? '').trim();
        const author = String(pick(row, ['author', 'authors']) ?? '').trim();

        const reasons = [];
        if (!localnumber) reasons.push('missing localnumber');
        if (!title) reasons.push('missing title');

        if (reasons.length > 0) {
            skippedRows.push({ sourceRow, reasons, localnumber, title, author });
            return;
        }

        mappedBooks.push({
            localNumber: localnumber,
            title,
            subtitle: pick(row, ['subtitle']) ? String(pick(row, ['subtitle'])).trim() : null,
            author: author || null,
            call1: pick(row, ['call1']) ? String(pick(row, ['call1'])).trim() : null,
            call2: pick(row, ['call2']) ? String(pick(row, ['call2'])).trim() : null,
            publisher: pick(row, ['publisher']) ? String(pick(row, ['publisher'])).trim() : null,
            published: pick(row, ['published']) ? parseYear(pick(row, ['published'])) : null,
            isbn: pick(row, ['isbn']) ? String(pick(row, ['isbn'])).trim() : null,
            bookLocation: pick(row, ['booklocation', 'location']) ? String(pick(row, ['booklocation', 'location'])).trim() : null
        });
    });

    console.log(`[importBooks] total rows: ${rows.length}, valid rows: ${mappedBooks.length}, skipped rows: ${skippedRows.length}`);
    if (skippedRows.length > 0) {
        console.warn('[importBooks] first skipped rows:', skippedRows.slice(0, 10));
    }

    if (mappedBooks.length === 0) {
        throw new Error('No valid rows found in import file. Ensure required columns include Local Number and Title.');
    }

    db.transaction((tx) => {
        tx.delete(books).run();
        tx.delete(authors).run();

        const uniqueAuthorNames = [...new Set(mappedBooks.map((book) => book.author).filter(Boolean))];
        for (let i = 0; i < uniqueAuthorNames.length; i += 200) {
            const authorChunk = uniqueAuthorNames.slice(i, i + 200).map((name) => ({ name }));
            tx.insert(authors).values(authorChunk).run();
        }

        const authorRows = tx.select({ id: authors.id, name: authors.name }).from(authors).all();
        const authorIdByName = new Map(authorRows.map((row) => [row.name, row.id]));

        const booksWithAuthorIds = mappedBooks.map((book) => ({
            ...book,
            authorId: book.author ? (authorIdByName.get(book.author) ?? null) : null,
        }));

        for (let i = 0; i < booksWithAuthorIds.length; i += 200) {
            const chunk = booksWithAuthorIds.slice(i, i + 200);
            const startRow = i + 1;
            const endRow = i + chunk.length;
            console.log(`[importBooks] inserting rows ${startRow}-${endRow} of ${booksWithAuthorIds.length}`);
            try {
                tx.insert(books).values(chunk).run();
            } catch (error) {
                const first = chunk[0];
                const last = chunk[chunk.length - 1];
                throw new Error(
                    `Insert failed for mapped rows ${startRow}-${endRow}. ` +
                    `First localNumber=${first?.localNumber ?? 'null'}, last localNumber=${last?.localNumber ?? 'null'}. ` +
                    `Original error: ${error.message}`
                );
            }
        }
    });

    return { importedCount: mappedBooks.length, skippedCount: rows.length - mappedBooks.length };

}


function parseYear(value) {
    if (value == null || value === '') return null;
    const match = String(value).match(/(\d{4})/);
    return match ? Number(match[0]) : null;
}