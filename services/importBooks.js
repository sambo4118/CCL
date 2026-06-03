import * as XLSX from 'xlsx'; 
import { db } from '../database/index.js';
import { books } from '../database/schema.js';

export async function importBooks(file) {
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

    const mappedBooks = rows.map(row => ({
        id: Number(r.localnumber),
        title: String(r.title ?? '').trim(),
        subtitle: r.subtitle ? String(r.subtitle).trim() : null,
        author: r.author ? String(r.author).trim() : r.authors ? String(r.authors).trim() : String("").trim(),
        call1: r.call1 ? String(r.Call1).trim() : null,
        call2: r.call2 ? String(r.Call2).trim() : null,
        publisher: r.publisher ? String(r.publisher).trim() : null,
        published: r.published ? parseYear(r.published) : null,
        isbn: r.isbn ? String(r.isbn).trim() : null,
        bookLocation: r.booklocation ? String(r.booklocation).trim() : null
    })).filter((book) => Number.isFinite(book.id) && book.title && book.author);

    await db.transaction(async (tx) => {
        await tx.delete(books)
        for (let i = 0; i < mappedBooks.length; i += 200) {
            await tx.insert(books).values(mappedBooks.slice(i, i + 200));
        }
    });

    return { importedCount: mappedBooks.length, skippedCount: rows.length - mappedBooks.length };

}


function parseYear(value) {
    if (value == null || value === '') return null;
    const match = String(value).match(/(\d{4})/);
    return match ? Number(match[0]) : null;
}