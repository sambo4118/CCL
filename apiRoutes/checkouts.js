import express from 'express';
import multer from 'multer';
import { and, eq, isNull, desc, count, notInArray } from 'drizzle-orm';
import { db } from '../database/index.js';
import { checkouts, students, authors, classes, books } from '../database/schema.js';

const checkoutsRoute = express.Router();

const checkoutsObjectShape = {
    id: checkouts.id,
    checkoutDate: checkouts.checkoutDate,
    returnDate: checkouts.returnDate,
    duration: checkouts.duration,
    studentId: checkouts.studentId,
    studentName: students.name,
    bookTitle: checkouts.bookId,
    classId: classes.id,
    className: classes.name,
    teacherName: classes.teacherName,
    bookId: books.id,
    bookTitle: books.title,
    bookAuthor: books.author,
    localNumber: books.localNumber,
};

// POST /api/checkouts — create a new checkout
checkoutsRoute.post('/', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const studentId = Number(req.body.studentId);
    const bookId = Number(req.body.bookId);
    const duration = Number(req.body.duration) || 14;

    if (!Number.isInteger(studentId) || !Number.isInteger(bookId)) {
        return res.status(400).json({ error: 'Valid studentId and bookId are required' });
    }

    try {
        const existing = await db
            .select({ id: checkouts.id })
            .from(checkouts)
            .where(and(eq(checkouts.bookId, bookId), isNull(checkouts.returnDate)));

        if (existing.length > 0) {
            return res.status(409).json({ error: 'This book is already checked out' });
        }

        const checkoutDate = new Date().toISOString().replace('T', ' ').substring(0, 19);
        const [inserted] = await db
            .insert(checkouts)
            .values({
                studentId,
                bookId,
                duration,
                checkoutDate,
            })
            .returning();

        const [result] = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(eq(checkouts.id, inserted.id));

        return res.status(201).json({
            ...result,
            bookCoverUrl: `/api/books/${result.bookId}/cover`,
        });
    } catch (error) {
        console.error('Error creating checkout:', error);
        return res.status(500).json({ error: 'Failed to create checkout' });
    }
});

// GET /api/checkouts/count — count checkouts (public, no auth required)
checkoutsRoute.get('/count', async (req, res) => {
    try {
        const [[totalResult], [activeResult]] = await Promise.all([
            db.select({ count: count() }).from(checkouts),
            db.select({ count: count() }).from(checkouts).where(isNull(checkouts.returnDate)),
        ]);
        const total = Number(totalResult?.count ?? 0);
        const activeCount = Number(activeResult?.count ?? 0);
        return res.json({
            count: total,
            activeCount,
            returnedCount: Math.max(0, total - activeCount),
        });
    } catch (error) {
        console.error('Error counting checkouts:', error);
        return res.status(500).json({ error: 'Failed to count checkouts' });
    }
});

// GET /api/checkouts/top-books — top N most checked out books not currently checked out (public, no auth required)
checkoutsRoute.get('/top-books', async (req, res) => {
    const limit = Math.min(Math.max(Number(req.query.limit) || 5, 1), 50);
    try {
        const activeCheckoutsSubquery = db
            .select({ bookId: checkouts.bookId })
            .from(checkouts)
            .where(isNull(checkouts.returnDate));

        const rows = await db
            .select({
                id: books.id,
                localNumber: books.localNumber,
                title: books.title,
                subtitle: books.subtitle,
                authorId: books.authorId,
                author: books.author,
                publisher: books.publisher,
                published: books.published,
                isbn: books.isbn,
                bookLocation: books.bookLocation,
                blurb: books.blurb,
                checkoutCount: count(checkouts.id),
            })
            .from(books)
            .leftJoin(checkouts, eq(books.id, checkouts.bookId))
            .where(notInArray(books.id, activeCheckoutsSubquery))
            .groupBy(books.id)
            .orderBy(desc(count(checkouts.id)))
            .limit(limit);

        return res.json(rows.map(row => ({
            ...row,
            coverUrl: `/api/books/${row.id}/cover`,
            checkoutCount: Number(row.checkoutCount ?? 0),
        })));
    } catch (error) {
        console.error('Error fetching top books from checkouts:', error);
        return res.status(500).json({ error: 'Failed to fetch top books' });
    }
});

// GET /api/checkouts/outstanding — list all outstanding checkouts
checkoutsRoute.get('/outstanding', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    try {
        const rows = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(isNull(checkouts.returnDate))
            .orderBy(desc(checkouts.checkoutDate));
        return res.json(rows.map(row => ({
            ...row,
            bookCoverUrl: `/api/books/${row.bookId}/cover`,
        })));
    } catch (error) {
        console.error('Error fetching outstanding checkouts:', error);
        return res.status(500).json({ error: 'Failed to fetch outstanding checkouts' });
    }

});

// GET /api/checkouts/book/:id — list all checkouts for a specific book
checkoutsRoute.get('/book/:id', async (req, res) => {
    if(!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    const bookId = req.params.id;
    try {
        const rows = await db
        .select(checkoutsObjectShape)
        .from(checkouts)
        .innerJoin(students, eq(checkouts.studentId, students.id))
        .innerJoin(books, eq(checkouts.bookId, books.id))
        .leftJoin(classes, eq(students.classId, classes.id))
        .where(eq(checkouts.bookId, bookId))
        .orderBy(desc(checkouts.checkoutDate));
        return res.json(rows.map(row => ({
            ...row,
            bookCoverUrl: `/api/books/${row.bookId}/cover`,
        })));
    } catch (error) {
        console.error(`Failed to fetch checkouts for book id: ${bookId}`, error);
        return res.status(500).json({ error: `Failed to fetch checkouts for book id: ${bookId}`});
    }
});

// GET /api/checkouts/book/:id/recent — get the most recent checkout for a specific book
checkoutsRoute.get('/book/:id/recent', async (req, res) => {
    if (!req.isAuthenticated()) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    const bookId = req.params.id;

    try {
        const rows = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(and(eq(checkouts.bookId, bookId), isNull(checkouts.returnDate)))
            .orderBy(desc(checkouts.checkoutDate))
            .limit(1);

        return res.json(rows[0] ?? null);
    } catch (error) {
        console.error(`Failed to fetch recent checkout for book id: ${bookId}`, error);
        return res.status(500).json({
            error: `Failed to fetch recent checkout for book id: ${bookId}`
        });
    }
});

// GET /api/checkouts/student/:id — list all checkouts for a specific student
checkoutsRoute.get('/student/:id', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    const studentId = req.params.id;
    try {
        const rows = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(eq(checkouts.studentId, studentId))
            .orderBy(desc(checkouts.checkoutDate));
        return res.json(rows.map(row => ({
            ...row,
            bookCoverUrl: `/api/books/${row.bookId}/cover`,
        })));
    } catch (error) {
        console.error(`Failed to fetch checkouts for student id: ${studentId}`, error);
        return res.status(500).json({ error: `Failed to fetch checkouts for student id: ${studentId}` });
    }
});

// POST /api/checkouts/:id/return — mark a checkout as returned
checkoutsRoute.post('/:id/return', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const id = Number(req.params.id);
    if (!Number.isInteger(id)) return res.status(400).json({ error: 'Invalid id' });

    try {
        const [checkout] = await db
            .select({ returnDate: checkouts.returnDate })
            .from(checkouts)
            .where(eq(checkouts.id, id));

        if (!checkout) return res.status(404).json({ error: 'Checkout not found' });
        if (checkout.returnDate) {
            return res.status(409).json({ error: 'Checkout has already been returned' });
        }

        const returnDate = new Date().toISOString().replace('T', ' ').substring(0, 19);
        const [updated] = await db
            .update(checkouts)
            .set({ returnDate })
            .where(eq(checkouts.id, id))
            .returning();

        if (!updated) return res.status(404).json({ error: 'Checkout not found' });

        const [result] = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(eq(checkouts.id, id));

        return res.json({
            ...result,
            bookCoverUrl: `/api/books/${result.bookId}/cover`,
        });
    } catch (error) {
        console.error(`Failed to return checkout id: ${id}`, error);
        return res.status(500).json({ error: 'Failed to return checkout' });
    }
});

// PUT /api/checkouts/:id — update an existing checkout
checkoutsRoute.put('/:id', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const id = Number(req.params.id);
    if (!Number.isInteger(id)) return res.status(400).json({ error: 'Invalid checkout id' });

    try {
        const [existing] = await db
            .select()
            .from(checkouts)
            .where(eq(checkouts.id, id));

        if (!existing) return res.status(404).json({ error: 'Checkout not found' });

        const studentId = req.body.studentId !== undefined ? Number(req.body.studentId) : existing.studentId;
        const bookId = req.body.bookId !== undefined ? Number(req.body.bookId) : existing.bookId;
        const duration = req.body.duration !== undefined ? (Number(req.body.duration) || 14) : existing.duration;
        const checkoutDate = (req.body.checkoutDate !== undefined && String(req.body.checkoutDate).trim() !== '')
            ? String(req.body.checkoutDate).trim()
            : existing.checkoutDate;
        const returnDate = req.body.returnDate !== undefined
            ? (req.body.returnDate === null || String(req.body.returnDate).trim() === ''
                ? null
                : String(req.body.returnDate).trim())
            : existing.returnDate;

        if (!Number.isInteger(studentId) || !Number.isInteger(bookId)) {
            return res.status(400).json({ error: 'Valid studentId and bookId are required' });
        }

        if (returnDate === null) {
            const activeForBook = await db
                .select({ id: checkouts.id })
                .from(checkouts)
                .where(and(eq(checkouts.bookId, bookId), isNull(checkouts.returnDate)));
            if (activeForBook.some(c => c.id !== id)) {
                return res.status(409).json({ error: 'This book already has an active checkout' });
            }
        }

        await db
            .update(checkouts)
            .set({
                studentId,
                bookId,
                duration,
                checkoutDate,
                returnDate,
            })
            .where(eq(checkouts.id, id));

        const [result] = await db
            .select(checkoutsObjectShape)
            .from(checkouts)
            .innerJoin(students, eq(checkouts.studentId, students.id))
            .innerJoin(books, eq(checkouts.bookId, books.id))
            .leftJoin(classes, eq(students.classId, classes.id))
            .where(eq(checkouts.id, id));

        return res.json({
            ...result,
            bookCoverUrl: `/api/books/${result.bookId}/cover`,
        });
    } catch (error) {
        console.error(`Failed to update checkout id: ${id}`, error);
        return res.status(500).json({ error: 'Failed to update checkout' });
    }
});

export default checkoutsRoute;