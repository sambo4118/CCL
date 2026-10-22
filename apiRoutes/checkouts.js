import express from 'express';
import multer from 'multer';
import { like, eq, isNull, desc } from 'drizzle-orm';
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

export default checkoutsRoute;