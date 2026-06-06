import express from 'express';
import { eq, count } from 'drizzle-orm';
import { db } from '../database/index.js';
import { classes, students } from '../database/schema.js';

const classesRoute = express.Router();

// GET /api/classes — list all classes with student counts
classesRoute.get('/', async (req, res) => {
    try {
        const rows = await db
            .select({
                id: classes.id,
                name: classes.name,
                teacherName: classes.teacherName,
                studentCount: count(students.id),
            })
            .from(classes)
            .leftJoin(students, eq(students.classId, classes.id))
            .groupBy(classes.id);

        res.json(rows);
    } catch (error) {
        console.error('Error listing classes:', error);
        res.status(500).json({ error: 'Failed to list classes' });
    }
});

export default classesRoute;
