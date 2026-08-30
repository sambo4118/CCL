import express from 'express';
import { eq, count, inArray, notInArray, and } from 'drizzle-orm';
import { db } from '../database/index.js';
import { classes, students } from '../database/schema.js';
import multer from 'multer';

const upload = multer({ storage: multer.memoryStorage() });

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

// GET api/classes/:id get all the class details available
classesRoute.get('/:id', async (req, res) => {
    const classId = req.params.id;
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    let classInfo;
    try {
        classInfo = await db
            .select({
                id: classes.id,
                name: classes.name,
                teacherName: classes.teacherName,
                studentCount: count(students.id),
                classPhoto: classes.image,
            })
            .from(classes)
            .leftJoin(students, eq(students.classId, classes.id))
            .where(eq(classes.id, classId))
            .groupBy(classes.id)
            .get();

        if (!classInfo) {
            return res.status(404).json({ error: 'Class not found' });
        }
    } catch (error) {
        console.error('Error fetching class info:', error);
        return res.status(500).json({ error: 'Failed to fetch class info' });
    }

    let studentsInClass;
    try {
        studentsInClass = await db
            .select({
                id: students.id,
                name: students.name,
            })
            .from(students)
            .where(eq(students.classId, classId))
            .all();
    } catch (error) {
        console.error('Error fetching students in class:', error);
        return res.status(500).json({ error: 'Failed to fetch students in class' });
    }
    
    if (classInfo.image) {
        classInfo.image = `data:image/jpeg;base64,${classInfo.image.toString('base64')}`;
    }

    res.json({ class: classInfo, students: studentsInClass });
});

// PUT api/classes/:id 
classesRoute.put('/:id', upload.single('image'), async (req, res) => {
    const classId = req.params.id;
    
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });

    const { name, teacherName } = req.body;
    
    const updateData = {};
    
    if (name && name !== 'undefined' && name !== 'null') updateData.name = name;
    if (teacherName && teacherName !== 'undefined' && teacherName !== 'null') updateData.teacherName = teacherName;
    if (req.file) updateData.image = req.file.buffer;

    let studentIds = null;
    if (req.body.studentIds !== undefined && req.body.studentIds !== 'undefined') {
        try {
            studentIds = typeof req.body.studentIds === 'string' 
                ? JSON.parse(req.body.studentIds) 
                : req.body.studentIds;
        } catch (error) {
            return res.status(400).json({ error: `Invalid JSON for studentIds: ${error.message}` });
        }
        if (!Array.isArray(studentIds)) {
            return res.status(400).json({ error: 'studentIds must be an array' });
        }
        studentIds = studentIds.map(Number).filter(id => !isNaN(id));
    }

    const hasClassUpdates = Object.keys(updateData).length > 0;
    const hasStudentUpdates = studentIds !== null;

    if (!hasClassUpdates && !hasStudentUpdates) {
        return res.status(400).json({ error: 'No valid data provided to update' });
    }

    try {
        const updatedClass = await db.transaction((transaction) => {
            let classRecord = null;
            if (hasClassUpdates) {
                classRecord = transaction
                    .update(classes)
                    .set(updateData)
                    .where(eq(classes.id, classId))
                    .returning()
                    .get();
            } else {
                classRecord = transaction
                    .select()
                    .from(classes)
                    .where(eq(classes.id, classId))
                    .get();
            }

            if (!classRecord) throw new Error('CLASS_NOT_FOUND');
            if (!hasStudentUpdates) return classRecord;

            if (studentIds.length === 0) {
                transaction.update(students)
                    .set({ classId: null })
                    .where(eq(students.classId, classId))
                    .run();
                return classRecord;
            }

            const existing = transaction
                .select({ id: students.id })
                .from(students)
                .where(inArray(students.id, studentIds))
                .all();
            
            if (existing.length !== studentIds.length) {
                throw new Error('INVALID_STUDENT_ID');
            }

            transaction.update(students)
                .set({ classId: null })
                .where(
                    and(
                        eq(students.classId, classId),
                        notInArray(students.id, studentIds)
                    )
                )
                .run()
            
            transaction.update(students)
                .set({ classId: Number(classId) })
                .where(inArray(students.id, studentIds))
                .run();
            
            return classRecord;
        });

        return res.json(updatedClass);

    } catch (error) {
        if (error.message === 'CLASS_NOT_FOUND') {
            return res.status(404).json({ error: 'Class not found' });
        }
        if (error.message === 'INVALID_STUDENT_ID') {
            return res.status(400).json({ error: 'One or more selected student IDs do not exist' });
        }
        console.error('--- Drizzle DB Error ---', error);
        return res.status(500).json({ error: 'Failed to update class in database' });
    }
});

export default classesRoute;
