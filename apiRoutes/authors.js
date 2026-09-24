import express from 'express';
import { eq, like } from 'drizzle-orm';
import { db } from '../database/index.js';
import { authors } from '../database/schema.js';

const authorsRoute = express.Router();

// GET /api/authors — search or list authors
authorsRoute.get('/', async (req, res) => {
    const searchQuery = req.query.search?.toString().trim();
    try {
        let queryBuilder = db
            .select({
                id: authors.id,
                name: authors.name,
                bookCount: authors.bookCount,
            })
            .from(authors);

        if (searchQuery) {
            queryBuilder = queryBuilder
                .where(like(authors.name, `%${searchQuery}%`))
                .limit(10);
        } else {
            queryBuilder = queryBuilder.limit(20);
        }

        const rows = await queryBuilder;
        return res.json(rows);
    } catch (error) {
        console.error('Error fetching authors:', error);
        return res.status(500).json({ error: 'Failed to fetch authors' });
    }
});

// GET /api/authors/:id — get a single author
authorsRoute.get('/:id', async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id' });
    try {
        const [author] = await db.select().from(authors).where(eq(authors.id, id));
        if (!author) return res.status(404).json({ error: 'Author not found' });
        return res.json(author);
    } catch (error) {
        console.error('Error fetching author:', error);
        return res.status(500).json({ error: 'Failed to fetch author' });
    }
});

// POST /api/authors — create a new author
authorsRoute.post('/', async (req, res) => {
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    const { name } = req.body;
    if (!name?.trim()) return res.status(400).json({ error: 'Author name is required' });

    try {
        const [newAuthor] = await db
            .insert(authors)
            .values({ name: name.trim() })
            .returning();
        return res.status(201).json(newAuthor);
    } catch (error) {
        console.error('Error creating author:', error);
        return res.status(500).json({ error: 'Failed to create author' });
    }
});

export default authorsRoute;

