import { db } from '../database/index.js';
import { books, students, checkouts } from '../database/schema.js';

// 1. Parse count from CLI args (e.g. npm run seed:checkouts 50 or --count=50)
const args = process.argv.slice(2);
const countArg = args.find(arg => !arg.startsWith('--')) 
    || args.find(arg => arg.startsWith('--count='))?.split('=')[1] 
    || '20';

const targetCount = Math.max(1, parseInt(countArg, 10) || 20);

async function seed() {
    console.log(`\n🌱 Starting checkouts seeder for ${targetCount} entries...`);

    // 2. Fetch existing students and books
    const allStudents = db.select({ id: students.id, name: students.name }).from(students).all();
    const allBooks = db.select({ id: books.id, title: books.title }).from(books).all();

    if (allStudents.length === 0 || allBooks.length === 0) {
        console.error('❌ Cannot seed checkouts: You need at least 1 student and 1 book in the database first.');
        process.exit(1);
    }

    const rows = [];
    const now = new Date();

    for (let i = 0; i < targetCount; i++) {
        const randomStudent = allStudents[Math.floor(Math.random() * allStudents.length)];
        const randomBook = allBooks[Math.floor(Math.random() * allBooks.length)];

        // Checkout date between 1 and 40 days ago
        const daysAgo = Math.floor(Math.random() * 40) + 1;
        const checkoutDate = new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000);

        // 60% chance to be outstanding (null), 40% chance returned
        let returnDate = null;
        const isReturned = Math.random() < 0.4;

        if (isReturned) {
            // Returned 1 to 10 days after checkout
            const returnDaysAfter = Math.min(daysAgo, Math.floor(Math.random() * 10) + 1);
            const returnedAt = new Date(checkoutDate.getTime() + returnDaysAfter * 24 * 60 * 60 * 1000);
            returnDate = returnedAt.toISOString().replace('T', ' ').substring(0, 19);
        }

        rows.push({
            studentId: randomStudent.id,
            bookId: randomBook.id,
            checkoutDate: checkoutDate.toISOString().replace('T', ' ').substring(0, 19),
            returnDate: returnDate
        });
    }

    // 3. Batch insert
    db.insert(checkouts).values(rows).run();

    const outstandingCount = rows.filter(r => r.returnDate === null).length;
    const returnedCount = rows.length - outstandingCount;

    console.log(`✅ Successfully generated ${rows.length} checkouts!`);
    console.log(`   - 📦 Outstanding (active): ${outstandingCount}`);
    console.log(`   - 🔄 Returned: ${returnedCount}\n`);
}

seed().catch(err => {
    console.error('Error seeding checkouts:', err);
    process.exit(1);
});