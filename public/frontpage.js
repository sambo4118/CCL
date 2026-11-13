import { Viewlist } from './listBuilderClass.js';

document.addEventListener('DOMContentLoaded', async () => {
    await Promise.all([
        loadAndRenderStats(),
        loadAndRenderTopBooks()
    ]);
});

async function fetchJsonSafe(url) {
    try {
        const res = await fetch(url);
        return res.ok ? await res.json() : null;
    } catch (err) {
        console.error(`Failed to fetch ${url}:`, err);
        return null;
    }
}

async function loadAndRenderStats() {
    const [booksData, checkoutsData, studentsData, authorsData, classesData] = await Promise.all([
        fetchJsonSafe('/api/books/count'),
        fetchJsonSafe('/api/checkouts/count'),
        fetchJsonSafe('/api/students/count'),
        fetchJsonSafe('/api/authors/count'),
        fetchJsonSafe('/api/classes/count')
    ]);

    const subtitleElement = document.getElementById('subtitle');
    if (subtitleElement && booksData?.count !== undefined) {
        subtitleElement.textContent = `Currently Indexing ${booksData.count.toLocaleString()} books and counting.`;
    }

    setStatText('totalBooksStat', booksData?.count);
    setStatText('availableBooksStat', booksData?.availableCount);
    setStatText('totalCheckoutsStat', checkoutsData?.count);
    setStatText('activeCheckoutsStat', checkoutsData?.activeCount);
    setStatText('returnedCheckoutsStat', checkoutsData?.returnedCount);
    setStatText('totalStudentsStat', studentsData?.count);
    setStatText('totalAuthorsStat', authorsData?.count);
    setStatText('totalClassesStat', classesData?.count);
}

function setStatText(elementId, value) {
    const el = document.getElementById(elementId);
    if (!el) return;
    el.textContent = value !== undefined && value !== null
        ? Number(value).toLocaleString()
        : '—';
}

async function loadAndRenderTopBooks() {
    const container = document.getElementById('topBooksContainer');
    if (!container) return;

    const topBooks = await fetchJsonSafe('/api/books/top?limit=5');
    const rankedBooks = Array.isArray(topBooks)
        ? topBooks.map((book, index) => ({ ...book, rank: index + 1 }))
        : [];

    const topBooksList = buildTopBooksList();
    topBooksList.render(rankedBooks);
}

function buildTopBooksList() {
    const columnsConfig = {
        left: {
            size: 8,
            align: 'left',
            lines: [
                {
                    type: 'title',
                    text: (book) => `#${book.rank} — ${book.title || 'Untitled Book'}`,
                    href: (book) => `/books/${book.id}`
                },
                {
                    type: 'subtitle',
                    text: (book) => book.author || 'Unknown Author'
                },
                {
                    type: 'subtitle',
                    text: (book) => [
                        book.publisher ? `Publisher: ${book.publisher}` : null,
                        book.published ? `Published: ${book.published}` : null,
                        book.localNumber ? `Local #: ${book.localNumber}` : null
                    ].filter(Boolean).join(' • ')
                }
            ]
        },
        right: {
            size: 4,
            align: 'right',
            className: 'pr-4',
            contentClassName: 'mr-4',
            lines: [
                {
                    type: 'title',
                    className: 'title is-5 has-text-info-light mb-2',
                    text: (book) => {
                        const count = book.checkoutCount ?? 0;
                        return `${count} ${count === 1 ? 'Checkout' : 'Checkouts'}`;
                    }
                },
                {
                    type: 'subtitle',
                    className: 'subtitle is-6 has-text-success',
                    text: () => 'Available Now'
                }
            ]
        }
    };

    return new Viewlist({
        container: '#topBooksContainer',
        baseColor: 'primary',
        emptyMessage: 'No available books found.',
        imageConfig: {
            srcKey: 'coverUrl',
            placeholder: '/images/128x128.png',
            alt: (book) => `${book.title ?? 'Book'} cover`
        },
        columnsConfig
    });
}
