document.addEventListener('DOMContentLoaded', async () => {
    const subtitleElement = document.getElementById('subtitle');
    if (!subtitleElement) return;

    const bookCount = await fetch('/api/books/count')
        .then((res) => (res.ok ? res.json() : null))
        .then((data) => (data ? data.count : null))
        .catch(() => null);

    if (bookCount !== null) {
        subtitleElement.textContent = `Currently Indexing ${bookCount} books and counting.`;
    }
});