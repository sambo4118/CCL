import 'dotenv/config';

export async function fetchBookInfoExternal(bookIsbn) {
    const key = process.env.GOOGLE_BOOKS_API_KEY;
    const params = new URLSearchParams({
        q: `isbn:${bookIsbn}`,
        maxResults: '1',
        fields: 'items(volumeInfo(title,authors,publisher,publishedDate,description,pageCount,categories,imageLinks))',
    });
    if (key) params.set('key', key);

    try {
        const response = await fetch(`https://www.googleapis.com/books/v1/volumes?${params}`);
        if (!response.ok) return null;
        const data = await response.json();
        return data;
    } catch (err) {
        throw new Error(`fetchBookInfoExternal failed for ISBN ${bookIsbn}: ${err.message}`);
    }
}