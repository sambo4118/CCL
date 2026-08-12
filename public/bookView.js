import { loadBookDetails, showEditInfoModal } from "./utils.js";

document.addEventListener('DOMContentLoaded', async () => {
    const bookId = window.location.pathname.split('/').pop();
    try {
        const book = await loadBookDetails(bookId);
        displayBookDetails(book);
    } catch (err) {
        console.error('Failed to load book:', err);
    }
});

function displayBookDetails(book) {
    const { cover, title, author, year, publisher, isbn, blurb, localnumber } = getDetailElements();
    cover.src = book.coverUrl;
    title.textContent = book.title ?? "Untitled";
    author.textContent = book.author ?? "";
    year.textContent = book.publishDate ?? book.published ?? "";
    publisher.textContent = book.publisher ?? "";
    isbn.textContent = book.isbn ? `ISBN: ${book.isbn}` : "";
    blurb.textContent = book.blurb ?? "";
    localnumber.textContent = book.localNumber;
}

function getDetailElements() {
    const cover = document.getElementById('bookCover');
    const title = document.getElementById('bookTitle');
    const author = document.getElementById('bookAuthor');
    const year = document.getElementById('bookYear');
    const publisher = document.getElementById('bookPublisher');
    const isbn = document.getElementById('bookISBN');
    const blurb = document.getElementById('bookBlurb');
    const localnumber = document.getElementById('bookLocalnumber');
    return { cover, title, author, year, publisher, isbn, blurb, localnumber };
}

function showBookEditModal(book) {
    const bookFormFields = [
        {
            name: "Title"
        }
    ]
}