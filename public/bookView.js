import { loadBookDetails, Modal, showWarning } from "./utils.js";

document.addEventListener('DOMContentLoaded', async () => {
    const bookId = window.location.pathname.split('/').pop();
    try {
        let book = await loadBookDetails(bookId);
        book = cleanBook(book);
        displayBookDetails(book);
        const modal = buildEditModal(book);
    } catch (err) {
        showWarning(`Error failed to load book: ${err}, press confirm to reload`, () => {
            window.location.reload();
        })
    }
});

function cleanBook(book) {
    if (!book.title) book.title = 'Untitled';
    if (!book.author) book.author = 'Unknown';
    if (!book.publishDate || book.published) book.published = book.published ?? book.publishDate;
    return book;
}



function displayBookDetails(book) {
    const { cover, title, author, year, publisher, isbn, blurb, localnumber } = getDetailElements();
    cover.src = book.coverUrl;
    title.textContent = book.title ?? 'untitled';
    author.textContent = book.author ?? 'Unknown';
    year.textContent = book.published;
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

function buildEditModal(book) {
    const modal = new Modal({title: book.title, mainColorBulmaVariable: 'primary', successButtonText: 'Save', onConfirm: (modal) => updateBookInfo(book, modal) });
    
    const editButton = document.getElementById('editBookButton');
    editButton.addEventListener('click', () => modal.open());

    const config = {
    
        bookTitle: {
            label:'Title:',
            type: 'text',
            placeholder: 'Input title',
            color:'primary',
            value: book.title
        },
        authorName: {
            label:'Author:',
            type: 'text',
            placeholder: 'Unknown author',
            color:'primary',
            value: (book.author == 'Unknown') ? null : book.author
        },
        localNumber: {
            label: 'local number:',
            type: 'text',
            placeholder: 'ERROR: MISSING LOCAL NUMBER',
            color:'primary',
            value: book.localNumber
        },
        published: {label:'Published year:',
            type: 'text',
            placeholder: 'the year published',
            color:'primary',
            value: book.published
        },
        publisher: {
            label:'Publisher:',
            type: 'text',
            placeholder: 'the publisher',
            color:'primary',
            value: book.publisher
        },
        isbn: {
            label: 'ISBN#:',
            type: 'responsive',
            placeholder: 'ISBN#',
            color:'primary',
            value: book.isbn ?? 'ERROR: MISSING ISBN NUMBER',
            responseFunction: (input) => fillModal(input),
            minChars: 10
        },
        blurb: {label: 'blurb (just a description of the book):',
            type: 'textarea',
            color:'primary',
            value: book.blurb
        },
        bookCover: {label:'Cover:',
            type: 'file',
            placeholder: 'Select cover...', 
            color:'primary',
        }

    };

    modal.addFields(config);


    return modal;
}

async function fillModal(input) {
    const isbn = input.value;
    if (!isbn) return false;

    const bookInfo = await fetchBookInfoExternal(isbn);
    if (!bookInfo) return false;

    console.log('bookInfo', bookInfo);
    return true;
} 