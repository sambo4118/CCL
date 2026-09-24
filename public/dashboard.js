import { showWarning, showAuthWarning, Modal } from './utils.js';

document.addEventListener('DOMContentLoaded', async () => { 
    const checkouts = await loadCheckouts()
    const { sortBy, addBookButton, addCheckoutButton, addStudentButton } = await findDomElements();
    if (!checkouts) return showWarning('Failed to load checkouts.');
    
    await renderGeneralCheckoutStats(checkouts);
    await displayCheckouts(sortCheckouts(checkouts, sortBy.value));
    
    const bookModal = await buildBookModal();
    
    const addcheckoutModal = await buildNewCheckoutModal(async () => {
        checkouts = await loadCheckouts();
        await displayCheckouts()
    });

    addBookButton?.addEventListener('click', () => bookModal.open());
    addCheckoutButton?.addEventListener('click', () => addcheckoutModal.open());
    
    sortBy.addEventListener('change', async (event) => {
        const sortedCheckouts = sortCheckouts(checkouts, event.target.value);
        await displayCheckouts(sortedCheckouts);
        console.debug(`Checkouts sorted by ${event.target.value}`);
    });
})

async function addNewCheckoutConfirm(m) {
    const student = m.hiddenValues['studentSearch'];
    const book = m.hiddenValues['bookSearch'];
    const duration = parseInt(m.getField('duration')?.value || '14', 10);
    if (!student?.id || !book?.id) {
        showWarning('Please select both a student and a book.');
        return;
    }
    try {
        const response = await fetch('/api/checkouts', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                studentId: student.id,
                bookId: book.id,
                duration
            })
        });
        if (!response.ok) throw new Error('Failed to create checkout');
        const newCheckout = await response.json();
        if (onCheckoutCreated) onCheckoutCreated(newCheckout);
    } catch (error) {
        console.error('Error creating checkout:', error);
        showWarning('Failed to create checkout');
    }
}

async function addNewBookConfirm(m) {
    const formData = new FormData();
    
    formData.append('title', m.getField('title')?.value || '');
    const authorSelection = m.hiddenValues['authorSearch'];
    if (authorSelection?.id) formData.append('authorId', authorSelection.id);
    formData.append('author', authorSelection?.text || m.getField('authorSearch')?.value || '');
    formData.append('localNumber', m.getField('localNumber')?.value || '');
    formData.append('published', m.getField('published')?.value || '');
    formData.append('publisher', m.getField('publisher')?.value || '');
    formData.append('isbn', m.getField('isbn')?.value || '');
    formData.append('blurb', m.getField('blurb')?.value || '');
    
    const coverFile = m.getField('cover')?.files?.[0];
    
    if (coverFile) formData.append('cover', coverFile);
    
    try {
    
        const response = await fetch('/api/books', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) throw new Error('Failed to create book');
        
        const newBook = await response.json();
        
        if (typeof onBookCreated === 'function') onBookCreated(newBook);
    
    } catch (error) {
    
        console.error('Error adding book:', error);
        showWarning('Failed to add book');
    
    }
}

async function buildBookModal() {
    const modal = new Modal({title: 'Add New Book', mainColorBulmaVariable:'primary', successButtonText:'Add Book', onConfirm: async (m) => await addNewBookConfirm(m)});
    
    const config = {
        isbn: {
            label: 'ISBN# (Auto-fills info):',
            type: 'responsive',
            placeholder: 'Scan or type 10/13-digit ISBN...',
            color: 'primary',
            minChars: 10,
            responseFunction: async (isbnValue) => {
                if (!isbnValue) return;
                try {
                    const res = await fetch(`/api/books/external/${isbnValue.trim()}`);
                    if (!res.ok) return;
                    const data = await res.json();
                    if (data.title && modal.getField('title')) modal.getField('title').value = data.title;
                    if (data.authors?.length && modal.getField('authorSearch')) modal.getField('authorSearch').value = data.authors.join(', ');
                    if (data.publisher && modal.getField('publisher')) modal.getField('publisher').value = data.publisher;
                    if (data.publishedDate && modal.getField('published')) modal.getField('published').value = data.publishedDate.substring(0, 4);
                    if (data.description && modal.getField('blurb')) modal.getField('blurb').value = data.description;
                } catch (error) {
                    console.warn('Auto-lookup failed:', error);
                }
            }
        },
        title: {
            label: 'Title:',
            type: 'text',
            placeholder: 'Book title...',
            color: 'primary'
        },
        authorSearch: {
            label: 'Author:',
            type: 'search',
            placeholder: 'Search author name...',
            color: 'primary',
            minChars: 2,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/authors?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const authors = await res.json();
                return authors.map(a => ({ id: a.id, text: a.name }));
            }
        },
        localNumber: {
            label: 'Local # / Barcode:',
            type: 'text',
            placeholder: 'e.g. 1042',
            color: 'primary'
        },
        published: {
            label: 'Published Year:',
            type: 'text',
            placeholder: 'e.g. 2024',
            color: 'primary'
        },
        publisher: {
            label: 'Publisher:',
            type: 'text',
            placeholder: 'Publisher name...',
            color: 'primary'
        },
        blurb: {
            label: 'Description / Blurb:',
            type: 'textarea',
            color: 'primary'
        },
        cover: {
            label: 'Cover Image:',
            type: 'file',
            color: 'primary'
        }
    };
    modal.addFields(config);
    return modal;
}

async function buildNewCheckoutModal(onCheckoutCreated) {
    const modal = new Modal({
        title: 'New Checkout',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Check Out',
        onConfirm: async (m) => await addNewCheckoutConfirm
    });
    const config = {
        studentSearch: {
            label: 'Student:',
            type: 'search',
            placeholder: 'Search student name...',
            color: 'primary',
            minChars: 2,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/students?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const students = await res.json();
                return students.map(s => ({ id: s.id, text: `${s.name}${s.className ? ' — ' + s.className : ''}` }));
            }
        },
        bookSearch: {
            label: 'Book:',
            type: 'search',
            placeholder: 'Search book title or local #...',
            color: 'primary',
            minChars: 2,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/books/search?q=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const books = await res.json();
                return books.map(b => ({ id: b.id, text: `${b.title} (${b.localNumber})` }));
            }
        },
        duration: {
            label: 'Loan Duration (Days):',
            type: 'text',
            value: '14',
            placeholder: '14',
            color: 'primary'
        }
    };
    modal.addFields(config);
    return modal;
}

async function loadCheckouts() {
    const checkoutsResponce = await fetch('/api/checkouts/outstanding');
    if (!checkoutsResponce.ok) { let res = await checkoutsResponce.json(); return showWarning('Failed to fetch outstanding checkouts', {Object: res}); }

    const checkouts = await checkoutsResponce.json();

    return checkouts;
}

async function renderGeneralCheckoutStats(checkouts) { 
    const {
        outstandingCheckouts,
        dueCheckouts,
        overdueCheckouts,
        addBookButton,
        addCheckoutButton,
        sortBy,
        sortBySelect,
        sortByControl,
        searchInput,
        clearSearch
    } = await findDomElements();

    const outstandingCount = checkouts.length;
    const dueCount = checkouts.filter(c => new Date(c.checkoutDate).toDateString() === new Date().toDateString()).length;
    const overdueCount = checkouts.filter(c => new Date(c.checkoutDate) < new Date()).length;
    
    outstandingCheckouts.textContent = outstandingCount;
    dueCheckouts.textContent = dueCount;
    overdueCheckouts.textContent = overdueCount;
}

async function findDomElements() {
    
    const outstandingCheckouts = document.getElementById('outstandingCheckouts');
    const dueCheckouts = document.getElementById('dueCheckouts');
    const overdueCheckouts = document.getElementById('overdueCheckouts');

    const addBookButton = document.getElementById('addBookButton');
    const addCheckoutButton = document.getElementById('addCheckoutButton');

    const sortBy = document.getElementById('sortBy');
    const sortBySelect = document.getElementById('sortBySelect');
    const sortByControl = document.getElementById('sortByControl');

    const searchInput = document.getElementById('searchInput');
    const clearSearch = document.getElementById('clearSearch');

    return {
        outstandingCheckouts,
        dueCheckouts,
        overdueCheckouts,
        addBookButton,
        addCheckoutButton,
        sortBy,
        sortBySelect,
        sortByControl,
        searchInput,
        clearSearch
    }
}

function createCheckoutElement(checkout) {
    if (!checkout) return null;
    const checkoutColor = checkout.returnDate ? 'success' : (new Date(checkout.checkoutDate) < new Date() ? 'danger' : 'warning');
    
    const Placeholder = "/images/128x128.png";
    
    const rowWrapper = document.createElement("div");
    rowWrapper.className = "columns is-gapless is-mobile mb-4";

    const leftCol = document.createElement("div");
    leftCol.className = "column is-11 has-background-primary p-4";
    leftCol.style.borderRadius = "0.5rem 0 0 0.5rem";

    const rightCol = document.createElement("div");
    rightCol.className = `column is-1 has-background-${checkoutColor} p-2 is-flex is-justify-content-center is-align-items-center`;
    rightCol.style.borderRadius = "0 0.5rem 0.5rem 0";

    const plusButton = document.createElement("button");
    plusButton.className = "button is-danger is-fullwidth has-text-white has-text-weight-extra-bold";
    plusButton.style.height = "100%";
    plusButton.style.width = "100%";
    plusButton.innerHTML = '<span class="icon is-medium"><i class="fas fa-check fa-lg"></i></span>';
    rightCol.appendChild(plusButton);

    const mediaElement = document.createElement("article");
    mediaElement.className = "media";

    const mediaLeft = document.createElement("figure");
    mediaLeft.className = "media-left";
    
    const imgContainer = document.createElement("p");
    imgContainer.className = "image ml-4 mt-4 mb-4";
    imgContainer.loading = "lazy";
    imgContainer.style.width = "96px";
    imgContainer.style.height = "128px";
    imgContainer.style.objectFit = "fill";
    imgContainer.alt = `${checkout.bookTitle ?? "Book"} cover`;
    
    const BookCoverImg = document.createElement("img");
    BookCoverImg.src = checkout.bookCoverUrl || Placeholder;
    BookCoverImg.alt = `${checkout.bookTitle ?? "Book"} cover`;
    BookCoverImg.loading = "lazy";
    BookCoverImg.style.width = "96px";
    BookCoverImg.style.height = "128px";
    BookCoverImg.style.objectFit = "fill"; // Matches searchView uniform 96x128 ratio
    
    BookCoverImg.addEventListener("error", () => {
        if (BookCoverImg.src !== Placeholder) BookCoverImg.src = Placeholder;
    });
    
    imgContainer.appendChild(BookCoverImg);
    mediaLeft.appendChild(imgContainer);

    const mediaContent = document.createElement("div");
    mediaContent.className = "media-content";

    const content = document.createElement("div");
    content.className = "content";

    const columns = document.createElement("div");
    columns.className = "columns is-mobile";

    // 1. Left Column (Book details) - is-5
    const bookCol = document.createElement("div");
    bookCol.className = "column is-5";

    const bookDetails = document.createElement("div");
    bookDetails.className = "content has-text-left mt-4";

    const bookTitle = document.createElement("p");
    bookTitle.className = "title is-4";
    const bookLink = document.createElement("a");
    bookLink.href = `/books/${checkout.bookId}`;
    bookLink.className = "has-text-white";
    bookLink.textContent = checkout.bookTitle || "Unknown Book";
    bookTitle.appendChild(bookLink);

    const authorName = document.createElement("p");
    authorName.className = "subtitle is-6 has-text-grey";
    authorName.textContent = checkout.bookAuthor || "Unknown Author";

    const checkoutDate = document.createElement("p");
    checkoutDate.className = "subtitle is-6 has-text-grey";
    checkoutDate.textContent = `Checked out: ${new Date(checkout.checkoutDate).toLocaleDateString()}`;

    const dueDate = document.createElement("p");
    dueDate.className = "subtitle is-6 has-text-grey";

    const due = new Date(checkout.checkoutDate);
    due.setDate(due.getDate() + (checkout.duration ?? 14));
    const today = new Date();
    const diffDays = Math.round((due.setHours(0, 0, 0, 0) - today.setHours(0, 0, 0, 0)) / (1000 * 60 * 60 * 24));

    let dueLabel = `Due in: ${diffDays} days`;
    if (diffDays === 0) dueLabel = 'Due today';
    if (diffDays < 0) dueLabel = `Overdue by: ${Math.abs(diffDays)} days`;

    dueDate.textContent = checkout.returnDate
        ? `Returned: ${new Date(checkout.returnDate).toLocaleDateString()}`
        : dueLabel;

    bookDetails.appendChild(bookTitle);
    bookDetails.appendChild(authorName);
    bookDetails.appendChild(checkoutDate);
    bookDetails.appendChild(dueDate);
    bookCol.appendChild(bookDetails);

    // 2. Center Column (Class name) - is-3
    const classCol = document.createElement("div");
    classCol.className = "column is-3 has-text-centered";

    const classDetails = document.createElement("div");
    classDetails.className = "content mt-4";

    const className = document.createElement("p");
    className.className = "subtitle is-6 has-text-white";
    className.textContent = checkout.className || "Unknown Class";

    classDetails.appendChild(className);
    classCol.appendChild(classDetails);

    // 3. Right Column (Student name) - is-4
    const studentCol = document.createElement("div");
    studentCol.className = "column is-4 has-text-right pr-4";

    const studentDetails = document.createElement("div");
    studentDetails.className = "content mt-4 mr-4";

    const studentLink = document.createElement("a");
    studentLink.href = `/students/${checkout.studentId}`;
    studentLink.className = "is-block";

    const nameParts = (checkout.studentName || "Unknown Student").trim().split(/\s+/);
    nameParts.forEach((part) => {
        const nameLine = document.createElement("p");
        nameLine.className = "subtitle is-6 has-text-white";
        nameLine.textContent = part;
        studentLink.appendChild(nameLine);
    });

    studentDetails.appendChild(studentLink);
    studentCol.appendChild(studentDetails);

    columns.appendChild(bookCol);
    columns.appendChild(classCol);
    columns.appendChild(studentCol);

    content.appendChild(columns);
    mediaContent.appendChild(content);
   
    mediaElement.appendChild(mediaLeft);
    mediaElement.appendChild(mediaContent);
    leftCol.appendChild(mediaElement);

    rowWrapper.appendChild(leftCol);
    rowWrapper.appendChild(rightCol);
    return rowWrapper;
}

function sortCheckouts(list, sortBy) {
    return [...list].sort((a, b) => {
        switch (sortBy) {
        case 'title':
            return (a.bookTitle || '').localeCompare(b.bookTitle || '');
        case 'student':
            return (a.studentName || '').localeCompare(b.studentName || '');
        case 'class':
            return (a.className || '').localeCompare(b.className || '');
        }
        // 'due' (default): oldest checkout dates first (most urgent/overdue at top)
        return new Date(a.checkoutDate) - new Date(b.checkoutDate);
    });
}

async function displayCheckouts(checkoutsList) {
    const container = document.getElementById('checkoutsContainer');
    if (!container) return;
    if (checkoutsList.length === 0) return container.innerHTML = '<p class="has-text-grey has-text-centered py-4">No outstanding checkouts.</p>';
    container.innerHTML = '';
    checkoutsList.forEach(checkout => {
        container.appendChild(createCheckoutElement(checkout));
    });
}