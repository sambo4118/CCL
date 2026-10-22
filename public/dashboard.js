import { showWarning, showAuthWarning, Modal } from './utils.js';
import { Viewlist } from './listBuilderClass.js';

document.addEventListener('DOMContentLoaded', async () => { 
    let checkouts = await loadCheckouts();
    const { sortBy, addBookButton, addCheckoutButton, addStudentButton } = await findDomElements();
    if (!checkouts) return showWarning('Failed to load checkouts.');
    
    const checkoutList = buildCheckoutList();

    await renderGeneralCheckoutStats(checkouts);
    checkoutList.render(sortCheckouts(checkouts, sortBy.value));
    
    const bookModal = await buildBookModal();
    
    const addcheckoutModal = await buildNewCheckoutModal(async () => {
        checkouts = await loadCheckouts();
        checkoutList.render(sortCheckouts(checkouts, sortBy.value));
    });

    addBookButton?.addEventListener('click', () => bookModal.open());
    addCheckoutButton?.addEventListener('click', () => addcheckoutModal.open());
    
    sortBy.addEventListener('change', async (event) => {
        const sortedCheckouts = sortCheckouts(checkouts, event.target.value);
        checkoutList.render(sortedCheckouts);
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
        overdueCheckouts
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

function buildCheckoutList() {
    const actionConfig = {
        size: 1,
        buttonColor: 'danger',
        iconClass: 'fas fa-check fa-lg',
        color: (checkout) => checkout.returnDate ? 'success' : (new Date(checkout.checkoutDate) < new Date() ? 'danger' : 'warning')
    };

    const columnsConfig = {
        left: {
            size: 5,
            align: 'left',
            lines: [
                {
                    type: 'title',
                    text: (checkout) => checkout.bookTitle || 'Unknown Book',
                    href: (checkout) => `/books/${checkout.bookId}`
                },
                {
                    type: 'subtitle',
                    text: (checkout) => checkout.bookAuthor || 'Unknown Author'
                },
                {
                    type: 'subtitle',
                    text: (checkout) => `Checked out: ${new Date(checkout.checkoutDate).toLocaleDateString()}`
                },
                {
                    type: 'subtitle',
                    text: (checkout) => {
                        if (checkout.returnDate) {
                            return `Returned: ${new Date(checkout.returnDate).toLocaleDateString()}`;
                        }
                        const due = new Date(checkout.checkoutDate);
                        due.setDate(due.getDate() + (checkout.duration ?? 14));
                        const today = new Date();
                        const diffDays = Math.round((due.setHours(0, 0, 0, 0) - today.setHours(0, 0, 0, 0)) / (1000 * 60 * 60 * 24));

                        let dueLabel = `Due in: ${diffDays} days`;
                        if (diffDays === 0) dueLabel = 'Due today';
                        if (diffDays < 0) dueLabel = `Overdue by: ${Math.abs(diffDays)} days`;
                        return dueLabel;
                    }
                }
            ]
        },
        center: {
            size: 3,
            align: 'centered',
            lines: [
                {
                    type: 'subtitle',
                    className: 'subtitle is-6 has-text-white',
                    text: (checkout) => checkout.className || 'Unknown Class'
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
                    type: 'stackedLink',
                    text: (checkout) => checkout.studentName || 'Unknown Student',
                    href: (checkout) => `/students/${checkout.studentId}`
                }
            ]
        }
    };

    const list = new Viewlist({
        container: '#checkoutsContainer',
        baseColor: 'primary',
        emptyMessage: 'No outstanding checkouts.',
        imageConfig: {
            srcKey: 'bookCoverUrl',
            placeholder: '/images/128x128.png',
            alt: (checkout) => `${checkout.bookTitle ?? 'Book'} cover`
        },
        actionConfig,
        columnsConfig
    });

    list.actionConfig = actionConfig;
    return list;
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