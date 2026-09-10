import { showWarning, showAuthWarning, Modal } from './utils.js';

document.addEventListener('DOMContentLoaded', async () => { 
    const checkouts = await loadCheckouts()
    await renderGeneralCheckoutStats(checkouts);
    await displayCheckouts(checkouts);
})
    

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
    rightCol.className = `column is-1 has-background-${checkoutColor} p-4 is-flex is-flex-direction-column is-justify-content-center`;
    rightCol.style.borderRadius = "0 0.5rem 0.5rem 0";

    const mediaElement = document.createElement("article");
    mediaElement.className = "media";

    const mediaLeft = document.createElement("figure");
    mediaLeft.className = "media-left";
    
    const imgContainer = document.createElement("p");
    imgContainer.className = "image ml-4 mt-4";
    imgContainer.loading = "lazy";
    imgContainer.style.width = "96px";
    imgContainer.style.height = "128px";
    imgContainer.style.objectFit = "fill";
    imgContainer.alt = `${checkout.bookTitle ?? "Book"} cover`;
    const BookCoverImg = document.createElement("img");
    BookCoverImg.src = checkout.coverUrl || Placeholder;
    
    imgContainer.appendChild(BookCoverImg);
    mediaLeft.appendChild(imgContainer);

    const mediaContent = document.createElement("div");
    mediaContent.className = "media-content";

    const content = document.createElement("div");
    content.className = "content";

    const level = document.createElement("div");
    level.className = "level is-mobile mt-4 mr-5";

    const levelLeft = document.createElement("div");
    levelLeft.className = "level-left";

    const bookLevel = document.createElement("div");
    bookLevel.className = "level-item";

    const BookLevelContent = document.createElement("div");
    BookLevelContent.className = "content has-text-left";

    const bookTitle = document.createElement("p");
    bookTitle.className = "title is-4";
    bookTitle.textContent = checkout.bookTitle || "Unknown Book";

    const authorName = document.createElement("p");
    authorName.className = "subtitle is-6 has-text-grey";
    authorName.textContent = checkout.bookAuthor || "Unknown Author";

    BookLevelContent.appendChild(bookTitle);
    BookLevelContent.appendChild(authorName);
    bookLevel.appendChild(BookLevelContent);
    levelLeft.appendChild(bookLevel);

    const levelRight = document.createElement("div");
    levelRight.className = "level-right";

    const studentLevel = document.createElement("div");
    studentLevel.className = "level-item";

    const studentLevelContent = document.createElement("div");
    studentLevelContent.className = "content has-text-right";

    const studentName = document.createElement("p");
    studentName.className = "subtitle is-6 has-text-grey";
    studentName.textContent = checkout.studentName || "Unknown Student";

    studentLevelContent.appendChild(studentName);
    studentLevel.appendChild(studentLevelContent);
    levelRight.appendChild(studentLevel);

    level.appendChild(levelLeft);
    level.appendChild(levelRight);
    content.appendChild(level);
    mediaContent.appendChild(content);
   
    mediaElement.appendChild(mediaLeft);
    mediaElement.appendChild(mediaContent);
    leftCol.appendChild(mediaElement);

    rowWrapper.appendChild(leftCol);
    rowWrapper.appendChild(rightCol);
    return rowWrapper;
}

async function displayCheckouts(checkoutsList) {
    const container = document.getElementById('checkoutsContainer');
    if (!container) return;
    container.replaceChildren();
    if (checkoutsList.length === 0) {
        container.innerHTML = '<p class="has-text-grey has-text-centered py-4">No outstanding checkouts.</p>';
        return;
    }
    checkoutsList.forEach(checkout => {
        container.appendChild(createCheckoutElement(checkout));
    });
}