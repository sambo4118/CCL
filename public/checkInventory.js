import { Modal, showWarning, createReturnModal } from './utils.js';
import { Viewlist } from './listBuilderClass.js';

const STORAGE_KEY = 'ccl_inventory_session_v1';
const PAGE_SIZE = 50;

let allBooks = [];
// Map of normalized localNumber -> array of book objects
let booksByLocalNumber = new Map();
// Map of bookId -> book object
let booksById = new Map();
// Map of bookId -> active checkout object
let activeCheckoutsByBookId = new Map();

// Session state persisted in localStorage
let sessionState = {
    scannedBookIds: [],       // unique book IDs confirmed scanned
    scannedLocalNumbers: [],  // unique normalized local numbers scanned
    recentScans: [],          // [{ localNumber, bookId, title, author, scannedAt }]
    startedAt: new Date().toISOString(),
    reportVisible: false
};

let unscannedViewlist = null;
let addBookModal = null;
let currentPage = 1;
let filteredUnscannedBooks = [];

// Modern Web Guidance: Fallback for :user-invalid and aria-invalid sync
const UserInvalidFallback = (() => {
    const dirtyState = new WeakMap();

    const updateState = (input) => {
        const isValid = input.checkValidity();
        input.classList.toggle('user-invalid-fallback', !isValid);
        input.classList.toggle('user-valid-fallback', isValid);
        if (!isValid) {
            input.setAttribute('aria-invalid', 'true');
        } else {
            input.removeAttribute('aria-invalid');
        }
    };

    const handleEvent = (event) => {
        const input = event.target;
        if (event.type === 'reset') {
            const controls = input.elements || [];
            for (const control of controls) {
                dirtyState.delete(control);
                control.classList.remove('user-invalid-fallback', 'user-valid-fallback');
                control.removeAttribute('aria-invalid');
            }
            return;
        }
        if (!input?.checkValidity) return;

        if (event.type === 'input' || event.type === 'change') {
            const state = dirtyState.get(input) || { hasInteracted: false, hasBlurred: false };
            state.hasInteracted = true;
            dirtyState.set(input, state);
            if (state.hasBlurred) updateState(input);
        } else if (event.type === 'blur') {
            const state = dirtyState.get(input) || { hasInteracted: false, hasBlurred: false };
            state.hasBlurred = true;
            dirtyState.set(input, state);
            if (state.hasInteracted) updateState(input);
        }
    };

    const init = (root = document) => {
        if (typeof CSS !== 'undefined' && CSS.supports && CSS.supports('selector(:user-invalid)')) return;
        root.addEventListener('blur', handleEvent, true);
        root.addEventListener('input', handleEvent);
        root.addEventListener('change', handleEvent);
        root.addEventListener('reset', handleEvent, true);
    };

    return { init };
})();

function syncAriaInvalid(el) {
    if (!el || typeof el.matches !== 'function') return;
    try {
        el.setAttribute('aria-invalid', el.matches(':user-invalid') ? 'true' : 'false');
    } catch {
        // Ignore if :user-invalid selector is unsupported in older engines
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    const scanForm = document.getElementById('scanBookForm');
    if (scanForm) {
        UserInvalidFallback.init(scanForm);
    }

    document.addEventListener('blur', (e) => syncAriaInvalid(e.target), true);
    document.addEventListener('input', (e) => {
        if (e.target?.hasAttribute?.('aria-invalid')) syncAriaInvalid(e.target);
    });

    loadSessionFromStorage();
    bindUIEvents();

    addBookModal = buildAddBookModal();
    unscannedViewlist = buildUnscannedViewlist();

    await loadLibraryCatalog();

    reconcileSessionWithCatalog();
    updateStatsAndRecentUI();

    if (sessionState.reportVisible) {
        showUnscannedReport({ scrollIntoView: false });
    }

    focusScannerInput();
});

function normalizeLocalNumber(value) {
    return String(value ?? '').trim().toLowerCase();
}

function loadSessionFromStorage() {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return;
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object') {
            sessionState = {
                scannedBookIds: Array.isArray(parsed.scannedBookIds) ? parsed.scannedBookIds : [],
                scannedLocalNumbers: Array.isArray(parsed.scannedLocalNumbers) ? parsed.scannedLocalNumbers : [],
                recentScans: Array.isArray(parsed.recentScans) ? parsed.recentScans : [],
                startedAt: parsed.startedAt || new Date().toISOString(),
                reportVisible: Boolean(parsed.reportVisible)
            };
        }
    } catch (err) {
        console.warn('Could not restore inventory session from localStorage:', err);
    }
}

function saveSessionToStorage() {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(sessionState));
    } catch (err) {
        console.warn('Could not save inventory session to localStorage:', err);
    }
}

async function loadLibraryCatalog() {
    const subtitle = document.getElementById('inventorySubtitle');
    try {
        const [booksRes, checkoutsRes] = await Promise.all([
            fetch('/api/books'),
            fetch('/api/checkouts/outstanding').catch(() => null)
        ]);

        if (!booksRes.ok) {
            throw new Error(`Failed to fetch books (${booksRes.status})`);
        }

        const booksData = await booksRes.json();
        allBooks = Array.isArray(booksData) ? booksData : [];

        activeCheckoutsByBookId.clear();
        if (checkoutsRes && checkoutsRes.ok) {
            const checkoutsData = await checkoutsRes.json();
            if (Array.isArray(checkoutsData)) {
                checkoutsData.forEach((checkout) => {
                    if (checkout.bookId != null && !checkout.returnDate) {
                        activeCheckoutsByBookId.set(Number(checkout.bookId), checkout);
                    }
                });
            }
        }

        rebuildBookLookups();

        if (subtitle) {
            subtitle.textContent = `Catalog loaded (${allBooks.length.toLocaleString()} books). Scan or enter each book's Local # below.`;
        }
    } catch (err) {
        console.error('Error loading library catalog:', err);
        showWarning('Failed to load library catalog. Press confirm to reload.', {
            object: err,
            onConfirm: () => window.location.reload()
        });
    }
}

function rebuildBookLookups() {
    booksByLocalNumber.clear();
    booksById.clear();

    for (const book of allBooks) {
        booksById.set(Number(book.id), book);
        const key = normalizeLocalNumber(book.localNumber);
        if (!key) continue;
        const list = booksByLocalNumber.get(key) || [];
        list.push(book);
        booksByLocalNumber.set(key, list);
    }
}

function reconcileSessionWithCatalog() {
    const validIds = new Set();
    const validLocals = new Set();

    // Ensure any scanned localNumber or bookId still maps to current catalog books
    for (const id of sessionState.scannedBookIds) {
        const book = booksById.get(Number(id));
        if (book) {
            validIds.add(Number(book.id));
            if (book.localNumber) validLocals.add(normalizeLocalNumber(book.localNumber));
        }
    }

    for (const localKey of sessionState.scannedLocalNumbers) {
        const matches = booksByLocalNumber.get(normalizeLocalNumber(localKey));
        if (matches && matches.length > 0) {
            validLocals.add(normalizeLocalNumber(localKey));
            matches.forEach((b) => validIds.add(Number(b.id)));
        }
    }

    sessionState.scannedBookIds = Array.from(validIds);
    sessionState.scannedLocalNumbers = Array.from(validLocals);
    sessionState.recentScans = sessionState.recentScans.filter((entry) => booksById.has(Number(entry.bookId)));
    saveSessionToStorage();
}

function bindUIEvents() {
    const scanForm = document.getElementById('scanBookForm');
    const localNumberInput = document.getElementById('localNumberInput');
    const addBookButton = document.getElementById('addBookButton');
    const finishInventoryButton = document.getElementById('finishInventoryButton');
    const resetInventoryButton = document.getElementById('resetInventoryButton');
    const toggleRecentScansButton = document.getElementById('toggleRecentScansButton');
    const hideUnscannedButton = document.getElementById('hideUnscannedButton');
    const exportUnscannedCsvButton = document.getElementById('exportUnscannedCsvButton');
    const unscannedSearchInput = document.getElementById('unscannedSearchInput');
    const unscannedStatusFilter = document.getElementById('unscannedStatusFilter');
    const unscannedSortBy = document.getElementById('unscannedSortBy');

    // Modern Web Guidance: IME-safe Enter-to-submit handling on single-line input
    localNumberInput?.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
            event.preventDefault();
            if (event.isComposing || event.keyCode === 229) {
                return;
            }
            scanForm?.requestSubmit();
        }
    });

    // Clear custom validity on input so error styling resets immediately while typing
    localNumberInput?.addEventListener('input', () => {
        localNumberInput.setCustomValidity('');
        localNumberInput.removeAttribute('aria-invalid');
        localNumberInput.classList.remove('is-danger', 'user-invalid-fallback');
    });

    scanForm?.addEventListener('submit', async (event) => {
        event.preventDefault();
        await handleLocalNumberSubmit();
    });

    addBookButton?.addEventListener('click', () => {
        const draftLocal = localNumberInput?.value?.trim() || '';
        openAddBookModal(draftLocal);
    });

    finishInventoryButton?.addEventListener('click', () => {
        showUnscannedReport({ scrollIntoView: true });
    });

    resetInventoryButton?.addEventListener('click', () => {
        const scannedCount = sessionState.scannedBookIds.length;
        showWarning(
            scannedCount > 0
                ? `Are you sure you want to reset the inventory session? This will clear all <strong>${scannedCount.toLocaleString()}</strong> scanned books.`
                : 'Reset the current inventory session and start fresh?',
            {
                onConfirm: () => {
                    resetInventorySession();
                }
            }
        );
    });

    toggleRecentScansButton?.addEventListener('click', () => {
        const panel = document.getElementById('recentScansPanel');
        const chevron = document.getElementById('recentScansChevron');
        if (!panel) return;
        const isHidden = panel.classList.toggle('is-hidden');
        toggleRecentScansButton.setAttribute('aria-expanded', String(!isHidden));
        if (chevron) {
            chevron.className = isHidden ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-up';
        }
    });

    hideUnscannedButton?.addEventListener('click', () => {
        const section = document.getElementById('unscannedSection');
        section?.classList.add('is-hidden');
        sessionState.reportVisible = false;
        saveSessionToStorage();
        focusScannerInput();
    });

    exportUnscannedCsvButton?.addEventListener('click', () => {
        exportUnscannedBooksCsv();
    });

    unscannedSearchInput?.addEventListener('input', () => {
        currentPage = 1;
        refreshUnscannedList();
    });

    unscannedStatusFilter?.addEventListener('change', () => {
        currentPage = 1;
        refreshUnscannedList();
    });

    unscannedSortBy?.addEventListener('change', () => {
        currentPage = 1;
        refreshUnscannedList();
    });

    const prevButtons = [
        document.getElementById('prevPageTopButton'),
        document.getElementById('prevPageBottomButton')
    ];
    const nextButtons = [
        document.getElementById('nextPageTopButton'),
        document.getElementById('nextPageBottomButton')
    ];

    prevButtons.forEach((btn) => {
        btn?.addEventListener('click', () => {
            if (currentPage > 1) {
                currentPage--;
                renderCurrentUnscannedPage();
            }
        });
    });

    nextButtons.forEach((btn) => {
        btn?.addEventListener('click', () => {
            const totalPages = Math.max(1, Math.ceil(filteredUnscannedBooks.length / PAGE_SIZE));
            if (currentPage < totalPages) {
                currentPage++;
                renderCurrentUnscannedPage();
            }
        });
    });
}

function focusScannerInput() {
    const input = document.getElementById('localNumberInput');
    if (!input) return;
    input.focus();
    input.select();
}

async function handleLocalNumberSubmit() {
    const input = document.getElementById('localNumberInput');
    const checkBtn = document.getElementById('checkLocalNumberButton');
    const control = document.getElementById('localNumberControl');
    if (!input) return;

    const rawValue = input.value.trim();
    if (!rawValue) {
        input.setCustomValidity('Please enter or scan a book local number.');
        input.setAttribute('aria-invalid', 'true');
        renderScanFeedback({
            type: 'warning',
            title: 'No Local Number Entered',
            message: 'Please scan a barcode or type a book Local # before checking.'
        });
        focusScannerInput();
        return;
    }

    input.setCustomValidity('');
    input.setAttribute('aria-invalid', 'false');

    const normalized = normalizeLocalNumber(rawValue);
    let matchingBooks = booksByLocalNumber.get(normalized);

    // Fallback to server lookup in case a book was recently added in another tab
    if (!matchingBooks || matchingBooks.length === 0) {
        checkBtn?.classList.add('is-loading');
        control?.classList.add('is-loading');
        try {
            const res = await fetch(`/api/books/local/${encodeURIComponent(rawValue)}`);
            if (res.ok) {
                const serverMatches = await res.json();
                if (Array.isArray(serverMatches) && serverMatches.length > 0) {
                    serverMatches.forEach((b) => {
                        if (!booksById.has(Number(b.id))) {
                            allBooks.push(b);
                        }
                    });
                    rebuildBookLookups();
                    matchingBooks = booksByLocalNumber.get(normalized);
                }
            }
        } catch (err) {
            console.warn('Fallback local number lookup failed:', err);
        } finally {
            checkBtn?.classList.remove('is-loading');
            control?.classList.remove('is-loading');
        }
    }

    if (!matchingBooks || matchingBooks.length === 0) {
        // Invalid local number
        input.value = '';
        renderScanFeedback({
            type: 'danger',
            title: `Invalid Local Number: "${rawValue}"`,
            message: `No book with Local # "${rawValue}" was found in the library catalog.`,
            unmatchedLocalNumber: rawValue
        });
        focusScannerInput();
        return;
    }

    // Valid local number! Check if already scanned in this session
    const scannedIdSet = new Set(sessionState.scannedBookIds.map(Number));
    const newlyScannedBooks = matchingBooks.filter((b) => !scannedIdSet.has(Number(b.id)));

    if (newlyScannedBooks.length === 0) {
        const primaryBook = matchingBooks[0];
        input.value = '';
        renderScanFeedback({
            type: 'warning',
            title: `Already Scanned (#${primaryBook.localNumber})`,
            message: `"${primaryBook.title || 'Untitled'}" is already recorded in your valid books count.`,
            book: primaryBook
        });
        focusScannerInput();
        return;
    }

    // Record valid book(s)
    recordValidBooksScanned(newlyScannedBooks, normalized);

    input.value = '';
    const primaryBook = newlyScannedBooks[0];
    const extraCopiesText = newlyScannedBooks.length > 1
        ? ` (+${newlyScannedBooks.length - 1} matching copy with same Local #)`
        : '';

    renderScanFeedback({
        type: 'success',
        title: `Valid Book Counted! (Local #${primaryBook.localNumber})${extraCopiesText}`,
        message: `Added "${primaryBook.title || 'Untitled'}" by ${primaryBook.author || 'Unknown Author'} to valid inventory count.`,
        book: primaryBook
    });

    focusScannerInput();
}

function recordValidBooksScanned(booksToRecord, normalizedLocalKey = null) {
    const scannedIdSet = new Set(sessionState.scannedBookIds.map(Number));
    const scannedLocalSet = new Set(sessionState.scannedLocalNumbers);

    if (normalizedLocalKey) {
        scannedLocalSet.add(normalizedLocalKey);
    }

    const nowIso = new Date().toISOString();

    for (const book of booksToRecord) {
        const numericId = Number(book.id);
        scannedIdSet.add(numericId);
        if (book.localNumber) {
            scannedLocalSet.add(normalizeLocalNumber(book.localNumber));
        }

        // Prepend to recent scans
        sessionState.recentScans = [
            {
                bookId: numericId,
                localNumber: book.localNumber,
                title: book.title || 'Untitled',
                author: book.author || 'Unknown Author',
                scannedAt: nowIso
            },
            ...sessionState.recentScans.filter((item) => Number(item.bookId) !== numericId)
        ].slice(0, 25);
    }

    sessionState.scannedBookIds = Array.from(scannedIdSet);
    sessionState.scannedLocalNumbers = Array.from(scannedLocalSet);
    saveSessionToStorage();

    updateStatsAndRecentUI({ pulseCounter: true });

    if (sessionState.reportVisible) {
        refreshUnscannedList();
    }
}

function removeScannedBook(bookId) {
    const numericId = Number(bookId);
    const book = booksById.get(numericId);

    sessionState.scannedBookIds = sessionState.scannedBookIds.filter((id) => Number(id) !== numericId);
    sessionState.recentScans = sessionState.recentScans.filter((entry) => Number(entry.bookId) !== numericId);

    if (book?.localNumber) {
        const normKey = normalizeLocalNumber(book.localNumber);
        const otherScannedWithSameLocal = (booksByLocalNumber.get(normKey) || []).some(
            (b) => Number(b.id) !== numericId && sessionState.scannedBookIds.includes(Number(b.id))
        );
        if (!otherScannedWithSameLocal) {
            sessionState.scannedLocalNumbers = sessionState.scannedLocalNumbers.filter((k) => k !== normKey);
        }
    }

    saveSessionToStorage();
    updateStatsAndRecentUI();

    if (sessionState.reportVisible) {
        refreshUnscannedList();
    }

    if (book) {
        renderScanFeedback({
            type: 'info',
            title: `Removed from Scanned Count (#${book.localNumber})`,
            message: `"${book.title || 'Untitled'}" was removed from your scanned books count.`
        });
    }

    focusScannerInput();
}

function resetInventorySession() {
    sessionState = {
        scannedBookIds: [],
        scannedLocalNumbers: [],
        recentScans: [],
        startedAt: new Date().toISOString(),
        reportVisible: false
    };
    saveSessionToStorage();

    const banner = document.getElementById('scanFeedbackBanner');
    if (banner) {
        banner.classList.add('is-hidden');
        banner.replaceChildren();
    }

    const unscannedSection = document.getElementById('unscannedSection');
    unscannedSection?.classList.add('is-hidden');

    updateStatsAndRecentUI();
    focusScannerInput();
}

function updateStatsAndRecentUI({ pulseCounter = false } = {}) {
    const validScannedCountEl = document.getElementById('validScannedCount');
    const totalCatalogCountEl = document.getElementById('totalCatalogCount');
    const remainingBooksCountEl = document.getElementById('remainingBooksCount');
    const inventoryPercentStatEl = document.getElementById('inventoryPercentStat');
    const inventoryProgressBar = document.getElementById('inventoryProgressBar');
    const recentScansBadge = document.getElementById('recentScansBadge');
    const sessionTimestampText = document.getElementById('sessionTimestampText');
    const finishBtnText = document.getElementById('finishInventoryButtonText');

    const scannedCount = sessionState.scannedBookIds.length;
    const totalCount = allBooks.length;
    const remainingCount = Math.max(0, totalCount - scannedCount);
    const percent = totalCount > 0 ? Math.min(100, (scannedCount / totalCount) * 100) : 0;
    const formattedPercent = totalCount > 0
        ? (percent > 0 && percent < 0.1 ? '<0.1%' : `${percent.toFixed(1)}%`)
        : '0%';

    if (validScannedCountEl) {
        validScannedCountEl.textContent = scannedCount.toLocaleString();
        if (pulseCounter) {
            const box = validScannedCountEl.closest('.box');
            if (box) {
                box.classList.remove('scan-pulse');
                void box.offsetWidth;
                box.classList.add('scan-pulse');
            }
        }
    }

    if (totalCatalogCountEl) {
        totalCatalogCountEl.textContent = totalCount.toLocaleString();
    }

    if (remainingBooksCountEl) {
        remainingBooksCountEl.textContent = remainingCount.toLocaleString();
    }

    if (inventoryPercentStatEl) {
        inventoryPercentStatEl.textContent = formattedPercent;
    }

    if (inventoryProgressBar) {
        inventoryProgressBar.value = percent;
        inventoryProgressBar.textContent = formattedPercent;
    }

    if (recentScansBadge) {
        recentScansBadge.textContent = String(scannedCount);
    }

    if (finishBtnText) {
        finishBtnText.textContent = `Finish & Get Unscanned Books (${remainingCount.toLocaleString()})`;
    }

    if (sessionTimestampText && sessionState.startedAt) {
        const startedDate = new Date(sessionState.startedAt);
        if (!Number.isNaN(startedDate.getTime())) {
            sessionTimestampText.textContent = `Session started ${startedDate.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}`;
        }
    }

    renderRecentScansChips();
}

function renderRecentScansChips() {
    const container = document.getElementById('recentScansContainer');
    if (!container) return;
    container.replaceChildren();

    if (sessionState.recentScans.length === 0) {
        const col = document.createElement('div');
        col.className = 'column';
        const emptyP = document.createElement('p');
        emptyP.className = 'is-size-7 has-text-grey-light';
        emptyP.textContent = 'No books scanned in this session yet.';
        col.appendChild(emptyP);
        container.appendChild(col);
        return;
    }

    sessionState.recentScans.slice(0, 12).forEach((scan) => {
        const col = document.createElement('div');
        col.className = 'column is-narrow';

        const tagWrap = document.createElement('div');
        tagWrap.className = 'tags has-addons mb-0';

        const localTag = document.createElement('span');
        localTag.className = 'tag is-warning has-text-white has-text-weight-bold';
        localTag.textContent = `#${scan.localNumber}`;

        const titleLink = document.createElement('a');
        titleLink.className = 'tag is-dark';
        titleLink.href = `/books/${scan.bookId}`;
        const truncatedTitle = scan.title.length > 28 ? `${scan.title.slice(0, 28)}…` : scan.title;
        titleLink.textContent = truncatedTitle;
        titleLink.title = `${scan.title} (${scan.author}) — Click to view book`;

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'tag is-delete is-danger';
        removeBtn.title = `Undo scan for #${scan.localNumber}`;
        removeBtn.setAttribute('aria-label', `Undo scan for ${scan.title}`);
        removeBtn.addEventListener('click', () => {
            removeScannedBook(scan.bookId);
        });

        tagWrap.append(localTag, titleLink, removeBtn);
        col.appendChild(tagWrap);
        container.appendChild(col);
    });
}

function renderScanFeedback({ type = 'info', title = '', message = '', book = null, unmatchedLocalNumber = null }) {
    const banner = document.getElementById('scanFeedbackBanner');
    if (!banner) return;

    banner.className = `notification is-${type} mt-4 mb-0 scan-pulse`;
    banner.replaceChildren();

    const iconMap = {
        success: 'fa-solid fa-circle-check',
        warning: 'fa-solid fa-triangle-exclamation',
        danger: 'fa-solid fa-circle-xmark',
        info: 'fa-solid fa-circle-info'
    };

    const media = document.createElement('article');
    media.className = 'media is-align-items-center';

    if (book) {
        const mediaLeft = document.createElement('figure');
        mediaLeft.className = 'media-left';
        const imgWrap = document.createElement('p');
        imgWrap.className = 'image';
        imgWrap.style.width = '54px';
        imgWrap.style.height = '72px';

        const img = document.createElement('img');
        img.src = book.coverUrl || '/images/128x128.png';
        img.alt = `${book.title || 'Book'} cover`;
        img.style.width = '54px';
        img.style.height = '72px';
        img.style.objectFit = 'fill';
        img.style.borderRadius = '4px';
        img.addEventListener('error', () => {
            if (img.src !== '/images/128x128.png') img.src = '/images/128x128.png';
        });

        imgWrap.appendChild(img);
        mediaLeft.appendChild(imgWrap);
        media.appendChild(mediaLeft);
    } else {
        const mediaLeft = document.createElement('div');
        mediaLeft.className = 'media-left';
        const iconSpan = document.createElement('span');
        iconSpan.className = 'icon is-large';
        const icon = document.createElement('i');
        icon.className = `${iconMap[type] || iconMap.info} fa-2x`;
        icon.setAttribute('aria-hidden', 'true');
        iconSpan.appendChild(icon);
        mediaLeft.appendChild(iconSpan);
        media.appendChild(mediaLeft);
    }

    const mediaContent = document.createElement('div');
    mediaContent.className = 'media-content';

    const titleEl = document.createElement('p');
    titleEl.className = 'has-text-weight-bold is-size-5 mb-1';
    titleEl.textContent = title;
    mediaContent.appendChild(titleEl);

    const msgEl = document.createElement('p');
    msgEl.className = 'is-size-6 mb-1';
    msgEl.textContent = message;
    mediaContent.appendChild(msgEl);

    if (book) {
        const metaParts = [
            book.bookLocation ? `Location: ${book.bookLocation}` : null,
            book.call1 || book.call2 ? `Call: ${[book.call1, book.call2].filter(Boolean).join(' ')}` : null,
            book.isbn ? `ISBN: ${book.isbn}` : null
        ].filter(Boolean);

        if (metaParts.length > 0) {
            const metaP = document.createElement('p');
            metaP.className = 'is-size-7';
            metaP.textContent = metaParts.join(' • ');
            mediaContent.appendChild(metaP);
        }

        const activeCheckout = activeCheckoutsByBookId.get(Number(book.id));
        if (activeCheckout) {
            const checkoutAlert = document.createElement('div');
            checkoutAlert.className = 'mt-2 is-flex is-align-items-center is-flex-wrap-wrap';
            checkoutAlert.style.gap = '0.5rem';

            const badge = document.createElement('span');
            badge.className = 'tag is-dark has-text-warning';
            badge.textContent = `Note: Checked out to ${activeCheckout.studentName || 'a student'}`;
            checkoutAlert.appendChild(badge);

            const returnBtn = document.createElement('button');
            returnBtn.type = 'button';
            returnBtn.className = 'button is-small is-dark';
            returnBtn.textContent = 'Mark Returned Now';
            returnBtn.addEventListener('click', () => {
                const modal = createReturnModal(activeCheckout, async () => {
                    activeCheckoutsByBookId.delete(Number(book.id));
                    renderScanFeedback({
                        type: 'success',
                        title: `Returned & Counted (#${book.localNumber})`,
                        message: `"${book.title}" has been marked returned and counted in inventory.`,
                        book
                    });
                    if (sessionState.reportVisible) refreshUnscannedList();
                    focusScannerInput();
                });
                modal.open();
            });
            checkoutAlert.appendChild(returnBtn);
            mediaContent.appendChild(checkoutAlert);
        }
    }

    media.appendChild(mediaContent);

    const mediaRight = document.createElement('div');
    mediaRight.className = 'media-right';

    if (unmatchedLocalNumber) {
        const quickAddBtn = document.createElement('button');
        quickAddBtn.type = 'button';
        quickAddBtn.className = 'button is-light is-small has-text-weight-semibold';
        quickAddBtn.innerHTML = '<span class="icon"><i class="fas fa-plus" aria-hidden="true"></i></span><span>Add Book #' + escapeHtml(unmatchedLocalNumber) + '</span>';
        quickAddBtn.addEventListener('click', () => {
            openAddBookModal(unmatchedLocalNumber);
        });
        mediaRight.appendChild(quickAddBtn);
    } else if (book) {
        const viewLink = document.createElement('a');
        viewLink.className = 'button is-small is-light';
        viewLink.href = `/books/${book.id}`;
        viewLink.textContent = 'View Book';
        mediaRight.appendChild(viewLink);
    }

    media.appendChild(mediaRight);
    banner.appendChild(media);
}

function escapeHtml(str) {
    return String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function showUnscannedReport({ scrollIntoView = true } = {}) {
    const unscannedSection = document.getElementById('unscannedSection');
    if (!unscannedSection) return;

    sessionState.reportVisible = true;
    saveSessionToStorage();

    unscannedSection.classList.remove('is-hidden');
    currentPage = 1;
    refreshUnscannedList();

    if (scrollIntoView) {
        unscannedSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

function getUnscannedBooks() {
    const scannedIdSet = new Set(sessionState.scannedBookIds.map(Number));
    const scannedLocalSet = new Set(sessionState.scannedLocalNumbers);

    return allBooks.filter((book) => {
        if (scannedIdSet.has(Number(book.id))) return false;
        const normLocal = normalizeLocalNumber(book.localNumber);
        if (normLocal && scannedLocalSet.has(normLocal)) return false;
        return true;
    });
}

function refreshUnscannedList() {
    const allUnscanned = getUnscannedBooks();
    const query = (document.getElementById('unscannedSearchInput')?.value || '').trim().toLowerCase();
    const statusFilter = document.getElementById('unscannedStatusFilter')?.value || 'all';
    const sortBy = document.getElementById('unscannedSortBy')?.value || 'localNumber';

    let checkedOutUnscannedCount = 0;
    allUnscanned.forEach((b) => {
        if (activeCheckoutsByBookId.has(Number(b.id))) checkedOutUnscannedCount++;
    });
    const missingFromShelfCount = allUnscanned.length - checkedOutUnscannedCount;

    const summaryText = document.getElementById('unscannedSummaryText');
    if (summaryText) {
        summaryText.textContent = `${allUnscanned.length.toLocaleString()} unscanned books total — ${missingFromShelfCount.toLocaleString()} expected on shelf, ${checkedOutUnscannedCount.toLocaleString()} currently checked out to students.`;
    }

    filteredUnscannedBooks = allUnscanned.filter((book) => {
        const isCheckedOut = activeCheckoutsByBookId.has(Number(book.id));
        if (statusFilter === 'available' && isCheckedOut) return false;
        if (statusFilter === 'checkedOut' && !isCheckedOut) return false;

        if (!query) return true;
        const haystack = [
            book.localNumber,
            book.title,
            book.subtitle,
            book.author,
            book.isbn,
            book.bookLocation,
            book.call1,
            book.call2,
            book.publisher
        ]
            .filter(Boolean)
            .join(' ')
            .toLowerCase();

        return haystack.includes(query);
    });

    filteredUnscannedBooks.sort((a, b) => {
        switch (sortBy) {
            case 'title':
                return String(a.title || '').localeCompare(String(b.title || ''), undefined, { sensitivity: 'base' });
            case 'author':
                return String(a.author || '').localeCompare(String(b.author || ''), undefined, { sensitivity: 'base' });
            case 'location': {
                const locA = `${a.bookLocation || ''} ${a.call1 || ''} ${a.call2 || ''}`.trim();
                const locB = `${b.bookLocation || ''} ${b.call1 || ''} ${b.call2 || ''}`.trim();
                return locA.localeCompare(locB, undefined, { numeric: true, sensitivity: 'base' });
            }
            case 'localNumber':
            default:
                return String(a.localNumber || '').localeCompare(String(b.localNumber || ''), undefined, {
                    numeric: true,
                    sensitivity: 'base'
                });
        }
    });

    const totalPages = Math.max(1, Math.ceil(filteredUnscannedBooks.length / PAGE_SIZE));
    if (currentPage > totalPages) currentPage = totalPages;

    renderCurrentUnscannedPage();
}

function renderCurrentUnscannedPage() {
    if (!unscannedViewlist) return;

    const totalItems = filteredUnscannedBooks.length;
    const totalPages = Math.max(1, Math.ceil(totalItems / PAGE_SIZE));
    const startIdx = (currentPage - 1) * PAGE_SIZE;
    const endIdx = Math.min(startIdx + PAGE_SIZE, totalItems);
    const pageItems = filteredUnscannedBooks.slice(startIdx, endIdx);

    unscannedViewlist.render(pageItems);

    const infoText = totalItems === 0
        ? 'No unscanned books match your filter 🎉'
        : `Showing ${(startIdx + 1).toLocaleString()}–${endIdx.toLocaleString()} of ${totalItems.toLocaleString()} unscanned books (Page ${currentPage} of ${totalPages})`;

    const topInfo = document.getElementById('unscannedPageInfoTop');
    const bottomInfo = document.getElementById('unscannedPageInfoBottom');
    if (topInfo) topInfo.textContent = infoText;
    if (bottomInfo) bottomInfo.textContent = infoText;

    const prevTop = document.getElementById('prevPageTopButton');
    const prevBottom = document.getElementById('prevPageBottomButton');
    const nextTop = document.getElementById('nextPageTopButton');
    const nextBottom = document.getElementById('nextPageBottomButton');

    const disablePrev = currentPage <= 1;
    const disableNext = currentPage >= totalPages;

    if (prevTop) prevTop.disabled = disablePrev;
    if (prevBottom) prevBottom.disabled = disablePrev;
    if (nextTop) nextTop.disabled = disableNext;
    if (nextBottom) nextBottom.disabled = disableNext;
}

function buildUnscannedViewlist() {
    const actionConfig = {
        size: 1,
        color: 'success',
        buttonColor: 'success',
        iconClass: 'fas fa-check fa-lg',
        onClick: (book) => {
            recordValidBooksScanned([book], normalizeLocalNumber(book.localNumber));
            renderScanFeedback({
                type: 'success',
                title: `Marked Scanned (#${book.localNumber})`,
                message: `"${book.title || 'Untitled'}" was marked as scanned directly from the unscanned list.`,
                book
            });
        }
    };

    const columnsConfig = {
        left: {
            size: 6,
            align: 'left',
            lines: [
                {
                    type: 'title',
                    text: (book) => book.title || 'Untitled Book',
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
                        book.isbn ? `ISBN: ${book.isbn}` : null
                    ].filter(Boolean).join(' • ')
                }
            ]
        },
        center: {
            size: 3,
            align: 'centered',
            lines: [
                {
                    type: 'subtitle',
                    className: 'subtitle is-6 has-text-white mb-1',
                    text: (book) => {
                        const parts = [book.bookLocation, [book.call1, book.call2].filter(Boolean).join(' ')].filter(Boolean);
                        return parts.length > 0 ? parts.join(' — ') : 'Location not set';
                    }
                },
                {
                    type: 'subtitle',
                    className: 'subtitle is-7 has-text-warning',
                    text: (book) => {
                        const checkout = activeCheckoutsByBookId.get(Number(book.id));
                        return checkout
                            ? `Checked out: ${checkout.studentName || 'Student'}`
                            : 'Expected on Shelf';
                    }
                }
            ]
        },
        right: {
            size: 3,
            align: 'right',
            className: 'pr-4',
            contentClassName: 'mr-4',
            lines: [
                {
                    type: 'title',
                    className: 'title is-5 has-text-warning mb-1',
                    text: (book) => `Local #: ${book.localNumber || 'N/A'}`
                },
                {
                    type: 'subtitle',
                    className: 'subtitle is-7 has-text-grey-light',
                    text: () => 'Click ✓ to mark found'
                }
            ]
        }
    };

    return new Viewlist({
        container: '#unscannedBooksContainer',
        baseColor: 'primary',
        emptyMessage: 'All books in the library have been scanned! 🎉',
        imageConfig: {
            srcKey: 'coverUrl',
            placeholder: '/images/128x128.png',
            alt: (book) => `${book.title ?? 'Book'} cover`
        },
        actionConfig,
        columnsConfig
    });
}

function buildAddBookModal() {
    const modal = new Modal({
        title: 'Add New Book',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Add Book & Count Scanned',
        onConfirm: async (m) => await handleAddNewBookConfirm(m)
    });

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
                    const res = await fetch(`/api/books/external/${encodeURIComponent(isbnValue.trim())}`);
                    if (!res.ok) return;
                    const data = await res.json();
                    if (data.title && modal.getField('title')) modal.getField('title').value = data.title;
                    if (data.authors?.length && modal.getField('authorSearch')) {
                        modal.getField('authorSearch').value = data.authors.join(', ');
                    }
                    if (data.publisher && modal.getField('publisher')) modal.getField('publisher').value = data.publisher;
                    if (data.publishedDate && modal.getField('published')) {
                        modal.getField('published').value = data.publishedDate.substring(0, 4);
                    }
                    if (data.description && modal.getField('blurb')) modal.getField('blurb').value = data.description;
                } catch (error) {
                    console.warn('Auto-lookup failed:', error);
                }
            }
        },
        localNumber: {
            label: 'Local # / Barcode (Required):',
            type: 'text',
            placeholder: 'e.g. 1042',
            color: 'primary'
        },
        title: {
            label: 'Title (Required):',
            type: 'text',
            placeholder: 'Book title...',
            color: 'primary'
        },
        authorSearch: {
            label: 'Author:',
            type: 'search',
            placeholder: 'Search or type author name...',
            color: 'primary',
            minChars: 2,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/authors?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const authors = await res.json();
                return authors.map((a) => ({ id: a.id, text: a.name }));
            }
        },
        bookLocation: {
            label: 'Location / Section:',
            type: 'text',
            placeholder: 'e.g. Juvenile Fiction, Easy, Biography...',
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

function openAddBookModal(prefillLocalNumber = '') {
    if (!addBookModal) return;

    Object.values(addBookModal.inputs).forEach((input) => {
        if (input) input.value = '';
    });
    addBookModal.hiddenValues = {};

    const localField = addBookModal.getField('localNumber');
    if (localField && prefillLocalNumber) {
        localField.value = prefillLocalNumber;
    }

    addBookModal.open();

    setTimeout(() => {
        const targetField = prefillLocalNumber
            ? addBookModal.getField('isbn') || addBookModal.getField('title')
            : addBookModal.getField('localNumber') || addBookModal.getField('isbn');
        targetField?.focus();
    }, 50);
}

async function handleAddNewBookConfirm(m) {
    const title = (m.getField('title')?.value || '').trim();
    const localNumber = (m.getField('localNumber')?.value || '').trim();

    if (!title || !localNumber) {
        showWarning('Both <strong>Title</strong> and <strong>Local # / Barcode</strong> are required to add a book.');
        return false;
    }

    const formData = new FormData();
    formData.append('title', title);
    const authorSelection = m.hiddenValues['authorSearch'];
    if (authorSelection?.id) formData.append('authorId', authorSelection.id);
    formData.append('author', authorSelection?.text || m.getField('authorSearch')?.value || '');
    formData.append('localNumber', localNumber);
    formData.append('bookLocation', m.getField('bookLocation')?.value || '');
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

        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || 'Failed to create book');
        }

        const newBook = await response.json();
        allBooks.push(newBook);
        rebuildBookLookups();

        // Automatically count the newly added book as scanned in this inventory session
        recordValidBooksScanned([newBook], normalizeLocalNumber(newBook.localNumber));

        const scannerInput = document.getElementById('localNumberInput');
        if (scannerInput) scannerInput.value = '';

        renderScanFeedback({
            type: 'success',
            title: `Book Added & Counted! (Local #${newBook.localNumber})`,
            message: `"${newBook.title}" was added to the library catalog and counted in your valid inventory.`,
            book: newBook
        });

        setTimeout(() => focusScannerInput(), 50);
        return true;
    } catch (error) {
        console.error('Error adding book:', error);
        showWarning(error.message || 'Failed to add book');
        return false;
    }
}

function exportUnscannedBooksCsv() {
    const booksToExport = filteredUnscannedBooks.length > 0 ? filteredUnscannedBooks : getUnscannedBooks();
    if (booksToExport.length === 0) {
        showWarning('There are no unscanned books to export.');
        return;
    }

    const headers = ['Local Number', 'Title', 'Author', 'Location', 'Call 1', 'Call 2', 'ISBN', 'Published', 'Publisher', 'Checkout Status'];
    const escapeCsv = (val) => `"${String(val ?? '').replace(/"/g, '""')}"`;

    const rows = booksToExport.map((book) => {
        const checkout = activeCheckoutsByBookId.get(Number(book.id));
        const status = checkout
            ? `Checked out to ${checkout.studentName || 'Student'}`
            : 'Missing / Unscanned on Shelf';
        return [
            book.localNumber,
            book.title,
            book.author,
            book.bookLocation,
            book.call1,
            book.call2,
            book.isbn,
            book.published,
            book.publisher,
            status
        ].map(escapeCsv).join(',');
    });

    const csvContent = [headers.map(escapeCsv).join(','), ...rows].join('\r\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    const dateStamp = new Date().toISOString().slice(0, 10);
    a.href = url;
    a.download = `unscanned_books_${dateStamp}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}
