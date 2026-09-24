document.addEventListener("DOMContentLoaded", () => {
    const searchInput = document.getElementById("searchInput");
    const searchResultsContainer = document.getElementById("searchResults");
    let timer;
    let requestId = 0;

    const setLoading = (on) => {
        const searchControl = searchInput?.closest(".control");
        if (searchControl) {
            searchControl.classList.toggle("is-loading", on);
        } else if (searchInput) {
            searchInput.classList.toggle("is-loading", on);
        }
    };

    const executeSearch = (value) => {
        clearTimeout(timer);
        const query = value.trim();

        // Empty input: bail immediately, no spinner, clear results.
        if (!query) {
            setLoading(false);
            if (searchResultsContainer) {
                searchResultsContainer.replaceChildren();
                searchResultsContainer.style.display = "none";
            }
            requestId++; // invalidate any in-flight response
            const url = new URL(window.location);
            url.searchParams.delete("q");
            window.history.replaceState({}, "", url.pathname + (url.search ? url.search : ""));
            return;
        }

        setLoading(true);
        const myId = ++requestId;

        timer = setTimeout(async () => {
            try {
                const results = await searchAll(query);
                if (myId !== requestId) return; // stale, ignore

                const url = new URL(window.location);
                url.searchParams.set("q", query);
                window.history.replaceState({}, "", url.pathname + url.search);

                displaySearchResults(results, query);
            } catch (err) {
                if (myId !== requestId) return;
                console.error("Error fetching search results:", err);
            } finally {
                if (myId === requestId) setLoading(false);
            }
        }, 300);
    };

    searchInput.addEventListener("input", (e) => {
        executeSearch(e.target.value);
    });

    // Check for existing query in URL (e.g. /search?q=...)
    const initialQuery = new URLSearchParams(window.location.search).get("q");
    if (initialQuery && initialQuery.trim()) {
        searchInput.value = initialQuery;
        executeSearch(initialQuery);
    }
});

async function searchAll(query) {
    const encoded = encodeURIComponent(query);

    const [booksRes, studentsRes, classesRes, authorsRes] = await Promise.all([
        fetch(`/api/books/search?q=${encoded}`).then(r => r.ok ? r.json() : []).catch(() => []),
        fetch(`/api/students?search=${encoded}`).then(r => r.ok ? r.json() : []).catch(() => []),
        fetch(`/api/classes?search=${encoded}`).then(r => r.ok ? r.json() : []).catch(() => []),
        fetch(`/api/authors?search=${encoded}`).then(r => r.ok ? r.json() : []).catch(() => [])
    ]);

    const books = (Array.isArray(booksRes) ? booksRes : []).map(b => ({ ...b, itemType: "book" }));
    const students = (Array.isArray(studentsRes) ? studentsRes : []).map(s => ({ ...s, itemType: "student" }));
    const classes = (Array.isArray(classesRes) ? classesRes : []).map(c => ({ ...c, itemType: "class" }));
    const authors = (Array.isArray(authorsRes) ? authorsRes : []).map(a => ({ ...a, itemType: "author" }));

    const allResults = [...books, ...students, ...classes, ...authors];

    // Sort by relevance to query
    const qLower = query.toLowerCase().trim();
    allResults.sort((a, b) => getRelevanceScore(b, qLower) - getRelevanceScore(a, qLower));

    return allResults;
}

function getRelevanceScore(item, q) {
    const primary = (item.title || item.name || "").toLowerCase();
    if (primary === q) return 100;
    if (primary.startsWith(q)) return 80;
    if (primary.includes(q)) return 60;

    const secondary = (item.author || item.teacherName || item.className || item.subtitle || "").toLowerCase();
    if (secondary.startsWith(q)) return 40;
    if (secondary.includes(q)) return 30;

    return 10;
}

function displaySearchResults(results, query) {
    const searchResultsContainer = document.getElementById("searchResults");
    if (!searchResultsContainer) return console.warn("Search results container not found");
    searchResultsContainer.replaceChildren();

    if (results.length === 0) {
        searchResultsContainer.style.display = "block";
        const emptyState = document.createElement("div");
        emptyState.className = "has-text-centered has-text-grey p-5";

        const iconSpan = document.createElement("span");
        iconSpan.className = "icon is-large mb-2";
        iconSpan.innerHTML = '<i class="fas fa-2x fa-search"></i>';

        const p = document.createElement("p");
        p.className = "is-size-5";
        p.textContent = `No results found for "${query}"`;

        emptyState.appendChild(iconSpan);
        emptyState.appendChild(p);
        searchResultsContainer.appendChild(emptyState);
        return;
    }

    searchResultsContainer.style.display = "block";

    for (const item of results) {
        searchResultsContainer.appendChild(createResultElement(item));
    }
}

function createResultElement(item) {
    switch (item.itemType) {
        case "book":
            return createBookElement(item);
        case "student":
            return createStudentElement(item);
        case "class":
            return createClassElement(item);
        case "author":
            return createAuthorElement(item);
        default:
            return createBookElement(item);
    }
}

function createBookElement(book) {
    const placeholder = "/images/128x128.png";
    const article = document.createElement("article");
    article.className = "media";

    const figure = document.createElement("figure");
    figure.className = "media-left";
    const imageContainer = document.createElement("p");
    imageContainer.className = "image";
    const img = document.createElement("img");
    img.src = book.coverUrl || placeholder;
    img.alt = `${book.title ?? "Book"} cover`;
    img.loading = "lazy";
    img.style.width = "96px";
    img.style.height = "128px";
    img.style.objectFit = "fill"; // squish to a uniform 96x128
    img.addEventListener("error", () => {
        if (img.src !== placeholder) img.src = placeholder;
    });
    imageContainer.appendChild(img);
    figure.appendChild(imageContainer);

    const linkImage = document.createElement("a");
    linkImage.href = `/books/${book.id}`;

    const linkTitle = document.createElement("a");
    linkTitle.href = `/books/${book.id}`;
    linkTitle.className = "media-content";

    const topRow = document.createElement("div");
    topRow.className = "level is-mobile mb-2";

    const topLeft = document.createElement("div");
    topLeft.className = "level-left";

    const titleWrap = document.createElement("div");
    titleWrap.className = "level-item";

    const title = document.createElement("span");
    title.className = "title is-5 mr-2";

    title.textContent = book.title ?? "Untitled";
    titleWrap.appendChild(title);

    if (book.subtitle) {
        const subtitle = document.createElement("span");
        subtitle.className = "subtitle is-6 has-text-grey";
        subtitle.textContent = book.subtitle;
        titleWrap.appendChild(subtitle);
    }
    topLeft.appendChild(titleWrap);

    const topRight = document.createElement("div");
    topRight.className = "level-right";

    const authorItem = document.createElement("div");
    authorItem.className = "level-item has-text-grey";
    authorItem.textContent = book.author ?? "";
    topRight.appendChild(authorItem);

    topRow.appendChild(topLeft);
    topRow.appendChild(topRight);

    const bottomRow = document.createElement("p");
    bottomRow.className = "is-size-7 has-text-grey";
    const parts = [];
    if (book.publishDate ?? book.published) parts.push(book.publishDate ?? book.published);
    if (book.isbn) parts.push(`ISBN: ${book.isbn}`);
    if (book.id != null) parts.push(`#${book.id}`);
    bottomRow.textContent = parts.join(" • ");

    linkTitle.appendChild(topRow);
    linkTitle.appendChild(bottomRow);

    linkImage.appendChild(figure);
    article.appendChild(linkImage);
    article.appendChild(linkTitle);

    return article;
}

function createStudentElement(student) {
    const article = document.createElement("article");
    article.className = "media";

    const figure = document.createElement("figure");
    figure.className = "media-left";
    const imageContainer = document.createElement("p");
    imageContainer.className = "image";
    imageContainer.style.width = "96px";
    imageContainer.style.height = "128px";
    imageContainer.style.display = "flex";
    imageContainer.style.alignItems = "center";
    imageContainer.style.justifyContent = "center";
    imageContainer.style.backgroundColor = "#ebfbee";
    imageContainer.style.borderRadius = "4px";
    imageContainer.innerHTML = '<span class="icon is-large has-text-success"><i class="fas fa-3x fa-user-graduate"></i></span>';
    figure.appendChild(imageContainer);

    const linkImage = document.createElement("a");
    linkImage.href = `/students/${student.id}`;

    const linkTitle = document.createElement("a");
    linkTitle.href = `/students/${student.id}`;
    linkTitle.className = "media-content";

    const topRow = document.createElement("div");
    topRow.className = "level is-mobile mb-2";

    const topLeft = document.createElement("div");
    topLeft.className = "level-left";

    const titleWrap = document.createElement("div");
    titleWrap.className = "level-item";

    const title = document.createElement("span");
    title.className = "title is-5 mr-2";
    title.textContent = student.name ?? "Untitled Student";
    titleWrap.appendChild(title);

    const tag = document.createElement("span");
    tag.className = "tag is-success is-light";
    tag.textContent = "Student";
    titleWrap.appendChild(tag);

    topLeft.appendChild(titleWrap);

    const topRight = document.createElement("div");
    topRight.className = "level-right";

    const classItem = document.createElement("div");
    classItem.className = "level-item has-text-grey";
    classItem.textContent = student.className ? `Class: ${student.className}` : "";
    topRight.appendChild(classItem);

    topRow.appendChild(topLeft);
    topRow.appendChild(topRight);

    const bottomRow = document.createElement("p");
    bottomRow.className = "is-size-7 has-text-grey";
    bottomRow.textContent = `Student ID: #${student.id}`;

    linkTitle.appendChild(topRow);
    linkTitle.appendChild(bottomRow);

    linkImage.appendChild(figure);
    article.appendChild(linkImage);
    article.appendChild(linkTitle);

    return article;
}

function createClassElement(c) {
    const article = document.createElement("article");
    article.className = "media";

    const figure = document.createElement("figure");
    figure.className = "media-left";
    const imageContainer = document.createElement("p");
    imageContainer.className = "image";
    imageContainer.style.width = "96px";
    imageContainer.style.height = "128px";
    imageContainer.style.display = "flex";
    imageContainer.style.alignItems = "center";
    imageContainer.style.justifyContent = "center";
    imageContainer.style.backgroundColor = "#e8f4fd";
    imageContainer.style.borderRadius = "4px";
    imageContainer.innerHTML = '<span class="icon is-large has-text-link"><i class="fas fa-3x fa-chalkboard-user"></i></span>';
    figure.appendChild(imageContainer);

    const linkImage = document.createElement("a");
    linkImage.href = `/classes/${c.id}`;

    const linkTitle = document.createElement("a");
    linkTitle.href = `/classes/${c.id}`;
    linkTitle.className = "media-content";

    const topRow = document.createElement("div");
    topRow.className = "level is-mobile mb-2";

    const topLeft = document.createElement("div");
    topLeft.className = "level-left";

    const titleWrap = document.createElement("div");
    titleWrap.className = "level-item";

    const title = document.createElement("span");
    title.className = "title is-5 mr-2";
    title.textContent = c.name ?? "Untitled Class";
    titleWrap.appendChild(title);

    const tag = document.createElement("span");
    tag.className = "tag is-link is-light";
    tag.textContent = "Class";
    titleWrap.appendChild(tag);

    topLeft.appendChild(titleWrap);

    const topRight = document.createElement("div");
    topRight.className = "level-right";

    const teacherItem = document.createElement("div");
    teacherItem.className = "level-item has-text-grey";
    teacherItem.textContent = c.teacherName ? `Teacher: ${c.teacherName}` : "";
    topRight.appendChild(teacherItem);

    topRow.appendChild(topLeft);
    topRow.appendChild(topRight);

    const bottomRow = document.createElement("p");
    bottomRow.className = "is-size-7 has-text-grey";
    const count = c.studentCount ?? 0;
    bottomRow.textContent = `${count} ${count === 1 ? "student" : "students"} enrolled`;

    linkTitle.appendChild(topRow);
    linkTitle.appendChild(bottomRow);

    linkImage.appendChild(figure);
    article.appendChild(linkImage);
    article.appendChild(linkTitle);

    return article;
}

function createAuthorElement(author) {
    const article = document.createElement("article");
    article.className = "media";

    const figure = document.createElement("figure");
    figure.className = "media-left";
    const imageContainer = document.createElement("p");
    imageContainer.className = "image";
    imageContainer.style.width = "96px";
    imageContainer.style.height = "128px";
    imageContainer.style.display = "flex";
    imageContainer.style.alignItems = "center";
    imageContainer.style.justifyContent = "center";
    imageContainer.style.backgroundColor = "#fef9e7";
    imageContainer.style.borderRadius = "4px";
    imageContainer.innerHTML = '<span class="icon is-large has-text-warning-dark"><i class="fas fa-3x fa-feather-pointed"></i></span>';
    figure.appendChild(imageContainer);

    const linkImage = document.createElement("a");
    linkImage.href = `/authors/${author.id}`;

    const linkTitle = document.createElement("a");
    linkTitle.href = `/authors/${author.id}`;
    linkTitle.className = "media-content";

    const topRow = document.createElement("div");
    topRow.className = "level is-mobile mb-2";

    const topLeft = document.createElement("div");
    topLeft.className = "level-left";

    const titleWrap = document.createElement("div");
    titleWrap.className = "level-item";

    const title = document.createElement("span");
    title.className = "title is-5 mr-2";
    title.textContent = author.name ?? "Untitled Author";
    titleWrap.appendChild(title);

    const tag = document.createElement("span");
    tag.className = "tag is-warning is-light";
    tag.textContent = "Author";
    titleWrap.appendChild(tag);

    topLeft.appendChild(titleWrap);

    const topRight = document.createElement("div");
    topRight.className = "level-right";

    const countItem = document.createElement("div");
    countItem.className = "level-item has-text-grey";
    countItem.textContent = author.bookCount ? `${author.bookCount} books` : "";
    topRight.appendChild(countItem);

    topRow.appendChild(topLeft);
    topRow.appendChild(topRight);

    const bottomRow = document.createElement("p");
    bottomRow.className = "is-size-7 has-text-grey";
    const parts = [];
    if (author.id != null) parts.push(`Author ID: #${author.id}`);
    if (author.bookCount != null) parts.push(`${author.bookCount} ${author.bookCount === 1 ? "book" : "books"}`);
    bottomRow.textContent = parts.join(" • ");

    linkTitle.appendChild(topRow);
    linkTitle.appendChild(bottomRow);

    linkImage.appendChild(figure);
    article.appendChild(linkImage);
    article.appendChild(linkTitle);

    return article;
}

