document.addEventListener("DOMContentLoaded", () => {
    const searchInput = document.getElementById("searchInput");
    let timer;
    let inflight = false;
    
    const setLoading = (on) => {
        searchInput.classList.toggle("is-loading", on);
    };

    searchInput.addEventListener("input", (e) => {
        clearTimeout(timer);
        setLoading(true);

        const value = e.target.value;
        timer = setTimeout(async () => {
            inflight = true;
            try {
                const books = await searchBooks(value);
                displaySearchResults(books);
            } catch (err) {
                console.error("Error fetching search results:", err);
            } finally {
                inflight = false;
                setLoading(false);
            }
        }, 300);
    })

});

function displaySearchResults(books) {
    const searchResultsContainer = document.getElementById("searchResults");
    if (!searchResultsContainer) return console.warn("Search results container not found");
    searchResultsContainer.replaceChildren();

    const placeholder = "/images/128x128.png";

    for (const book of books) {
        const article = document.createElement("article");
        article.className = "media";

        // Cover (left)
        const figure = document.createElement("figure");
        figure.className = "media-left";
        const imageContainer = document.createElement("p");
        imageContainer.className = "image is-96x128";
        const img = document.createElement("img");
        img.src = book.coverUrl || placeholder;
        img.alt = `${book.title ?? "Book"} cover`;
        imageContainer.appendChild(img);
        figure.appendChild(imageContainer);

        // Content
        const content = document.createElement("div");
        content.className = "media-content";

        // Top row: title + subtitle (left), author (right)
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

        // Bottom row: publish date (left)
        const bottomRow = document.createElement("p");
        bottomRow.className = "is-size-7 has-text-grey";
        bottomRow.textContent = book.publishDate ?? book.published ?? "";

        content.appendChild(topRow);
        content.appendChild(bottomRow);

        article.appendChild(figure);
        article.appendChild(content);
        searchResultsContainer.appendChild(article);
    }
}
async function searchBooks(query) {
    const response = await fetch(`/api/books/search?q=${encodeURIComponent(query)}`);
    if (!response.ok) {
        const body = await response.text();
        throw new Error(`Search failed ${response.status} ${response.statusText}: ${body}`);
    }
    return response.json();
}