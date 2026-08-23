document.addEventListener('DOMContentLoaded', () => {
    const burgers = document.querySelectorAll('.navbar-burger');
    burgers.forEach(b => {
        b.addEventListener('click', () => {
        const target = document.getElementById(b.dataset.target);
        b.classList.toggle('is-active');
        target?.classList.toggle('is-active');
        });
    });
    const dropdowns = document.querySelectorAll('.navbar-item.has-dropdown');
    dropdowns.forEach(d => {
        d.addEventListener('click', () => {
        d.classList.toggle('is-active');
        });
    });
    initAuthGatedNav();
    populateClassesDropdown();

    // If we got bounced from a protected page, surface why.
    const params = new URLSearchParams(window.location.search);
    if (params.get('denied') === 'settings') {
        showAuthWarning('You must be signed in to access Settings.');
        params.delete('denied');
        const newSearch = params.toString();
        const cleanUrl = window.location.pathname + (newSearch ? `?${newSearch}` : '') + window.location.hash;
        window.history.replaceState({}, '', cleanUrl);
    }
});

let cachedAuth = null;

async function fetchAuth() {
    if (cachedAuth) return cachedAuth;
    try {
        const response = await fetch('/api/me');
        cachedAuth = await response.json();
    } catch (err) {
        console.error('Failed to fetch auth state:', err);
        cachedAuth = { loggedIn: false };
    }
    return cachedAuth;
}

async function initAuthGatedNav() {
    const data = await fetchAuth();
    updateAuthSection(data);

    const settingsLink = document.getElementById('settingsNavItem');
    if (settingsLink && !data.loggedIn) {
        settingsLink.addEventListener('click', (e) => {
            e.preventDefault();
            showAuthWarning('You must be signed in to access Settings.');
        });
    }
}

function updateAuthSection(data) {
    const user = data.user;
    if (!data.loggedIn) return;

    const authSection = document.getElementById('authSection');
    if (!authSection) return console.warn('Auth section not found in navbar');

    const signOutLink = document.createElement('a');
    signOutLink.href = '/logout';

    const userProfile = document.createElement('figure');
    userProfile.className = 'image is-32x32';
    userProfile.style.alignSelf = 'center';

    if (user.picture) {
        const image = document.createElement('img');
        image.src = user.picture;
        image.className = 'is-rounded';
        image.style.maxHeight = 'none';
        image.style.width = '100%';
        image.style.height = '100%';
        image.style.objectFit = 'cover';
        userProfile.appendChild(image);
    } else {
        const placeholder = document.createElement('span');
        placeholder.className = 'icon is-medium';
        placeholder.innerHTML = '<i class="fas fa-user-circle fa-lg"></i>';
        userProfile.appendChild(placeholder);
    }
    signOutLink.appendChild(userProfile);
    authSection.replaceChildren(signOutLink);
}

function showAuthWarning(message) {
    const modal = document.createElement('div');
    modal.className = 'modal is-active';

    const close = () => modal.remove();

    const background = document.createElement('div');
    background.className = 'modal-background';
    background.addEventListener('click', close);

    const card = document.createElement('div');
    card.className = 'modal-card';

    const head = document.createElement('header');
    head.className = 'modal-card-head has-background-warning';
    const title = document.createElement('p');
    title.className = 'modal-card-title';
    title.textContent = 'Authentication required';
    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'delete';
    deleteBtn.setAttribute('aria-label', 'close');
    deleteBtn.addEventListener('click', close);
    head.append(title, deleteBtn);

    const body = document.createElement('section');
    body.className = 'modal-card-body';
    body.textContent = message;

    const foot = document.createElement('footer');
    foot.className = 'modal-card-foot';
    const loginBtn = document.createElement('a');
    loginBtn.className = 'button is-info has-text-white mr-2';
    loginBtn.href = '/login';
    loginBtn.textContent = 'Log in';
    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'button';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', close);
    foot.append(loginBtn, cancelBtn);

    card.append(head, body, foot);
    modal.append(background, card);
    document.body.appendChild(modal);
}

async function getClassList() {
    const response = await fetch('/api/classes');
    if (!response.ok) {
        console.error('Failed to load classes:', response.statusText);
        return [];
    }
    return response.json();
}

async function populateClassesDropdown() {
    const dropdown = document.getElementById('classesDropdown');
    if (!dropdown) return;

    const classes = await getClassList();
    if (!Array.isArray(classes) || classes.length === 0) {
        dropdown.replaceChildren();
        const empty = document.createElement('span');
        empty.className = 'navbar-item has-text-grey';
        empty.textContent = 'No classes yet';
        dropdown.appendChild(empty);
        return;
    }

    classes.sort(compareClasses);

    const items = classes.map((cls) => {
        const link = document.createElement('a');
        link.className = 'navbar-item';
        link.href = `/classes/${cls.id}`;
        link.textContent = cls.name;
        return link;
    });

    dropdown.replaceChildren(...items);
}

// Sort "Grade K" first, then "Grade 1", "Grade 2", ..., then anything else alphabetically.
function compareClasses(a, b) {
    return classSortKey(a.name) - classSortKey(b.name)
        || String(a.name).localeCompare(String(b.name));
}

function classSortKey(name) {
    const value = String(name).trim();
    const kMatch = /^Grade\s+K$/i.test(value);
    if (kMatch) return 0;
    const numMatch = value.match(/^Grade\s+(\d+)$/i);
    if (numMatch) return Number(numMatch[1]);
    return Number.MAX_SAFE_INTEGER;
}
