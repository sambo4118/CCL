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
    updateAuthSection();
    populateClassesDropdown();
});

async function updateAuthSection() {

    const response = await fetch('/api/me');
    const data = await response.json();
    const user = data.user;
    console.debug('User data:', data);
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
