import { addChips, showWarning } from '../utils.js'

export async function setupManageUsers() {
    const manageUsersButton = document.getElementById('manageUsersButton');
    if (!manageUsersButton) return console.error('Manage Users button not found');
    manageUsersButton.addEventListener('click', openManageUsersModal);
}

async function openManageUsersModal() {

    const modal = document.getElementById('manageUsersModal');
    if (!modal) return console.error('Manage Users modal not found');
    modal.classList.add('is-active');

    const closemodal = () => {
        modal.classList.remove('is-active');
    };

    const closeButton = modal.querySelector('.delete');
    if (closeButton) closeButton.addEventListener('click', closemodal);

    const footerCloseButton = document.getElementById('closeManageUsersFooterButton');
    if (footerCloseButton) footerCloseButton.addEventListener('click', closemodal);

    const chipContainer = document.getElementById('userChipsContainer');
    const emailInput = document.getElementById('newUserEmail');
    const addUserButton = document.getElementById('addUserButton');

    await refreshChips(chipContainer);

    const submitNewUser = async () => {
        const email = (emailInput.value || '').trim().toLowerCase();
        if (!email) return;

        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
            showWarning('Please enter a valid email address.', () => {});
            return;
        }

        addUserButton.classList.add('is-loading');
        try {
            const ok = await addUser(email);
            if (ok) {
                emailInput.value = '';
                await refreshChips(chipContainer);
            }
        } finally {
            addUserButton.classList.remove('is-loading');
        }
    };

    addUserButton.addEventListener('click', submitNewUser);
    emailInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') submitNewUser();
    });
}

async function refreshChips(chipContainer) {
    const users = await loadUsers();
    if (!users) return;
    const emails = users.map(u => u.email);
    const chipList = addChips(emails, null, (email) => {
        showWarning(`Remove <strong>${email}</strong> from the allowed users?`, async () => {
            await removeUser(email);
            await refreshChips(chipContainer);
        });
    });
    chipContainer.replaceChildren(...chipList);
}

async function loadUsers() {
    const response = await fetch('/api/users');
    if (!response.ok) {
        console.error('Failed to load users:', response.statusText);
        return;
    }
    const { loggedIn, users } = await response.json();

    if (!loggedIn) {
        console.warn('User not logged in, cannot load users');
        return;
    }

    return users;
}

async function addUser(email) {
    const response = await fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email })
    });
    if (!response.ok) {
        const text = await response.text().catch(() => response.statusText);
        showWarning(`Failed to add user: ${text}`, () => {});
        return false;
    }
    return true;
}

async function removeUser(email) {
    const response = await fetch(`/api/users/${encodeURIComponent(email)}`, {
        method: 'DELETE'
    });
    if (!response.ok) {
        const text = await response.text().catch(() => response.statusText);
        showWarning(`Failed to remove user: ${text}`, () => {});
        return false;
    }
    return true;
}
