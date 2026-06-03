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
