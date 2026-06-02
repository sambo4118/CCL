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
});

