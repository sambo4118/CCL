export function showWarning(message, onConfirm) {
    console.warn(`Warning: ${message}`);
    const warningModal = document.createElement('div');
    warningModal.classList.add('modal', 'is-active');
    
    const modalBackground = document.createElement('div');
    modalBackground.classList.add('modal-background');
    modalBackground.addEventListener('click', closeModal);
    warningModal.appendChild(modalBackground);

    const modalCard = document.createElement('div');
    modalCard.classList.add('modal-card');
    modalBackground.appendChild(modalCard);
    
    const modalHeader = document.createElement('header');
    modalHeader.classList.add('modal-card-header', 'has-background-danger');
    modalCard.appendChild(modalHeader);

    const modalTitle = document.createElement('p');
    modalTitle.classList.add('modal-card-title');
    modalTitle.textContent = 'Warning';
    modalHeader.appendChild(modalTitle);

    const closeButton = document.createElement('button');
    closeButton.classList.add('delete');
    closeButton.setAttribute('aria-label', 'close');
    closeButton.addEventListener('click', closeModal);
    modalHeader.appendChild(closeButton);

    const modalBody = document.createElement('section');
    modalBody.classList.add('modal-card-body');
    modalBody.innerHTML = `${message}`;
    modalCard.appendChild(modalBody);

    const modalFooter = document.createElement('footer');
    modalFooter.classList.add('modal-card-foot');
    modalCard.appendChild(modalFooter);

    const confirmButton = document.createElement('button');
    confirmButton.classList.add('button', 'is-danger');
    confirmButton.textContent = 'Confirm';
    confirmButton.addEventListener('click', () => {
        onConfirm();
        closeModal();
    });
    modalFooter.appendChild(confirmButton);

    const cancelButton = document.createElement('button');
    cancelButton.classList.add('button');
    cancelButton.textContent = 'Cancel';
    cancelButton.addEventListener('click', closeModal);
    modalFooter.appendChild(cancelButton);

    function closeModal() {
        warningModal.classList.remove('is-active');
        warningModal.remove();
    }

    document.body.appendChild(warningModal);
}

export async function loadBookDetails(bookId) {
    const book = await fetch(`/api/books/${bookId}`).then((responce) => responce.json());
    return book;
}

export function addChips( items, onClick, onDelete ) {
    const chipslist = [];
    for (const item of items) {
        const chipColumn = document.createElement('div');
        chipColumn.classList.add('column');

        const chip = document.createElement('p');
        chip.classList.add('notification', 'is-info', 'has-text-white');
        chip.style.display = 'inline-flex';
        chip.style.alignItems = 'center';
        chip.style.gap = '0.5em';
        chip.style.padding = '0.25em 0.5em';
        chip.style.margin = '0';
        chip.style.width = 'auto';
        chip.style.lineHeight = '1';

        const deleteButton = document.createElement('button');
        deleteButton.classList.add('delete');
        deleteButton.style.position = 'relative';
        deleteButton.style.top = 'auto';
        deleteButton.style.right = 'auto';
        deleteButton.style.insetInlineEnd = 'auto';
        deleteButton.style.marginLeft = '0.25em';

        deleteButton.addEventListener('click', (e) => {
            e.stopPropagation();
            chip.remove()
            if (onDelete) onDelete(item);
        });

        chip.append(item, deleteButton);

        if (onClick) chip.addEventListener('click', () => onClick(item));
        chipColumn.appendChild(chip);

        chipslist.push(chipColumn);
    }
    return chipslist;
}