function closeModal(modal) {
    this.classList.remove('is-active');
    this.remove();
}

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
        closeModal(warningModal);
    });
    modalFooter.appendChild(confirmButton);

    const cancelButton = document.createElement('button');
    cancelButton.classList.add('button');
    cancelButton.textContent = 'Cancel';
    cancelButton.addEventListener('click', closeModal(warningModal));
    modalFooter.appendChild(cancelButton);

    document.body.appendChild(warningModal);
}

export async function loadBookDetails(bookId) {
    const responce = await fetch(`/api/books/${bookId}`)
    if (!responce.ok) { console.error('fetch error:', responce.json()); return false; }
    
    const book = responce.json();
    return book;
}

export async function loadClassDetails(classId) {
    const targetClass = await fetch(`/api/classes/${classId}`).then((responce) => responce.json());
    return targetClass;
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

export class Modal { 
    constructor({mainColorBulmaVariable, successButtonText, title, onConfirm}) {

        if (!mainColorBulmaVariable) mainColorBulmaVariable = "success";
        
        this.configKeys = [];
        this.isOpen = false;
        
        // 1. Create the main modal container
        this.element = document.createElement('div');
        this.element.className = 'modal';

        // 2. Inject the HTML
        this.element.innerHTML = `
            <div class="modal-background"></div>
            <div class="modal-card">
                <header class="modal-card-head">
                    <p class="modal-card-title">Modal title</p>
                    <button class="delete" aria-label="close"></button>
                </header>
                <section class="modal-card-body">
                </section>
                <footer class="modal-card-foot">
                    <div class="buttons">
                        <button class="button save-btn">Save changes</button>
                        <button class="button cancel-btn">Cancel</button>
                    </div>
                </footer>
            </div>
        `;

        // 3. Append the new modal to the page body so it exists in the DOM
        document.body.appendChild(this.element);

        // 4. Map properties to the newly created DOM elements
        this.background = this.element.querySelector('.modal-background');
        this.head = this.element.querySelector('.modal-card-head');
        this.title = this.element.querySelector('.modal-card-title');
        this.closeButton = this.element.querySelector('.delete');
        this.body = this.element.querySelector('.modal-card-body');
        this.foot = this.element.querySelector('.modal-card-foot');
        this.footButtons = this.element.querySelector('.buttons');
        this.successButton = this.element.querySelector('.save-btn');
        this.dangerButton = this.element.querySelector('.cancel-btn');

        // 5. Apply Bulma variables and parameters
        this.head.classList.add(`has-background-${mainColorBulmaVariable}`);
        this.title.textContent = title ?? "Title not specified";
        this.successButton.classList.add(`is-${mainColorBulmaVariable}`);
        this.successButton.textContent = successButtonText ?? "Save";

        // 6. Bind Event Listeners
        this.successButton.addEventListener('click', () => {
            if (onConfirm) onConfirm(this);
            this.close();
        });

        this.dangerButton.addEventListener('click', () => this.close());
        this.closeButton.addEventListener('click', () => this.close());
        this.background.addEventListener('click', () => this.close());

        // 7. Field tracking setup
        this.fields = {};
        this.labels = {};
        this.controlDivs = {};
        this.inputs = {};
        this.values = {};
    }

    open() {
        this.element.classList.add('is-active');
    }

    close() {
        this.element.classList.remove('is-active');
    }
    
    // Config is type {elementNameKey: {label: 'text', type: 'input type', color: 'bulma color', placeholder: 'input placeholder text'}, ...}
    addFields(config) {
        
        this.configKeys.push(...Object.keys(config));

        Object.entries(config).forEach(([key, data]) => {

            this.fields[key] = document.createElement('div');
            this.fields[key].className = 'field';

            this.labels[key] = document.createElement('div');
            this.labels[key].textContent = data.label;
            this.fields[key].appendChild(this.labels[key]);

            this.controlDivs[key] = document.createElement('div');
            this.fields[key].appendChild(this.controlDivs[key]);

            this.inputs[key] = document.createElement('input');
            this.inputs[key].className = `is-${data.color ?? this.mainColorBulmaVariable} input`;
            this.inputs[key].type = data.type;
            this.inputs[key].placeholder = data.placeholder;
            if (data.value) this.inputs[key].value = data.value;
            
            this.controlDivs[key].appendChild(this.inputs[key]);
            this.body.appendChild(this.fields[key]);
        });
    }

    getAllFields() {
        const values = {};
        Object.entries(this.inputs).forEach(([key, input]) => {
            values[key] = input.value;
        });
        return values;
    }

    getField(fieldKey) {
        return this.inputs[fieldKey];
    }
}