export function showWarning(message, {object = null, onConfirm = null, onCancel = null }) {
    console.warn(`Warning: ${message}`, object);

    const warningModal = document.createElement('div');
    warningModal.classList.add('modal', 'is-active');
    warningModal.innerHTML = `
        <div class="modal-background"> </div>
        <div class="modal-card">
            <header class="modal-card-head has-background-danger">
                <p class="modal-card-title">Warning</p>
                <button class="delete" aria-label="close"></button>
            </header>
            <section class="modal-card-body">
                ${message}
            </section>
            <footer class="modal-card-foot">
                <button class="button is-danger conf-btn">Confirm</button>
                <button class="button canc-btn ml-2">Cancel</button>
            </footer>
        </div>
    `    
    const closeModal = () => {
        warningModal.classList.remove('is-active');
        warningModal.remove()
    }

    if (!onConfirm) onConfirm = () => closeModal;
    if (!onCancel) onCancel = () => closeModal;

    warningModal.querySelectorAll('.modal-background, .modal-close, .modal-card-head .delete, .modal-card-foot .button').forEach((close) => {
        close.addEventListener('click', closeModal);
    }) 

    const modalFooter = warningModal.querySelector('.modal-card-foot')

    const confirmButton = modalFooter.querySelector('.conf-btn');
    confirmButton.addEventListener('click', () => {
        onConfirm();
        closeModal;
    });

    const cancelButton = modalFooter.querySelector('.canc-btn');;
    cancelButton.addEventListener('click', onCancel);

    document.body.appendChild(warningModal);
}

export function showAuthWarning(message) {
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

export async function loadBookDetails(bookId) {
    const responce = await fetch(`/api/books/${bookId}`)
    if (!responce.ok) { console.error('fetch error:', responce.json()); return false; }
    
    const book = responce.json();
    return book;
}

export async function loadClassDetails(classId) {
    let responce = await fetch(`/api/classes/${classId}`);

    if (!responce.ok) {
        if (responce.status == 401) {
            showAuthWarning('You are not authrized to view classes, or students please sign in');
            return { success: false, responce };
        } else {
            showWarning('error loading class', { object: responce });
            return { success: false, responce };
        }
    }

    const targetClass = await responce.json();
    return { success: true, targetClass };
}

export class Chips {
    constructor({
        container,
        items = [],
        labelKey = 'label',
        onClick = null,
        onDelete = null,
        className = 'columns mb-5 is-multiline is-mobile' }) {
        
        this.containerParent = typeof container === 'string' ? document.querySelector(container) : container; // resolve from string name of container

        this.container = document.createElement('div');
        this.container.className = className;
        this.containerParent.appendChild(this.container);

        this.onClick = onClick
        this.onDelete = onDelete
        this.items = [];
        this.labelKey = labelKey;
        this.className = className;

        if (items.length) {
            this.addItems(items);
        }
        
    }

    createChipElement(item) {

        const isObject = item !== null && typeof item === 'object';
        const displayLabel = isObject 
            ? (typeof this.labelKey === 'function' ? this.labelKey(item) : item[this.labelKey] ?? JSON.stringify(item))
            : item;
        
        const chipColumn = document.createElement('div');
        chipColumn.classList.add('column', 'is-narrow');

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

        deleteButton.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            this.removeItem(item, chipColumn);
            if (this.onDelete) this.onDelete({ item, chip });
        });

        chip.append(String(displayLabel), deleteButton);

        if (this.onClick) chip.addEventListener('click', () => this.onClick(item));
        chipColumn.appendChild(chip);

        return chipColumn;
    }

    addItem(item) {
        this.items.push(item);
        const element = this.createChipElement(item);
        if (this.container) {
            this.container.appendChild(element)
        }
        return element;
    }

    addItems(items) {
        return items.map((item) => this.addItem(item));
    }

    removeItem(item, element) {
        this.items = this.items.filter((i) => i !== item);
        if (element) {
            element.remove();
        }
        if (this.onDelete) {
            this.onDelete(item);
        }
    }

    clear() {
        this.items = [];
        if (this.container) {
            this.container.innerHTML = '';
        }
    }

    getItems() {
        return [...this.items];
    }
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
        this.hiddenValues = {};
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

            const { fieldContainer, controlDiv } = this._createFieldContainer(data.label);
            
            switch (data.type) {
                case 'search':
                    this._renderSearchField(key, data, controlDiv);
                    break;
                case 'chips':
                    this._renderChipsField(key, data, controlDiv);
                default:
                    this._renderStandardField(key, data, controlDiv);
                    break;
                    
            }
            this.body.appendChild(fieldContainer);
        });
    }

    _createFieldContainer(labelText = null) {
        const field = document.createElement('div');
        field.className = 'field';

        if (labelText) {
            const label = document.createElement('label');
            label.textContent = labelText;
            field.appendChild(label);
        }

        const control = document.createElement('div')
        control.className = 'control'

        field.appendChild(control)

        return { fieldContainer: field, controlDiv: control };
    }
    
    // types = text, password, email, tel: no special data
    _renderStandardField(key, data, controlDiv) {
        
        const input = document.createElement('input');
        input.className = `input is-${data.color ?? this.mainColorBulmaVariable}`;
        input.type = data.type ?? 'text';
        input.placeholder = data.placeholder ?? '';
        if (data.value) input.value = data.value;

        controlDiv.appendChild(input);
        this.inputs[key] = input; // Keep reference for retrieval
        this.controlDivs[key] = controlDiv;
    }
    
    // type = search
    // takes resultsQuery, and minChars args
    // resultsQuery is a function that takes input.value and returns type [{text: "text", id:int}...]
    _renderSearchField(key, data, controlDiv) {
        const dropdown = document.createElement('div');
        dropdown.className = `dropdown is-up is-fullwidth`

        dropdown.innerHTML = (
            `<div class="field">
                <div class="control">
                    <input class="input is-${data.color ?? this.mainColorBulmaVariable}" type="text" placeholder="${data.placeholder}">
                </div>
            </div>
            <div class="dropdown-menu" role="menu">
                <div class="dropdown-content">
                
                </div)
            </div>
            `
        );

        const debouncedSearch = this._debounce(async (searchValue) => {
            try {
                const results = await data.resultsQuery(searchValue);
                this._addSearchDropdownContent(dropdown, dropdownContent, results, input, key, data);
            } catch(error) {
                console.error('search query failed reason:', error);
            }
        }, 300)

        const input = dropdown.querySelector('.input');
        const dropdownContent = dropdown.querySelector('.dropdown-content');

        input.addEventListener('input', (event) => {
            const value = event.target.value;
            const minChars = data.minChars ?? 3

            if (value.length < minChars) {
                if (value.length == 0) dropdownContent.innerHTML = ''
                return false;
            };

            debouncedSearch(value);
           
        })

        controlDiv.parentNode.replaceChild(dropdown, controlDiv)

        this.inputs[key] = input;
        this.controlDivs[key] = dropdown;
    }

    _addSearchDropdownContent(dropdown, dropdownContent, results, input, key, data) {

        let dropdownItem;
        let dropdownText;
        dropdownContent.innerHTML = ''
        dropdown.classList.remove('is-active');

        results.forEach((result, index) => {
            
            if (!result.id) throw new Error(`results does not meet the minimum required shape at ${index}`);
            if (!result.text) result.text = result.id;

            dropdownItem = document.createElement('div');
            dropdownItem.className = 'dropdown-item';

            dropdownText = document.createElement('button');
            dropdownText.type = 'button';
            dropdownText.textContent = result.text;

            dropdownText.addEventListener('click', () =>{
                input.value = result.text;
                this.hiddenValues[key] = result;
                if (data.onSelect) data.onSelect(result);
                dropdownContent.innerHTML = '';
            });

            dropdownItem.appendChild(dropdownText);
            dropdownContent.appendChild(dropdownItem);
        })

        if (dropdownContent.innerHTML) dropdown.classList.add('is-active');
    }

    _renderChipsField(key, data, controlDiv) {
        
        const labelKey = data.labelKey ?? 'label'
        const items = data.value ?? []
        const onClick = data.onClick ?? null
        const onDelete = data.onDelete ?? null

        const chipsContainer = document.createElement('div');
        chipsContainer.className = 'columns'
        let chips
        try {
            chips = new Chips({ container: chipsContainer, items, labelKey, onClick, onDelete })
        } catch (error) {
            console.error('error creating modal chips', error);
            throw new Error('error creating modal chips');
        }

        controlDiv.parentNode.replaceChild(chipsContainer, controlDiv);
        this.controlDivs[key] = chipsContainer;
        if (!this.hiddenValues[key]) this.hiddenValues[key] = {}
        this.hiddenValues[key].chips = chips;
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
  
    _debounce(fn, delay = 300) {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => fn(...args), delay);
        };
    }
    
}