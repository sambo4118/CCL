export function showWarning(message, {object = null, onConfirm = null, onCancel = null } = {}) {
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

    if (!onConfirm) onConfirm = () => closeModal();
    if (!onCancel) onCancel = () => closeModal();

    warningModal.querySelectorAll('.modal-background, .modal-close, .modal-card-head .delete, .modal-card-foot .button').forEach((close) => {
        close.addEventListener('click', closeModal);
    }) 

    const modalFooter = warningModal.querySelector('.modal-card-foot')

    const confirmButton = modalFooter.querySelector('.conf-btn');
    confirmButton.addEventListener('click', () => {
        onConfirm();
        closeModal();
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
        this.mainColorBulmaVariable = mainColorBulmaVariable;

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
        this.successButton.addEventListener('click', async () => {
            if (onConfirm) {
                const shouldClose = await onConfirm(this);
                if (shouldClose === false) return;
            }
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
                    break;
                case 'responsive':
                    this._renderResponsiveField(key, data, controlDiv);
                    break;
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
    
    // types = text, password, email, tel, textarea: no special data
    _renderStandardField(key, data, controlDiv) {
        const isTextarea = data.type === 'textarea';
        const input = document.createElement(isTextarea ? 'textarea' : 'input');
        input.className = `${isTextarea ? 'textarea' : 'input'} is-${data.color ?? this.mainColorBulmaVariable}`;
        if (!isTextarea) input.type = data.type ?? 'text';
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
                    <input class="input is-${data.color ?? this.mainColorBulmaVariable}" type="text" placeholder="${data.placeholder ?? ''}">
                </div>
            </div>
            <div class="dropdown-menu" role="menu">
                <div class="dropdown-content">
                
                </div>
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

        if (data.value) input.value = data.value;
        if (data.hiddenValue) this.hiddenValues[key] = data.hiddenValue;
        if (data.disabled) input.disabled = true;

        input.addEventListener('input', (event) => {
            const value = event.target.value;
            const minChars = data.minChars ?? 3

            if (value.length < minChars) {
                if (value.length == 0) dropdownContent.innerHTML = ''
                return false;
            };

            debouncedSearch(value);
           
        })

        input.addEventListener('blur', () => {
            setTimeout(() => {
                dropdown.classList.remove('is-active');
            }, 150);
        });

        input.addEventListener('focus', () => {
            if (dropdownContent.children.length > 0) dropdown.classList.add('is-active');
        });

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

            dropdownText.addEventListener('mousedown', (event) => {
                event.preventDefault();
                input.value = result.text;
                this.hiddenValues[key] = result;
                if (data.onSelect) data.onSelect(result);
                dropdown.classList.remove('is-active');
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

    // type = responsive
    // takes responseFunction, and minChars args
    // responseFunction is a function that takes input.value and is run on debounced keystrokes.
    _renderResponsiveField(key, data, controlDiv) {
        const input = document.createElement('input');
        input.className = `input is-${data.color ?? this.mainColorBulmaVariable}`;
        input.type = data.type ?? 'text';
        input.placeholder = data.placeholder ?? '';
        if (data.value) input.value = data.value;

        const debouncedResponse = this._debounce(async (inputValue) => {
            try {
                await data.responseFunction(inputValue);
            } catch (error) {
                console.error(`Error in responsive field for key ${key}:`, error);
            }
        }, data.delay ?? 300);

        input.addEventListener('input', (e) => {
            debouncedResponse(e.target.value);
        });

        controlDiv.appendChild(input);
        this.inputs[key] = input; // Keep reference for retrieval
        this.controlDivs[key] = controlDiv;
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

export async function addNewCheckoutConfirm(m, onCheckoutCreated = null) {
    const student = m.hiddenValues['studentSearch'];
    const book = m.hiddenValues['bookSearch'];
    const duration = parseInt(m.getField('duration')?.value || '14', 10);
    if (!student?.id || !book?.id) {
        showWarning('Please select both a student and a book.');
        return false;
    }
    try {
        // Verify the book does not already have an active checkout
        const activeRes = await fetch(`/api/checkouts/book/${book.id}/recent`);
        if (activeRes.ok) {
            const activeCheckout = await activeRes.json();
            if (activeCheckout && !activeCheckout.returnDate) {
                showWarning(`"${book.text || 'This book'}" is already checked out to ${activeCheckout.studentName || 'another student'}.`);
                return false;
            }
        }

        const response = await fetch('/api/checkouts', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                studentId: student.id,
                bookId: book.id,
                duration
            })
        });
        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || 'Failed to create checkout');
        }
        const newCheckout = await response.json();
        if (typeof onCheckoutCreated === 'function') await onCheckoutCreated(newCheckout);
        return newCheckout;
    } catch (error) {
        console.error('Error creating checkout:', error);
        showWarning(error.message || 'Failed to create checkout');
        return false;
    }
}

export function createCheckoutModal(fromType = 'none', presetData = null, onCheckoutCreated = null) {
    const normalizedType = (fromType ?? 'none').trim().toLowerCase();
    if (!['book', 'class', 'student', 'author', 'none'].includes(normalizedType)) {
        throw new Error('Invalid fromType.');
    }

    // Support passing either (fromType, entity, onConfirm) or (fromType, { book, student, onConfirm })
    let bookPreset = null;
    let studentPreset = null;
    let onConfirmCallback = onCheckoutCreated;

    if (typeof presetData === 'function') {
        onConfirmCallback = presetData;
    } else if (presetData && typeof presetData === 'object') {
        if (presetData.book || presetData.student || presetData.onConfirm) {
            bookPreset = presetData.book ?? null;
            studentPreset = presetData.student ?? null;
            onConfirmCallback = presetData.onConfirm ?? onCheckoutCreated;
        } else if (normalizedType === 'book') {
            bookPreset = presetData;
        } else if (normalizedType === 'student') {
            studentPreset = presetData;
        }
    }

    const studentText = studentPreset
        ? `${studentPreset.name}${studentPreset.className || studentPreset.grade ? ' — ' + (studentPreset.className || studentPreset.grade) : ''}`
        : '';
    const bookText = bookPreset
        ? `${bookPreset.title}${bookPreset.localNumber ? ` (${bookPreset.localNumber})` : ''}`
        : '';

    const modal = new Modal({
        title: 'New Checkout',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Check Out',
        onConfirm: async (m) => await addNewCheckoutConfirm(m, onConfirmCallback)
    });
    
    const config = {
        studentSearch: {
            label: 'Student:',
            type: 'search',
            placeholder: 'Search student name...',
            color: 'primary',
            minChars: 2,
            value: studentText,
            hiddenValue: studentPreset ? { id: studentPreset.id, text: studentText } : null,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/students?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const students = await res.json();
                return students.map(s => ({ id: s.id, text: `${s.name}${s.className ? ' — ' + s.className : ''}` }));
            }
        },
        bookSearch: {
            label: 'Book:',
            type: 'search',
            placeholder: 'Search book title or local #...',
            color: 'primary',
            minChars: 2,
            value: bookText,
            hiddenValue: bookPreset ? { id: bookPreset.id, text: bookText } : null,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/books/search?q=${encodeURIComponent(query)}&available=true`);
                if (!res.ok) return [];
                const books = await res.json();
                return books.map(b => ({ id: b.id, text: `${b.title} (${b.localNumber})` }));
            }
        },
        duration: {
            label: 'Loan Duration (Days):',
            type: 'text',
            value: '14',
            placeholder: '14',
            color: 'primary'
        }
    };
    modal.addFields(config);
    return modal;
}

export function createReturnModal(checkout, onReturned = null) {
    const modal = new Modal({
        title: 'Return Checkout?',
        mainColorBulmaVariable: 'warning',
        successButtonText: 'Return',
        onConfirm: async (_) => {
            const result = await returnCheckout(checkout);
            if (result && typeof onReturned === 'function') {
                await onReturned(result);
            }
            return result;
        }
    });
    return modal;
}

export async function returnCheckout(checkout) {
    const responce = await fetch(`/api/checkouts/${checkout.id}/return`, { method: 'POST' });
    if (!responce.ok) {
        console.error('Error returning checkout:', responce);
        showWarning('Error returning checkout', { object: responce });
        return false;
    }
    const data = await responce.json();
    return data;
}

export async function updateCheckoutConfirm(checkout, m, onCheckoutUpdated = null) {
    const student = m.hiddenValues['studentSearch'];
    const book = m.hiddenValues['bookSearch'];
    const studentId = student?.id ?? checkout.studentId;
    const bookId = book?.id ?? checkout.bookId;
    const duration = parseInt(m.getField('duration')?.value || String(checkout.duration ?? 14), 10);
    const checkoutDate = m.getField('checkoutDate')?.value?.trim() || checkout.checkoutDate;
    const returnDateRaw = m.getField('returnDate')?.value?.trim() ?? '';
    const returnDate = returnDateRaw === '' ? null : returnDateRaw;

    if (!studentId || !bookId) {
        showWarning('Please select both a student and a book.');
        return false;
    }

    try {
        const response = await fetch(`/api/checkouts/${checkout.id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                studentId,
                bookId,
                duration,
                checkoutDate,
                returnDate
            })
        });
        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || 'Failed to update checkout');
        }
        const updatedCheckout = await response.json();
        if (typeof onCheckoutUpdated === 'function') await onCheckoutUpdated(updatedCheckout);
        return updatedCheckout;
    } catch (error) {
        console.error('Error updating checkout:', error);
        showWarning(error.message || 'Failed to update checkout');
        return false;
    }
}

export function createEditCheckoutModal(checkout, onCheckoutUpdated = null) {
    const studentText = checkout.studentName
        ? `${checkout.studentName}${checkout.className ? ' — ' + checkout.className : ''}`
        : '';
    const bookText = checkout.bookTitle
        ? `${checkout.bookTitle}${checkout.localNumber ? ` (${checkout.localNumber})` : ''}`
        : '';

    const modal = new Modal({
        title: `Edit Checkout${checkout.bookTitle ? ` — ${checkout.bookTitle}` : ''}`,
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Save',
        onConfirm: async (m) => await updateCheckoutConfirm(checkout, m, onCheckoutUpdated)
    });

    const config = {
        studentSearch: {
            label: 'Student:',
            type: 'search',
            placeholder: 'Search student name...',
            color: 'primary',
            minChars: 2,
            value: studentText,
            hiddenValue: checkout.studentId ? { id: checkout.studentId, text: studentText } : null,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/students?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const students = await res.json();
                return students.map(s => ({ id: s.id, text: `${s.name}${s.className ? ' — ' + s.className : ''}` }));
            }
        },
        bookSearch: {
            label: 'Book:',
            type: 'search',
            placeholder: 'Search book title or local #...',
            color: 'primary',
            minChars: 2,
            value: bookText,
            hiddenValue: checkout.bookId ? { id: checkout.bookId, text: bookText } : null,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/books/search?q=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const books = await res.json();
                return books.map(b => ({ id: b.id, text: `${b.title} (${b.localNumber})` }));
            }
        },
        duration: {
            label: 'Loan Duration (Days):',
            type: 'text',
            value: String(checkout.duration ?? 14),
            placeholder: '14',
            color: 'primary'
        },
        checkoutDate: {
            label: 'Checkout Date:',
            type: 'text',
            value: checkout.checkoutDate ?? '',
            placeholder: 'YYYY-MM-DD HH:MM:SS',
            color: 'primary'
        },
        returnDate: {
            label: 'Return Date (blank if still checked out):',
            type: 'text',
            value: checkout.returnDate ?? '',
            placeholder: 'Leave blank if active...',
            color: 'primary'
        }
    };
    modal.addFields(config);
    return modal;
}


