import { showWarning } from '../utils.js'

function loadBooks() {
    const modal = document.getElementById('importBooksModal');
    if (!modal) return console.error('Import Books modal not found');
    modal.classList.add('is-active');
    
    const closemodal = () => {
        modal.classList.remove('is-active');
    };
    
    const closeButton = document.getElementById('closeImportModal');
    closeButton.addEventListener('click', closemodal);

    const cancelButton = document.getElementById('cancelImport');
    cancelButton.addEventListener('click', closemodal);

    const fileInput = document.getElementById('bookFileInput');
    fileInput.addEventListener('change', async () => {
        const file = fileInput.files?.[0];
        handleFileUpload(file);
    });

    const confirmButton = document.getElementById('confirmImport');
    confirmButton.addEventListener('click', async () => {
        const file = fileInput.files?.[0];
        if (!file) {
            confirmButton.classList.add('is-loading');
            new Promise(resolve => setTimeout(resolve, 250)).then(() => {
                confirmButton.classList.remove('is-loading');
                confirmButton.classList.add('is-danger');
                confirmButton.textContent = 'No file selected';
                new Promise(resolve => setTimeout(resolve, 1000)).then(() => {
                    confirmButton.classList.remove('is-danger');
                    confirmButton.textContent = 'Confirm';
                });
            });
            return;
        }
        const formData = new FormData();
        formData.append('file', file);
        confirmButton.classList.add('is-loading');
        try {
            const result = await uploadBooks(formData);
            console.log('Books uploaded successfully:', result);
            closemodal();
        } catch (error) {
            console.error('Error uploading books:', error);
            confirmButton.classList.add('is-danger');
            confirmButton.textContent = 'Upload failed';
            new Promise(resolve => setTimeout(resolve, 2000)).then(() => {
                confirmButton.classList.remove('is-danger');
                confirmButton.textContent = 'Confirm';
            });
        } finally {
            confirmButton.classList.remove('is-loading');
        }
    });
}

function handleFileUpload(file) {
    const formData = new FormData();

    const fileNameField = document.getElementById('fileName');
    fileNameField.textContent = `${file.name}`;

    return formData.append('file', file);
}

async function uploadBooks(formData) {
    const res = await fetch('/api/books/import', {
        method: 'POST',
        body: formData
    });
    if (!res.ok) throw new Error(`Failed to upload books: ${res.status}`);
    return res.json();
}

document.addEventListener('DOMContentLoaded', () => {
    const loadBooksButton = document.getElementById('importBooksButton');
    loadBooksButton?.addEventListener('click', () => {
        showWarning('This will overwrite your current library. Are you sure you want to continue?', loadBooks)
    });
});

