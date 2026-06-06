function loadStudents() {
    const modal = document.getElementById('importStudentsModal');
    if (!modal) return console.error('Import Students modal not found');
    modal.classList.add('is-active');

    const closemodal = () => {
        modal.classList.remove('is-active');
    };

    const closeButton = document.getElementById('closeStudentImportModal');
    closeButton.addEventListener('click', closemodal);

    const cancelButton = document.getElementById('cancelStudentImport');
    cancelButton.addEventListener('click', closemodal);

    const fileInput = document.getElementById('studentFileInput');
    fileInput.addEventListener('change', async () => {
        const file = fileInput.files?.[0];
        handleFileUpload(file);
    });

    const confirmButton = document.getElementById('confirmStudentImport');
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
            const result = await uploadStudents(formData);
            console.log('Students uploaded successfully:', result);
            closemodal();
        } catch (error) {
            console.error('Error uploading students:', error);
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

    const fileNameField = document.getElementById('studentFileName');
    fileNameField.textContent = `${file.name}`;

    return formData.append('file', file);
}

async function uploadStudents(formData) {
    const res = await fetch('/api/students/import', {
        method: 'POST',
        body: formData
    });
    if (!res.ok) throw new Error(`Failed to upload students: ${res.status}`);
    return res.json();
}

export function setupImportStudents() {
    const loadStudentsButton = document.getElementById('importStudentsButton');
    loadStudentsButton?.addEventListener('click', () => {
        loadStudents();
    });
}
