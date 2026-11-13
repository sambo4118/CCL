import { Modal, showWarning } from '../utils.js';

export async function addNewStudentConfirm(m, onStudentCreated = null) {
    const name = (m.getField('name')?.value || '').trim();
    if (!name) {
        showWarning('Please enter a student name.');
        return false;
    }

    const chipsInstance = m.hiddenValues.classChips?.chips;
    const classChips = chipsInstance ? chipsInstance.getItems() : [];
    const selectedClassId = classChips[0]?.id ?? m.hiddenValues['classSearch']?.id ?? '';

    const formData = new FormData();
    formData.append('name', name);
    if (selectedClassId !== '') formData.append('classId', selectedClassId);
    formData.append('age', m.getField('age')?.value || '');
    formData.append('enrollmentdate', m.getField('enrollmentdate')?.value || '');
    formData.append('contact', m.getField('contact')?.value || '');
    formData.append('notes', m.getField('notes')?.value || '');

    const photoFile = m.getField('photo')?.files?.[0];
    if (photoFile) formData.append('photo', photoFile);

    try {
        const response = await fetch('/api/students', {
            method: 'POST',
            body: formData,
            credentials: 'include'
        });
        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(errData.error || 'Failed to create student');
        }
        const newStudent = await response.json();
        resetStudentModal(m);
        if (typeof onStudentCreated === 'function') await onStudentCreated(newStudent);
        return newStudent;
    } catch (error) {
        console.error('Error adding student:', error);
        showWarning(error.message || 'Failed to add student');
        return false;
    }
}

export function buildStudentModal(onStudentCreated = null) {
    const modal = new Modal({
        title: 'Add New Student',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Add Student',
        onConfirm: async (m) => await addNewStudentConfirm(m, onStudentCreated)
    });

    const config = {
        name: {
            label: 'Student Name:',
            type: 'text',
            placeholder: 'Full name...',
            color: 'primary'
        },
        age: {
            label: 'Age:',
            type: 'number',
            placeholder: 'Input student age...',
            color: 'primary'
        },
        enrollmentdate: {
            label: 'Enrollment Year/Date:',
            type: 'text',
            placeholder: 'e.g. 2026-09-01',
            color: 'primary'
        },
        contact: {
            label: 'Parent / Emergency Contact:',
            type: 'text',
            placeholder: 'Phone or email...',
            color: 'primary'
        },
        notes: {
            label: 'Notes / Accommodations:',
            type: 'textarea',
            placeholder: 'Student notes, accommodations, or bio...',
            color: 'primary'
        },
        photo: {
            label: 'Student Photo:',
            type: 'file',
            color: 'primary'
        },
        classSearch: {
            label: 'Class / Homeroom:',
            type: 'search',
            placeholder: 'Search class or teacher name...',
            color: 'primary',
            minChars: 1,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/classes?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const classes = await res.json();
                return classes.map(c => ({
                    id: c.id,
                    name: c.name,
                    teacherName: c.teacherName,
                    text: `${c.name}${c.teacherName ? ' — ' + c.teacherName : ''}`
                }));
            }
        },
        classChips: {
            label: '',
            type: 'chips',
            value: [],
            color: 'primary',
            labelKey: 'name',
            items: []
        },
        deleteStudentSearch: {
            label: 'Delete Existing Student:',
            type: 'search',
            placeholder: 'Search student name to delete...',
            color: 'danger',
            minChars: 1,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/students?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const students = await res.json();
                return students.map(s => ({
                    id: s.id,
                    name: s.name,
                    text: `${s.name}${s.className ? ' — ' + s.className : ''}`
                }));
            }
        }
    };

    modal.addFields(config);
    customizeStudentModalFields(modal);
    return modal;
}

function customizeStudentModalFields(modal) {
    const classDropdown = modal.controlDivs.classSearch;
    if (classDropdown) {
        const innerField = classDropdown.querySelector('.field');
        if (innerField) innerField.classList.add('mb-0');

        const rowWrapper = document.createElement('div');
        rowWrapper.className = 'columns is-mobile is-vcentered mb-0';

        const inputCol = document.createElement('div');
        inputCol.className = 'column mb-0 mr-0';

        const btnCol = document.createElement('div');
        btnCol.className = 'column is-narrow mr-0';

        const addButton = document.createElement('button');
        addButton.type = 'button';
        addButton.className = 'button is-info has-text-light';
        addButton.textContent = 'Add';

        const addSelectedClassChip = () => {
            const selectedClass = modal.hiddenValues.classSearch;
            if (!selectedClass || !selectedClass.id) return;

            const chips = modal.hiddenValues.classChips?.chips;
            if (!chips) return;

            chips.clear();
            chips.addItem({
                id: selectedClass.id,
                name: selectedClass.name || selectedClass.text
            });
            modal.hiddenValues.classSearch = {};

            const searchInput = modal.getField('classSearch');
            if (searchInput) searchInput.value = '';
        };

        addButton.addEventListener('click', addSelectedClassChip);

        btnCol.appendChild(addButton);
        classDropdown.parentNode.insertBefore(rowWrapper, classDropdown);
        inputCol.appendChild(classDropdown);
        rowWrapper.appendChild(inputCol);
        rowWrapper.appendChild(btnCol);
    }

    const deleteDropdown = modal.controlDivs.deleteStudentSearch;
    if (deleteDropdown) {
        const fieldContainer = deleteDropdown.closest('.field');
        if (fieldContainer && fieldContainer.parentNode) {
            const divider = document.createElement('hr');
            divider.className = 'my-4';
            fieldContainer.parentNode.insertBefore(divider, fieldContainer);
        }

        const innerField = deleteDropdown.querySelector('.field');
        if (innerField) innerField.classList.add('mb-0');

        const rowWrapper = document.createElement('div');
        rowWrapper.className = 'columns is-mobile is-vcentered mb-0';

        const inputCol = document.createElement('div');
        inputCol.className = 'column mb-0 mr-0';

        const btnCol = document.createElement('div');
        btnCol.className = 'column is-narrow mr-0';

        const deleteButton = document.createElement('button');
        deleteButton.type = 'button';
        deleteButton.className = 'button is-danger has-text-white';
        deleteButton.textContent = 'Delete';

        deleteButton.addEventListener('click', () => {
            const selectedStudent = modal.hiddenValues.deleteStudentSearch;
            if (!selectedStudent || !selectedStudent.id) {
                showWarning('Please search and select a student to delete first.');
                return;
            }

            const studentLabel = selectedStudent.name || selectedStudent.text;
            showWarning(`Are you sure you want to delete student <strong>${studentLabel}</strong>? This will also remove their checkout records.`, {
                onConfirm: async () => {
                    try {
                        const res = await fetch(`/api/students/${selectedStudent.id}`, {
                            method: 'DELETE',
                            credentials: 'include'
                        });
                        if (!res.ok) {
                            const errData = await res.json().catch(() => ({}));
                            throw new Error(errData.error || 'Failed to delete student');
                        }
                        modal.hiddenValues.deleteStudentSearch = {};
                        const deleteInput = modal.getField('deleteStudentSearch');
                        if (deleteInput) deleteInput.value = '';
                    } catch (error) {
                        console.error('Error deleting student:', error);
                        showWarning(error.message || 'Failed to delete student');
                    }
                }
            });
        });

        btnCol.appendChild(deleteButton);
        deleteDropdown.parentNode.insertBefore(rowWrapper, deleteDropdown);
        inputCol.appendChild(deleteDropdown);
        rowWrapper.appendChild(inputCol);
        rowWrapper.appendChild(btnCol);
    }
}

function resetStudentModal(modal) {
    Object.values(modal.inputs).forEach((input) => {
        if (input) input.value = '';
    });
    modal.hiddenValues.classSearch = {};
    modal.hiddenValues.deleteStudentSearch = {};
    modal.hiddenValues.classChips?.chips?.clear();
}

export function setupAddStudents() {
    const addStudentButton = document.getElementById('addStudentButton');
    if (!addStudentButton) return;

    const modal = buildStudentModal();
    addStudentButton.addEventListener('click', () => {
        resetStudentModal(modal);
        modal.open();
    });
}
