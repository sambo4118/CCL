import { Modal, showWarning } from '../utils.js';

export async function addNewClassConfirm(m, onClassCreated = null) {
    const className = (m.getField('className')?.value || '').trim();
    if (!className) {
        showWarning('Please enter a class name.');
        return false;
    }

    const teacherName = (m.getField('teacherName')?.value || '').trim();
    const classPhoto = m.getField('classPhoto')?.files?.[0];

    const chipsInstance = m.hiddenValues.studentChips?.chips;
    const studentChips = chipsInstance ? chipsInstance.getItems() : [];
    const studentIds = studentChips.map(student => student.id);

    const pendingStudent = m.hiddenValues.studentSearch;
    if (pendingStudent?.id && !studentIds.includes(pendingStudent.id)) {
        studentIds.push(pendingStudent.id);
    }

    const formData = new FormData();
    formData.append('name', className);
    formData.append('teacherName', teacherName);
    if (classPhoto) formData.append('image', classPhoto);
    formData.append('studentIds', JSON.stringify(studentIds));

    try {
        const response = await fetch('/api/classes', {
            method: 'POST',
            body: formData,
            credentials: 'include'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.error || 'Failed to create class');
        }

        const newClass = await response.json();
        resetClassModal(m);
        if (typeof onClassCreated === 'function') await onClassCreated(newClass);
        return newClass;
    } catch (error) {
        console.error('Error creating class:', error);
        showWarning(error.message || 'Failed to create class');
        return false;
    }
}

export function buildClassModal(onClassCreated = null) {
    const modal = new Modal({
        title: 'Add New Class',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Add Class',
        onConfirm: async (m) => await addNewClassConfirm(m, onClassCreated)
    });

    const config = {
        className: {
            label: 'Class Name:',
            type: 'text',
            placeholder: 'e.g. Grade 1...',
            color: 'primary'
        },
        teacherName: {
            label: 'Teacher Name:',
            type: 'text',
            placeholder: 'Input teacher name...',
            color: 'primary'
        },
        classPhoto: {
            label: 'Class Photo:',
            type: 'file',
            placeholder: 'Select class photo...',
            color: 'primary'
        },
        studentSearch: {
            label: 'Students:',
            type: 'search',
            placeholder: 'Search student name to add...',
            color: 'primary',
            minChars: 1,
            resultsQuery: async (inputValue) => {
                const response = await fetch(`/api/students?search=${encodeURIComponent(inputValue)}`);
                if (!response.ok) return [];
                const studentsList = await response.json();
                return studentsList.map((student) => ({
                    id: student.id,
                    name: student.name,
                    classId: student.classId,
                    className: student.className,
                    text: `${student.name}${student.className ? ' — ' + student.className : ''}`
                }));
            }
        },
        studentChips: {
            label: '',
            type: 'chips',
            value: [],
            color: 'primary',
            labelKey: 'name',
            items: []
        },
        deleteClassSearch: {
            label: 'Delete Existing Class:',
            type: 'search',
            placeholder: 'Search class or teacher name to delete...',
            color: 'danger',
            minChars: 1,
            resultsQuery: async (inputValue) => {
                const response = await fetch(`/api/classes?search=${encodeURIComponent(inputValue)}`);
                if (!response.ok) return [];
                const classesList = await response.json();
                return classesList.map((cls) => ({
                    id: cls.id,
                    name: cls.name,
                    teacherName: cls.teacherName,
                    text: `${cls.name}${cls.teacherName ? ' — ' + cls.teacherName : ''}`
                }));
            }
        }
    };

    modal.addFields(config);
    customizeClassModalFields(modal);
    return modal;
}

function customizeClassModalFields(modal) {
    const searchDropdown = modal.controlDivs.studentSearch;
    if (searchDropdown) {
        const innerField = searchDropdown.querySelector('.field');
        if (innerField) innerField.classList.add('mb-0');

        const rowWrapper = document.createElement('div');
        rowWrapper.className = 'columns is-mobile is-vcentered mb-0';

        const inputCol = document.createElement('div');
        inputCol.className = 'column mb-0 mr-0';

        const btnCol = document.createElement('div');
        btnCol.className = 'column is-narrow mr-0';

        const actionButton = document.createElement('button');
        actionButton.type = 'button';
        actionButton.className = 'button is-info has-text-light';
        actionButton.textContent = 'Add';

        actionButton.addEventListener('click', () => {
            const selectedStudent = modal.hiddenValues.studentSearch;
            if (!selectedStudent || !selectedStudent.id) return;

            const chips = modal.hiddenValues.studentChips?.chips;
            if (!chips) return;

            const alreadyAdded = chips.getItems().some(s => s.id === selectedStudent.id);
            if (!alreadyAdded) {
                chips.addItem({
                    id: selectedStudent.id,
                    name: selectedStudent.name || selectedStudent.text
                });
            }
            modal.hiddenValues.studentSearch = {};

            const searchInput = modal.getField('studentSearch');
            if (searchInput) searchInput.value = '';
        });

        btnCol.appendChild(actionButton);
        searchDropdown.parentNode.insertBefore(rowWrapper, searchDropdown);
        inputCol.appendChild(searchDropdown);
        rowWrapper.appendChild(inputCol);
        rowWrapper.appendChild(btnCol);
    }

    const deleteDropdown = modal.controlDivs.deleteClassSearch;
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
            const selectedClass = modal.hiddenValues.deleteClassSearch;
            if (!selectedClass || !selectedClass.id) {
                showWarning('Please search and select a class to delete first.');
                return;
            }

            const classLabel = selectedClass.name || selectedClass.text;
            showWarning(`Are you sure you want to delete class <strong>${classLabel}</strong>? Students in this class will be unassigned.`, {
                onConfirm: async () => {
                    try {
                        const res = await fetch(`/api/classes/${selectedClass.id}`, {
                            method: 'DELETE',
                            credentials: 'include'
                        });
                        if (!res.ok) {
                            const errData = await res.json().catch(() => ({}));
                            throw new Error(errData.error || 'Failed to delete class');
                        }
                        modal.hiddenValues.deleteClassSearch = {};
                        const deleteInput = modal.getField('deleteClassSearch');
                        if (deleteInput) deleteInput.value = '';
                    } catch (error) {
                        console.error('Error deleting class:', error);
                        showWarning(error.message || 'Failed to delete class');
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

function resetClassModal(modal) {
    Object.values(modal.inputs).forEach((input) => {
        if (input) input.value = '';
    });
    modal.hiddenValues.studentSearch = {};
    modal.hiddenValues.deleteClassSearch = {};
    modal.hiddenValues.studentChips?.chips?.clear();
}

export function setupAddClasses() {
    const addClassButton = document.getElementById('addClassButton');
    if (!addClassButton) return;

    const modal = buildClassModal();
    addClassButton.addEventListener('click', () => {
        resetClassModal(modal);
        modal.open();
    });
}
