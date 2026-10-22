import { Modal, showWarning, showAuthWarning } from './utils.js';
import { Viewlist } from './listBuilderClass.js';

document.addEventListener('DOMContentLoaded', async () => {
    const studentId = window.location.pathname.split('/').pop();
    try {
        const student = await loadStudentDetails(studentId);
        if (!student) return;

        updateStudentInfoFields(student);

        const modal = buildStudentEditModal(student);
        const { editStudentButton } = getStudentInfoFields();
        editStudentButton?.addEventListener('click', () => modal.open());

        const checkoutList = buildStudentCheckoutList();
        const checkouts = await loadStudentCheckouts(studentId);
        checkoutList.render(checkouts);
    } catch (error) {
        console.error('failed to get student info', error);
    }
});

async function loadStudentDetails(studentId) {
    const responce = await fetch(`/api/students/${studentId}`);
    if (!responce.ok) {
        if (responce.status === 401) {
            showAuthWarning('You are not authorized to view students, please sign in');
        } else {
            showWarning('Failed to load student details', { object: responce });
        }
        return null;
    }
    return await responce.json();
}

async function loadStudentCheckouts(studentId) {
    try {
        const response = await fetch(`/api/checkouts/student/${studentId}`);
        if (!response.ok) return [];
        return await response.json();
    } catch (error) {
        console.error('Failed to fetch student checkouts:', error);
        return [];
    }
}

function buildStudentEditModal(student) {
    const initialClassChips = student.classId
        ? [{ id: student.classId, name: student.className || `Class #${student.classId}` }]
        : [];

    const modal = new Modal({
        title: student.name || 'Edit Student',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Save',
        onConfirm: (m) => updateStudentInfo(student, m)
    });

    const config = {
        studentName: {
            label: 'Student Name:',
            type: 'text',
            placeholder: 'Input student name...',
            color: 'primary',
            value: student.name ?? ''
        },
        age: {
            label: 'Age:',
            type: 'number',
            placeholder: 'Input student age...',
            color: 'primary',
            value: student.age ?? ''
        },
        enrollmentdate: {
            label: 'Enrollment Date:',
            type: 'text',
            placeholder: 'e.g. 2026-09-01',
            color: 'primary',
            value: student.enrollmentdate ?? ''
        },
        contact: {
            label: 'Parent / Contact Info:',
            type: 'text',
            placeholder: 'Input phone number or email...',
            color: 'primary',
            value: student.contact ?? ''
        },
        notes: {
            label: 'Notes:',
            type: 'text',
            placeholder: 'Student notes, accommodations, or bio...',
            color: 'primary',
            value: student.notes ?? ''
        },
        studentPhoto: {
            label: 'Student Photo:',
            type: 'file',
            placeholder: 'Select student photo...',
            color: 'primary'
        },
        classSearch: {
            label: 'Class:',
            type: 'search',
            placeholder: 'Search class or teacher name...',
            color: 'primary',
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
        },
        classChips: {
            label: '',
            type: 'chips',
            value: initialClassChips,
            color: 'primary',
            labelKey: 'name',
            items: initialClassChips
        }
    };

    modal.addFields(config);
    customizeStudentModalFields(modal);
    return modal;
}

function customizeStudentModalFields(modal) {
    const searchDropdown = modal.controlDivs.classSearch;
    if (!searchDropdown) return;

    const innerField = searchDropdown.querySelector('.field');
    if (innerField) {
        innerField.classList.add('mb-0');
    }

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
        const selectedClass = modal.hiddenValues.classSearch;
        if (!selectedClass || !selectedClass.id) return;

        const chips = modal.hiddenValues.classChips?.chips;
        if (!chips) return;

        // A student belongs to a single class, so replace existing chip
        chips.clear();
        chips.addItem({
            id: selectedClass.id,
            name: selectedClass.name || selectedClass.text
        });
        modal.hiddenValues.classSearch = {};

        const searchInput = modal.getField('classSearch');
        if (searchInput) searchInput.value = '';
    });

    btnCol.appendChild(actionButton);

    searchDropdown.parentNode.insertBefore(rowWrapper, searchDropdown);
    inputCol.appendChild(searchDropdown);
    rowWrapper.appendChild(inputCol);
    rowWrapper.appendChild(btnCol);
}

async function updateStudentInfo(student, modal) {
    const studentId = student.id;

    const name = modal.getField('studentName')?.value;
    const age = modal.getField('age')?.value;
    const enrollmentdate = modal.getField('enrollmentdate')?.value;
    const contact = modal.getField('contact')?.value;
    const notes = modal.getField('notes')?.value;
    const photoFile = modal.getField('studentPhoto')?.files?.[0];

    const chipsInstance = modal.hiddenValues.classChips?.chips;
    const classChips = chipsInstance ? chipsInstance.getItems() : [];
    const selectedClassId = classChips.length > 0 ? classChips[0].id : '';

    const formData = new FormData();
    if (name !== undefined) formData.append('name', name);
    if (age !== undefined) formData.append('age', age);
    if (enrollmentdate !== undefined) formData.append('enrollmentdate', enrollmentdate);
    if (contact !== undefined) formData.append('contact', contact);
    if (notes !== undefined) formData.append('notes', notes);
    formData.append('classId', selectedClassId);
    if (photoFile) formData.append('photo', photoFile);

    try {
        const response = await fetch(`/api/students/${studentId}`, {
            method: 'PUT',
            body: formData,
            credentials: 'include'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            showWarning('Failed to update Student', {
                object: { error: errorData?.error || response.statusText }
            });
            return false;
        }

        const updatedStudent = await loadStudentDetails(studentId);
        if (updatedStudent) {
            Object.assign(student, updatedStudent);
            updateStudentInfoFields(updatedStudent);
        }
        return true;
    } catch (error) {
        showWarning(`Error saving student: ${error.message}`);
        return false;
    }
}

function buildStudentCheckoutList() {
    const actionConfig = {
        size: 1,
        buttonColor: 'danger',
        iconClass: 'fas fa-check fa-lg',
        color: (checkout) => checkout.returnDate ? 'success' : (new Date(checkout.checkoutDate) < new Date() ? 'danger' : 'warning')
    };

    const columnsConfig = {
        left: {
            size: 12,
            align: 'left',
            lines: [
                {
                    type: 'title',
                    text: (checkout) => checkout.bookTitle || 'Unknown Book',
                    href: (checkout) => `/books/${checkout.bookId}`
                },
                {
                    type: 'subtitle',
                    text: (checkout) => checkout.bookAuthor || 'Unknown Author'
                },
                {
                    type: 'subtitle',
                    text: (checkout) => `Checked out: ${new Date(checkout.checkoutDate).toLocaleDateString()}`
                },
                {
                    type: 'subtitle',
                    text: (checkout) => {
                        if (checkout.returnDate) {
                            return `Returned: ${new Date(checkout.returnDate).toLocaleDateString()}`;
                        }
                        const due = new Date(checkout.checkoutDate);
                        due.setDate(due.getDate() + (checkout.duration ?? 14));
                        const today = new Date();
                        const diffDays = Math.round((due.setHours(0, 0, 0, 0) - today.setHours(0, 0, 0, 0)) / (1000 * 60 * 60 * 24));

                        let dueLabel = `Due in: ${diffDays} days`;
                        if (diffDays === 0) dueLabel = 'Due today';
                        if (diffDays < 0) dueLabel = `Overdue by: ${Math.abs(diffDays)} days`;
                        return dueLabel;
                    }
                }
            ]
        }
    };

    const list = new Viewlist({
        container: '#checkoutsContainer',
        baseColor: 'primary',
        emptyMessage: 'No checkouts for this student.',
        imageConfig: {
            srcKey: 'bookCoverUrl',
            placeholder: '/images/128x128.png',
            alt: (checkout) => `${checkout.bookTitle ?? 'Book'} cover`
        },
        actionConfig,
        columnsConfig
    });

    list.actionConfig = actionConfig;
    return list;
}

function getStudentInfoFields() {
    const studentPhoto = document.getElementById('studentPhoto');
    const studentName = document.getElementById('studentName');
    const studentGrade = document.getElementById('studentGrade');
    const studentNotes = document.getElementById('studentNotes');
    const editStudentButton = document.getElementById('editStudentButton');
    const studentAge = document.getElementById('studentAge');
    const studentEnrollmentYear = document.getElementById('studentEnrollmentYear');
    const studentHomeroom = document.getElementById('studentHomeroom');
    const studentIdNumber = document.getElementById('studentIdNumber');
    const studentContact = document.getElementById('studentContact');
    return {
        studentPhoto,
        studentName,
        studentGrade,
        studentNotes,
        editStudentButton,
        studentAge,
        studentEnrollmentYear,
        studentHomeroom,
        studentIdNumber,
        studentContact
    };
}

function updateStudentInfoFields(student) {
    const {
        studentPhoto,
        studentName,
        studentGrade,
        studentNotes,
        studentAge,
        studentEnrollmentYear,
        studentHomeroom,
        studentIdNumber,
        studentContact
    } = getStudentInfoFields();

    studentName.textContent = student.name ?? 'Error no student Name found';
    
    if (!student.photo) {
        studentPhoto.classList.add('is-hidden');
        studentPhoto.src = '';
    } else {
        studentPhoto.classList.remove('is-hidden');
        studentPhoto.src = student.photo;
    }

    if (!student.grade) {
        studentGrade.classList.add('is-hidden');
    } else {
        studentGrade.classList.remove('is-hidden');
        studentGrade.textContent = student.grade;
    }

    if (!student.notes) {
        studentNotes.classList.add('is-hidden');
    } else {
        studentNotes.classList.remove('is-hidden');
        studentNotes.textContent = student.notes;
    }

    if (studentAge) {
        if (student.age === null || student.age === undefined || student.age === '') {
            studentAge.classList.add('is-hidden');
        } else {
            studentAge.classList.remove('is-hidden');
            studentAge.textContent = `Age: ${student.age}`;
        }
    }

    if (!student.enrollmentdate) {
        studentEnrollmentYear.classList.add('is-hidden');
    } else {
        studentEnrollmentYear.classList.remove('is-hidden');
        studentEnrollmentYear.textContent = `Enrolled: ${student.enrollmentdate}`;
    }

    if (!student.homeroom) {
        studentHomeroom.classList.add('is-hidden');
    } else {
        studentHomeroom.classList.remove('is-hidden');
        studentHomeroom.textContent = `Homeroom: ${student.homeroom}`;
    }

    if (!student.id) {
        studentIdNumber.classList.add('is-hidden');
    } else {
        studentIdNumber.classList.remove('is-hidden');
        studentIdNumber.textContent = `ID: ${student.id}`;
    }

    if (!student.contact) {
        studentContact.classList.add('is-hidden');
    } else {
        studentContact.classList.remove('is-hidden');
        studentContact.textContent = `Contact: ${student.contact}`;
    }
}
