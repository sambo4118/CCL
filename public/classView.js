import { loadClassDetails, Modal, showWarning } from './utils.js';

document.addEventListener('DOMContentLoaded', async () => {
    const classEditButton = document.getElementById('editClassButton');
    const classId = window.location.pathname.split('/').pop();

    try {
        let result = await loadClassDetails(classId);
        if (!result?.success) return; 

        const targetClass = result.targetClass; // { class: {...}, students: [...] }
        const modal = buildClassEditModal(targetClass);
        
        displayClassDetails(targetClass);
        classEditButton.addEventListener('click', () => modal.open());

        const studentListContainer = document.getElementById('studentListContainer');
        if (studentListContainer && targetClass.students) {
            setStudentList(targetClass.students, studentListContainer);
        }
        
    } catch (err) {
        showWarning(`failed to load class: ${classId}, press confirm to reload page`, {
            object: err, 
            onConfirm: () => window.location.reload() 
        });
    }
});

function displayClassDetails(targetClass) {
    const classInfo = targetClass.class;
    const { className, teacherName, studentCount, gradeLevel, classPhoto, classPhotoContainer } = getDetailElements();

    className.textContent = classInfo.name ?? 'Error Loading Class Name.';
    teacherName.textContent = classInfo.teacherName ?? console.log(`${classInfo.name ?? ('Class: ' + classInfo.id)} Missing Teacher Name`);
    studentCount.textContent = `student #: ${classInfo.studentCount ?? 0}`;
    
    if (gradeLevel && classInfo.gradeLevel) {
        gradeLevel.textContent = `grade ${classInfo.gradeLevel}`;
    }

    handleClassPhoto(classPhotoContainer, classPhoto, classInfo.classPhoto);
    return true;
}

function getDetailElements() {
    const className = document.getElementById('className');
    const teacherName  = document.getElementById('teacherName');
    const studentCount = document.getElementById('studentCount');
    const gradeLevel = document.getElementById('gradeLevel');
    const studentList = document.getElementById('studentList');
    const classPhoto = document.getElementById('classPhoto');
    const classPhotoContainer = document.getElementById('classPhotoContainer');
    return { className, teacherName, studentCount, gradeLevel, studentList, classPhoto, classPhotoContainer }
}

function handleClassPhoto(classPhotoContainer, classPhoto, image) {
    if (!classPhotoContainer) return console.warn('missing ClassPhotoContainer in current HTML context.'), false;

    if (!image) {
        classPhotoContainer.style.display = 'none';
        return false;
    }

    const uint8Array = new Uint8Array(image.data);
    const blob = new Blob([uint8Array], { type: 'image/jpeg' });
    const imageUrl = URL.createObjectURL(blob);
    
    classPhotoContainer.style.display = 'block';
    classPhoto.src = imageUrl;
}

function buildClassEditModal(targetClass) {
    const classInfo = targetClass.class;
    const initialStudents = targetClass.students ?? [];

    const modal = new Modal({ 
        title: classInfo.name, 
        mainColorBulmaVariable: 'primary', 
        successButtonText: 'Save', 
        onConfirm: ((modal) => updateClassInfo(targetClass, modal)) 
    });
    
    const config = {
        className: {
            label: 'class name:',
            type: 'text',
            placeholder: 'Input class name...',
            color: 'primary',
            value: classInfo.name
        },
        teacherName: {
            label: 'teacher name:',
            type: 'text',
            placeholder: 'Input teacher name...',
            color: 'primary',
            value: classInfo.teacherName
        },
        classPhoto: {
            label: 'class photo:',
            type: 'file',
            placeholder: 'Select class photo...',
            color: 'primary',
            value: classInfo.classPhoto ?? null
        },
        studentSearch: { 
            label: 'students:', 
            type: 'search', 
            placeholder: 'Search student name...', 
            color: 'primary', 
            minChars: 2, 
            resultsQuery: async (inputValue) => {
                const response = await fetch(`/api/students?search=${inputValue}`);
                const responseJson = await response.json();
                if (!response.ok) throw new Error(`Response is not ok, ${responseJson}`);
                return responseJson.map((student) => ({
                    text: student.name,
                    id: student.id,
                    classId: student.classId,
                    className: student.className
                }));
            }
        },
        studentChips: { 
            label: '', 
            type: 'chips', 
            value: initialStudents, 
            color: 'primary', 
            labelKey: 'name', 
            items: initialStudents 
        }
    };
    
    modal.addFields(config);
    customizeModalFields(modal, targetClass);
    return modal;
}

async function updateClassInfo(targetClass, modal) {
    const classInfo = targetClass.class;
    const classId = classInfo.id;

    const className = modal.getField('className')?.value;
    const teacherName = modal.getField('teacherName')?.value;
    const classPhoto = modal.getField('classPhoto')?.files?.[0];

    const chipsInstance = modal.hiddenValues.studentChips?.chips;
    const studentChips = chipsInstance ? chipsInstance.getItems() : [];
    const studentIds = studentChips.map(student => student.id);

    const formData = new FormData();
    if (className) formData.append('name', className);
    if (teacherName) formData.append('teacherName', teacherName);
    if (classPhoto) formData.append('image', classPhoto);
    formData.append('studentIds', JSON.stringify(studentIds));

    try {
        const response = await fetch(`/api/classes/${classId}`, {
            method: 'PUT',
            body: formData,
            credentials: 'include'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            showWarning('Failed to update Class', { 
                object: { error: errorData?.error || response.statusText } 
            });
            return false;
        }

        const result = await loadClassDetails(classId);
        if (!result?.success) return false;

        const updatedClass = result.targetClass; // fresh { class, students }
        displayClassDetails(updatedClass);

        const studentListContainer = document.getElementById('studentListContainer');
        if (studentListContainer && updatedClass.students) {
            setStudentList(updatedClass.students, studentListContainer);
        }

        return true;
    } catch (error) {
        showWarning(`Error saving class: ${error.message}`);
        return false;
    }
}

function setStudentList(students, studentListContainer) {
    studentListContainer.innerHTML = '';

    students.forEach(student => {
        const studentMediaBox = document.createElement('a');
        studentMediaBox.className = 'box has-background-primary';
        studentMediaBox.href = `/students/${student.id}`;

        const studentMedia = document.createElement('div');
        studentMedia.className = 'media';

        if (student.image) {
            const mediaLeft = document.createElement('figure');
            mediaLeft.className = 'media-left';
            
            const p = document.createElement('p');
            p.className = 'image is-64x64';
            
            const img = document.createElement('img');
            img.src = student.image;
            
            p.appendChild(img);
            mediaLeft.appendChild(p);
            studentMedia.appendChild(mediaLeft);
        }

        if (student.name || student.className) {
            const mediaContent = document.createElement('div');
            mediaContent.className = 'media-content';
            
            const content = document.createElement('div');
            content.className = 'content';
            
            const textContainer = document.createElement('p');
            
            if (student.name) {
                const nameNode = document.createElement('strong');
                nameNode.textContent = student.name;
                textContainer.appendChild(nameNode);
            }
            
            content.appendChild(textContainer);
            mediaContent.appendChild(content);
            studentMedia.appendChild(mediaContent);
        }

        if (student.rightField) {
            const mediaRight = document.createElement('div');
            mediaRight.className = 'media-right';
            mediaRight.textContent = student.rightField;
            studentMedia.appendChild(mediaRight);
        }

        studentMediaBox.appendChild(studentMedia);
        studentListContainer.appendChild(studentMediaBox);
    });
}

function customizeModalFields(modal, targetClass) {
    const searchDropdown = modal.controlDivs.studentSearch;
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
        const selectedStudent = modal.hiddenValues.studentSearch;
        if (!selectedStudent || !selectedStudent.id) return;

        const chips = modal.hiddenValues.studentChips?.chips;
        if (!chips) return;

        const item = { name: selectedStudent.text, id: selectedStudent.id };
        chips.addItem(item);
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