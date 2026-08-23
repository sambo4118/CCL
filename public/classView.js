import { loadClassDetails, Modal } from './utils.js';

document.addEventListener('DOMContentLoaded', async () => {
    const classEditButton = document.getElementById('editClassButton');
    const classId = window.location.pathname.split('/').pop();
    try {
        const targetClass = await loadClassDetails(classId);
        const modal = buildClassEditModal(targetClass);
        displayClassDetails(targetClass);
        classEditButton.addEventListener('click', () => modal.open());
        const studentListContainer = document.getElementById('studentListContainer')
        console.log(studentListContainer)
        setStudentList(targetClass.students, studentListContainer)
        
    } catch (err) {
        console.error(`failed to load class: ${classId}`, err)
    }
    
});

function displayClassDetails(targetClass) {
    const targetClassStudents = targetClass.students
    targetClass = targetClass.class
    const { className, teacherName, studentCount, gradeLevel, studentList, classPhoto, classPhotoContainer } = getDetailElements();
    className.textContent = targetClass.name ?? 'Error Loading Class Name.';
    teacherName.textContent = targetClass.teacherName ?? console.log(`${targetClass.className ??( 'Class: ' + targetClass.classId ) } Missing Teacher Name`);
    studentCount.textContent = `student #: ${targetClass.studentCount}` ?? console.log('missing student count' , targetClass.studentCount);
    if (gradeLevel) gradeLevel.textContent = `grade ${targetClass.gradeLevel}`;
    handleClassPhoto(classPhotoContainer, classPhoto, targetClass.classPhoto);
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
    if (!classPhotoContainer) return console.warn('missing ClassPhotoContainer in current HTML context.'), false

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
    targetClass = targetClass.class ?? targetClass;

    const modal = new Modal({ title:targetClass.name, mainColorBulmaVariable:'primary', successButtonText:'Save', onConfirm: ((modal) => updateClassInfo(targetClass, modal)) });
    
    const config = {
        className: {label:'class name:', type: 'text', placeholder: 'Input class name...', color: 'primary', value: targetClass.className},
        teacherName: {label:'teacher name:', type: 'text', placeholder: 'Input teacher name...', color: 'primary', value: targetClass.teacherName},
        classPhoto: { label: 'class photo:', type: 'file', placeholder: 'Select class photo...', color: 'primary', value: targetClass.image ?? null },
    };

    modal.addFields(config);

    return modal;
}

async function updateClassInfo(targetClass, modal) {
    const className = modal.getField('className').value;
    const teacherName = modal.getField('teacherName').value;
    const classPhoto = modal.getField('classPhoto').files[0];
    const classId = targetClass.id

    const formData = new FormData();
    
    if (className) formData.append('name', className);
    if (teacherName) formData.append('teacherName', teacherName);
    if (classPhoto) formData.append('image', classPhoto);

    const responce = await fetch(`/api/classes/${classId}`, {
        method: 'PUT',
        body: formData,
        credentials: 'include'
    });

    if (!responce.ok) {
        const errorData = await responce.json();
        console.error('request error', errorData ?? '');
    }

    targetClass = await loadClassDetails(classId);
    return displayClassDetails(targetClass);

}

function setStudentList(students, studentListContainer) {
    studentListContainer.innerHTML = ''

    console.log('running:', students, studentListContainer)

    students.forEach(student => {
        const studentMediaBox = document.createElement('a');
        studentMediaBox.className = 'box has-background-primary';
        studentMediaBox.href = `/students/${student.id}`

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

        studentMediaBox.appendChild(studentMedia)
        studentListContainer.appendChild(studentMediaBox);
    });
    
}