import { loadClassDetails, Modal } from './utils.js';

document.addEventListener('DOMContentLoaded', async () => {
    const classEditButton = document.getElementById('editClassButton');
    const classId = window.location.pathname.split('/').pop();
    try {
        const targetClass = await loadClassDetails(classId);
        const modal = buildClassEditModal(targetClass);
        displayClassDetails(targetClass);
        classEditButton.addEventListener('click', () => modal.open());
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
    handleClassPhoto(classPhotoContainer, classPhoto, targetClass.image)
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

    classPhotoContainer.style.display = 'block';
    classPhoto.src = image;
}

function buildClassEditModal(targetClass) {
    targetClass = targetClass.class ?? targetClass;

    const modal = new Modal({ title:targetClass.name, mainColorBulmaVariable:'info', successButtonText:'Save' });
    
    const config = {
        className: {label:'class name:', type: 'text', placeholder: 'Input class name...', color: 'info'},
        teacherName: {label:'teacher name:', type: 'text', placeholder: 'Input teacher name...', color: 'info'},
        classPhoto: {label:'class photo:', type: 'file', placeholder: 'Select class photo...', color: 'info'}
    };

    modal.addFields(config);

    return modal;
}