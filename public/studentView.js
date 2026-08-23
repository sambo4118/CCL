document.addEventListener('DOMContentLoaded', async () => {
    const studentId = window.location.pathname.split('/').pop()
    try {
        const responce = await fetch(`/api/students/${studentId}`)
        if (!responce.ok) {
            if (responce.status == 401) console.warn('Unauthorized Access')
            else console.error(responce)
            return;
        }
        const student = await responce.json();
        updateStudentInfoFields(student);
    } catch (error) {
        console.error('failed to get student info', error)
    }
});

function getStudentInfoFields() {
    
    const studentPhoto = document.getElementById('studentPhoto');
    const studentName = document.getElementById('studentName');
    const studentGrade = document.getElementById('studentGrade');
    const studentNotes = document.getElementById('studentNotes');
    const editStudentButton = document.getElementById('editStudentButton');
    const studentEnrollmentYear = document.getElementById('studentEnrollmentYear');
    const studentHomeroom = document.getElementById('studentHomeroom');
    const studentIdNumber = document.getElementById('studentIdNumber');
    const studentContact = document.getElementById('studentContact');
    return {studentPhoto, studentName, studentGrade, studentNotes, editStudentButton, studentEnrollmentYear, studentHomeroom, studentIdNumber, studentContact}
};

function updateStudentInfoFields(student) {
    const { studentPhoto, studentName, studentGrade, studentNotes, editStudentButton, studentEnrollmentYear, studentHomeroom, studentIdNumber, studentContact } = getStudentInfoFields();
    studentName.textContent = student.name ?? 'Error no student Name found';
    
    if (!student.photo) {
        studentPhoto.classList.add('is-hidden');
        studentPhoto.src = null
    }
    else studentPhoto.src = student.photo;

    if (!student.grade) studentGrade.classList.add('is-hidden');
    else studentGrade.textContent = student.grade;

    if (!student.notes) studentNotes.classList.add('is-hidden');
    else studentNotes.textContent = student.notes;

    if (!student.enrollmentdate) studentEnrollmentYear.classList.add('is-hidden');
    else studentEnrollmentYear.textContent = `Enrolled: ${student.enrollmentdate}`;

    if (!student.homeroom) studentHomeroom.classList.add('is-hidden');
    else studentHomeroom.textContent = `Homeroom: ${student.homeroom}`;

    if (!student.id) studentIdNumber.classList.add('is-hidden');
    else studentIdNumber.textContent = `ID: ${student.id}`;

    if (!student.contact) studentContact.classList.add('is-hidden');
    else studentContact.textContent = `Contact: ${student.contact}`;

}