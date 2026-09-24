async function addNewStudentConfirm(m) {
    const selectedClass = m.hiddenValues['classSearch'];
    const formData = new FormData();
    formData.append('name', m.getField('name')?.value || '');
    if (selectedClass?.id) formData.append('classId', selectedClass.id);
    formData.append('age', m.getField('age')?.value || '');
    formData.append('enrollmentdate', m.getField('enrollmentdate')?.value || '');
    formData.append('contact', m.getField('contact')?.value || '');
    formData.append('notes', m.getField('notes')?.value || '');
    const photoFile = m.getField('photo')?.files?.[0];
    if (photoFile) formData.append('photo', photoFile);
    try {
        const response = await fetch('/api/students', {
            method: 'POST',
            body: formData
        });
        if (!response.ok) throw new Error('Failed to create student');
        const newStudent = await response.json();
        if (onStudentCreated) onStudentCreated(newStudent);
    } catch (error) {
        console.error('Error adding student:', error);
        showWarning('Failed to add student');
    }
}

async function buildStudentModal() {
    const modal = new Modal({
        title: 'Add New Student',
        mainColorBulmaVariable: 'primary',
        successButtonText: 'Add Student',
        onConfirm: addNewStudentConfirm(m)
    });

    const config = {
        name: { label: 'Student Name:', type: 'text', placeholder: 'Full name...', color: 'primary' },
        classSearch: {
            label: 'Class / Homeroom:',
            type: 'search',
            placeholder: 'Search class...',
            color: 'primary',
            minChars: 1,
            resultsQuery: async (query) => {
                const res = await fetch(`/api/classes?search=${encodeURIComponent(query)}`);
                if (!res.ok) return [];
                const classes = await res.json();
                return classes.map(c => ({ id: c.id, text: `${c.name} (${c.teacherName})` }));
            }
        },
        age: {
            label: 'Age / Grade:',
            type: 'text',
            placeholder: 'e.g. 5th Grade',
            color: 'primary'
        },
        enrollmentdate: {
            label: 'Enrollment Year/Date:',
            type: 'text',
            placeholder: 'e.g. 2026',
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
            color: 'primary'
        },
        photo: {
            label: 'Student Photo:',
            type: 'file',
            color: 'primary'
        }
    };
    modal.addFields(config);
    return modal;
}
