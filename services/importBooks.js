import * as XLSX from 'xlsx'; 

export async function importBooks(file) {
    const extension = file.originalname.split('.').pop().toLowerCase();
    let rows;

    if (extension === 'csv' || extension === 'xls' || extension === 'xlsx') {
        const workbook = XLSX.read(file.buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        rows = XLSX.utils.sheet_to_json(sheet);
    } else {
        throw new Error(`Unsupported file type: ${extension}`);
    }
}