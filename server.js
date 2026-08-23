const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// API ROUTES
// ============================================================

// example: app.use('/api/books', require('./routes/books'));

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
