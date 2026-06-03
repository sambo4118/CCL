import 'dotenv/config';

export default {
    schema: './database/schema.js',
    out: './database/migrations',
    dialect: 'sqlite',
    dbCredentials: {
        url: './database/library.db'
    }
};