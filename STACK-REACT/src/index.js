import app from './app.js';

import { connectDB } from './database.js';
connectDB();
app.listen(5002, () => console.log('Servidor on Port 5002'));