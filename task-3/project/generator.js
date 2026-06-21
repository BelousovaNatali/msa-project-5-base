const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

const config = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'postgres',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD || 'postgres',
};

const TABLE = process.env.TABLE_NAME || 'shipments';

async function exportTable() {
    const client = new Client(config);
    await client.connect();

    console.log(`[${new Date().toISOString()}] Начинаю выгрузку таблицы ${TABLE}`);

    const query = `SELECT * FROM ${TABLE}`;
    const res = await client.query(query);
    const rows = res.rows;

    if (rows.length === 0) {
        console.log('Нет данных для выгрузки');
        await client.end();
        process.exit(0);
    }

    const headers = Object.keys(rows[0]);
    const csvRows = [headers.join(',')];

    for (const row of rows) {
        const values = headers.map(header => {
            const val = row[header];
            if (val === null || val === undefined) return '';
            if (typeof val === 'string' && (val.includes(',') || val.includes('"'))) {
                return `"${val.replace(/"/g, '""')}"`;
            }
            return String(val);
        });
        csvRows.push(values.join(','));
    }

    const date = new Date().toISOString().split('T')[0];
    const filename = `${TABLE}_${date}.csv`;
    const filePath = path.join('/tmp', filename);

    fs.writeFileSync(filePath, csvRows.join('\n'), 'utf-8');
    console.log(`CSV файл сохранён: ${filePath}, записей: ${rows.length}`);
    console.log(`[${new Date().toISOString()}] Выгрузка завершена успешно`);

    await client.end();
    process.exit(0);
}

exportTable().catch(err => {
    console.error('Ошибка:', err.message);
    process.exit(1);
});