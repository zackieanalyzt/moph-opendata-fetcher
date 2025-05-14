const axios = require('axios');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

// PostgreSQL connection configuration
const pool = new Pool({
  user: 'postgres',
  host: '192.168.100.70',
  database: 'databank',
  password: 'grespost',
  port: 5432,
});

// API settings
const apiSettings = {
  url: 'https://opendata.moph.go.th/api/report_data',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  data: {
    tableName: 's_kpi_cvd_risk',
    year: '2568',
    province: '51', // lamphun province code
    type: 'json',
  },
};

// Function to fetch data from API
async function fetchData() {
  try {
    const response = await axios(apiSettings);
    return response.data;
  } catch (error) {
    console.error('Error fetching data from API:', error.message);
    throw error;
  }
}

// Function to insert data into PostgreSQL
async function insertData(data, province) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Prepare insert query
    const queryText = `
      INSERT INTO s_kpi_cvd_risk (id, hospcode, areacode, date_com, b_year, target, result)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `;

    let insertedCount = 0;
    // Insert each record, ensuring areacode starts with province
    for (const record of data) {
      const areacode = record.areacode || '';
      // Check if areacode starts with province code
      if (areacode.startsWith(province)) {
        const values = [
          uuidv4().replace(/-/g, ''), // Generate UUID without hyphens
          record.hospcode || '',
          areacode,
          record.date_com || null,
          record.b_year || '2568',
          record.target || null,
          record.result || null,
        ];

        await client.query(queryText, values);
        insertedCount++;
      }
    }

    await client.query('COMMIT');
    console.log(`Successfully inserted ${insertedCount} records`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error inserting data:', error.message);
    throw error;
  } finally {
    client.release();
  }
}

// Main function to run the fetch and insert process
async function main() {
  try {
    const data = await fetchData();
    if (data && Array.isArray(data)) {
      await insertData(data, apiSettings.data.province);
    } else {
      console.error('No valid data received from API');
    }
  } catch (error) {
    console.error('Main process failed:', error.message);
  } finally {
    await pool.end();
  }
}

// Run the program
main();

// Export for testing or module use
module.exports = { fetchData, insertData, main };