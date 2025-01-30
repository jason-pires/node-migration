const mysql = require("mysql2/promise");
const mysqlConnStr = process.env.CONNECTION_STRING_MYSQL;

async function connectMYSQL() {
	return mysql.createConnection(mysqlConnStr);
}

async function executeSQL(sqlQry, values) {
	const _conn = await connectMYSQL();
	const [result, fields] = await _conn.execute(sqlQry, values);
	await _conn.end();
	return result;
}

async function executeBulkSQL(sqlQry, values) {
	const _conn = await connectMYSQL();
	const [result, fields] = await _conn.query(sqlQry, [values]);
	await _conn.end();
	return result;
}

async function executeStream(sqlQry) {
	const _conn = await connectMYSQL();
	const [result, fields] = await _conn.execute(sqlQry);
	await _conn.end();
	return result;
}

module.exports = {
	executeStream,
	executeSQL,
	executeBulkSQL,
};
