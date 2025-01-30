const sql = require("mssql");
const mssqlConnStr = process.env.CONNECTION_STRING_MSSQL;

async function connectMSSQL() {
	await sql.connect(mssqlConnStr);
}

async function execSQLQuery(sqlQry) {
	await connectMSSQL();
	const request = new sql.Request();
	const {recordset} = await request.query(sqlQry);
	return recordset;
}

async function execSQLStream(sqlQry) {
	await connectMSSQL();
	const request = new sql.Request();
	request.stream = true;
	request.query(sqlQry);
	return request;
}

module.exports = {
	execSQLQuery,
	execSQLStream,
};
