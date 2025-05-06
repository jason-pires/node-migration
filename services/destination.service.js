const db_mssql = require("../db/mssqlFuncions");
const db_mysql = require("../db/mysqlFuncions");
const utils = require("../utils/utils");

async function fillDestination(query, sources) {
	console.log(`inserindo...`);
	console.time(`insert`);

	while (sources.length > 0) {
		const item = sources.shift();
		const values = utils.json2array(item);
		await db_mysql.executeSQL(query, values);
		if (item.id % 500 === 0) {
			console.log(`INSERIU registro ${item.id} / fila ${sources.length}`);
		}
		if (item.id % 5000 === 0) {
			console.log(`PAUSANDO NO ${item.id} - fila = ${sources.length}`);
			await new Promise((resolve) => setTimeout(resolve, 100));
			console.log(`INSERINDO DO ${item.id}`);
		}
	}
	console.timeEnd(`insert`);
	console.log(`insert finalizado.`);
}

/**
 * https://medium.com/@daviemakz/how-to-vastly-improve-mysql-performance-in-node-js-77220aec540b
 */
async function bulkDestination(query, sources) {
	const bulk = [];

	for (const item of sources) {
		const values = utils.json2array(item);
		bulk.push(values);
	}

	return await db_mysql.executeBulkSQL(query, bulk);
}

let sources = [];
const paging = 50000;

async function bulkProcess(fields, table, fromMaxId) {
	let count = fromMaxId;
	if (!fromMaxId) {
		count = await getMaxDestination(table);
	}
	const query = `SELECT TOP ${paging} ${fields} FROM ${table} where id > ${count} order by id;`;
	const bulkStatement = `INSERT INTO ${table} (${fields}) VALUES ?;`;
	const stream = await db_mssql.execSQLStream(query);

	console.time(`transfer`);
	console.log(`iniciando ${paging} registros pelo id = ${count}`);
	stream.on("row", async (row) => {
		stream.pause();
		count = row.id;
		sources.push(row);
		// if (sources.length >= 1000) {
		await bulkDestination(bulkStatement, sources);
		sources = [];
		// }
		stream.resume();
	});

	stream.on("done", async () => {
		console.log();
		console.timeEnd(`transfer`);
		console.log(`ultimo id = ${count}`);
		if (fromMaxId == count) {
			console.log(`FINALIZADO - ultimo id = ${count}`);
			return;
		}
		console.log(`continuando...`);
		await bulkProcess(fields, table, count);
	});

	stream.on("error", async (err) => {
		console.log();
		console.error(`ERRO - ultimo id = ${count}`);
		console.error(`ERRO - detalhe = ${err}`);
		return;
	});
}

async function getMaxDestination(table) {
	console.log(`iniciando max...`);
	const queryMax = `select max(id) as max From ${table}`;
	const result = await db_mysql.executeStream(queryMax);
	count = result ? result[0].max : 0;
	count = count ?? 0;
	console.log(`max finalizado. resultado = ${count}`);
	return count;
}

module.exports = {
	fillDestination,
	bulkDestination,
	bulkProcess,
};
