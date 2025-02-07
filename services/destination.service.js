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
	console.log(`inserindo bulk...`);
	const bulk = [];

	for (const item of sources) {
		const values = utils.json2array(item);
		bulk.push(values);
	}

	console.log(`bulk insert finalizado.`);
	return await db_mysql.executeBulkSQL(query, bulk);
}

let sources = [];

async function bulkProcess(fields, table) {
	let count = await getMax(table);
	const query = `SELECT TOP 10000 ${fields} FROM ${table} where id > ${count} order by id;`;
	const bulkStatement = `INSERT INTO ${table} (${fields}) VALUES ?;`;
	const stream = await db_mssql.execSQLStream(query);

	stream.on("row", async (row) => {
		stream.pause();
		count++;
		sources.push(row);
		// if (sources.length >= 1000) {
		await bulkDestination(bulkStatement, sources);
		sources = [];
		// }
		stream.resume();
	});

	stream.on("done", async () => {
		console.log();
		console.log(`ultimo id = ${count}`);
		// await bulkProcess(fields, table);
		console.log(`continuando...`);
	});
}

async function getMax(table) {
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
