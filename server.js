require("dotenv").config();
const express = require("express");
const app = express();
const port = process.env.PORT;
const db_mssql = require("./db/mssqlFuncions");
const db_mysql = require("./db/mysqlFuncions");
const destination = require("./services/destination.service");

app.use(express.json());

let count = 0;
let sources = [];
const table = `log`;
const queryMax = `select max(id) as max From ${table}`;
const statement = `INSERT INTO ${table} (id, cpf, email, acao, url_api_externa, conteudo_enviado, conteudo_retornado, conteudo_erro, sucesso, dt_inicio, dt_termino, acesso_de_cliente, box, dt_vencimento_fatura, documento_id, bandeira_cartao, status_fatura, url_boleto, log_acao_id, documento_numero) 
				VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`;
const bulkStatement = `INSERT INTO ${table} (id, cpf, email, acao, url_api_externa, conteudo_enviado, conteudo_retornado, conteudo_erro, sucesso, dt_inicio, dt_termino, acesso_de_cliente, box, dt_vencimento_fatura, documento_id, bandeira_cartao, status_fatura, url_boleto, log_acao_id, documento_numero) 
				VALUES ?;`;

app.get("/max", async (req, res) => {
	console.log(`iniciando max...`);
	console.time(`max`);
	const result = await db_mysql.executeStream(queryMax);
	count = result ? result[0].max : 0;
	console.log(`max finalizado. resultado = ${count}`);
	res.json({});
});

app.get("/source/stream", async (req, res) => {
	console.log(`iniciando source...`);
	console.time(`source`);
	const query = `SELECT id, cpf, email, acao, url_api_externa, conteudo_enviado, conteudo_retornado, conteudo_erro, sucesso, dt_inicio, dt_termino, acesso_de_cliente, box, dt_vencimento_fatura, documento_id, bandeira_cartao, status_fatura, url_boleto, log_acao_id, documento_numero
		 FROM ${table} where id > ${count} order by id;`;
	const stream = await db_mssql.execSQLStream(query);

	console.log(`aguardando source...`);
	stream.on("row", async (row) => {
		sources.push(row);
		count++;
		if (count % 10000 === 0) {
			console.log(`count = ${count} fila = ${sources.length}. pausando...`);
			stream.pause();
			await new Promise((resolve) => setTimeout(resolve, 25000));
			console.log(`VOLTANDO!`);
			stream.resume();
		}
	});

	stream.on("done", () => {
		console.timeEnd(`source`);
		console.log();
		console.log(`linhas = ${sources.length}`);
		console.log(`fim`);
	});
	res.json({});
});

app.get("/destination/stream", async (req, res) => {
	console.log(`iniciando destination...`);
	destination.fillDestination(statement, sources);
	console.log(`destination finalizado.`);

	res.json({});
});

app.get("/transfer/stream", async (req, res) => {
	console.log(`iniciando transfer...`);
	console.time(`transfer`);
	const query = `SELECT id, cpf, email, acao, url_api_externa, conteudo_enviado, conteudo_retornado, conteudo_erro, sucesso, dt_inicio, dt_termino, acesso_de_cliente, box, dt_vencimento_fatura, documento_id, bandeira_cartao, status_fatura, url_boleto, log_acao_id, documento_numero
		 FROM ${table} where id > ${count} order by id;`;
	const stream = await db_mssql.execSQLStream(query);

	console.log(`aguardando source...`);
	stream.on("row", async (row) => {
		count++;
		sources.push(row);
		//if (sources.length >= 150) {
		let start = new Date().getTime();
		stream.pause();
		await destination.bulkDestination(bulkStatement, sources);
		processRows();
		let end = new Date().getTime() - start;
		const interval = new Date(end);
		console.log(
			`ultimo id inserido = ${row.id} em ${interval.getMilliseconds()}ms.`,
		);
		//}
	});

	stream.on("done", () => {
		console.timeEnd(`transfer`);
		console.log();
		console.log(`linhas = ${sources.length}`);
		console.log(`fim`);
	});

	function processRows() {
		sources = [];
		stream.resume();
	}
	res.json({});
});

//definindo as rotas
app.use("/", (req, res) => {
	res.json({message: "Funcionando!"});
});

//inicia o servidor
app.listen(port, () => console.log("API funcionando!"));
