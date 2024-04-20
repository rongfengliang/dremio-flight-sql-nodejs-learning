const { createFlightSqlClient } = require("@lakehouse-rs/flight-sql-client");
const { tableFromIPC } = require("apache-arrow");

BigInt.prototype.toJSON = function () {
  return this.toString();
};

const options = {
  username: "admin",
  password: "admin123",
  tls: false,
  host: "localhost",
  port: 32010,
  headers: [],
};

async function start(options,sql) {
  const client = await createFlightSqlClient(options);
  const buffer = await client.query(sql);
  const table = tableFromIPC(buffer);
  let res = {
    rowCount: table.numRows,
    schema: table.schema.fields.map((f) => {
      return { name: f.name, type: { name: f.metadata.get("ARROW:FLIGHT:SQL:TYPE_NAME") } };
    }),
    rows: table.toArray(),
  };
  return  res
}

(async function (){
  let result = await start(options,"select * from sys.options");
  console.log(result)
})() 
