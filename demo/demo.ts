import {BinlogTriggers, DbConfig} from "../src"
import * as mysql from "mysql"

const dbConfig: DbConfig = {
  database: "binlog_demo",
  host: "localhost",
  password: "test",
  user: "test",
  port: 3306,
}

function sql(s: string): Promise<void> {
  const connection = mysql.createConnection(dbConfig)
  connection.connect()

  return new Promise((resolve, reject) => {
    connection.query(s, (error, results, fields) => {
      if (error) reject(error)
      else resolve(results)

      try {
        connection.end()
      } catch (e) {
        console.log(e)
      }
    })
  })
}

async function initDatabase() {
  await sql("drop table if exists test")
  await sql("create table test (id int(11) primary key auto_increment)")
}

async function insertRow() {
  await sql("insert into test values()")
}

async function start() {
  await initDatabase()

  let countRows = 0

  const binlogTriggers = new BinlogTriggers()
  binlogTriggers.table("test", (rows) => {
    console.log("Got new rows", rows)
    countRows++
  })

  binlogTriggers.start(dbConfig)

  await insertRow()
  await new Promise(r => setTimeout(r, 100))
  console.log("Count rows", countRows)
  // should be 1

  // break connection here
  // it should be reconnected

  await insertRow()
  console.log("Count rows", countRows)
  // should be 2
}

start()