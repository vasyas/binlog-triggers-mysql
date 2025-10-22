import {BinlogTriggers, DbConfig} from "../src"
import * as mysql from "@vlasky/mysql"

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
  await sql(
    "create table test (id int(11) primary key auto_increment, blocked bit(1) default 0, active tinyint(1) default 1)"
  )
}

async function insertRow() {
  console.log("Insert row")
  await sql("insert into test values()")
}

async function adelay(ms) {
  await new Promise((r) => setTimeout(r, ms))
}

async function start() {
  await initDatabase()

  let countRows = 0

  const binlogTriggers = new BinlogTriggers()
  binlogTriggers.table("test", (rows, prevRows, event) => {
    console.log("Got new rows", rows)
    console.log({event})
    countRows++
  })

  // binlogTriggers.on("binlog", console.log)

  binlogTriggers.start(dbConfig, 100500)
  await adelay(50) // some for for triggers to start

  await insertRow()
  await adelay(100)
  console.log("Count rows", countRows)
  // should be 1

  console.log("Here the connection should be broken / app restarted")
  await adelay(40_000)
  // break connection here
  // it should be reconnected

  console.log("Inserting new row")

  await insertRow()
  await adelay(100)
  console.log("Count rows", countRows)
  // should be 2

  binlogTriggers.stop()
}

start()
