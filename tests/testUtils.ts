import * as mysql from "@vlasky/mysql"
import type {DbConfig} from "../src/binlogMonitor.ts"
import {before} from "node:test"

export const dbConfig: DbConfig = {
  database: "binlog_demo",
  host: "localhost",
  password: "test",
  user: "test",
  port: 3306,
}

export function sql(s: string): Promise<any> {
  const connection = mysql.createConnection(dbConfig)
  connection.connect()

  return new Promise<any>((resolve, reject) => {
    // console.log("SQL: " + s)

    connection.query(s, (error: Error | undefined, results: unknown, fields: unknown) => {
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
    `create table test (
        id int(11) primary key auto_increment,
        name varchar(12), 
        blocked bit(1) default 0, 
        active tinyint(1) default 1
    )`
  )
}

before(async () => {
  await initDatabase()
})

export async function killConnection(connection: {threadId: string}) {
  await sql(`kill ${connection.threadId}`)
}
