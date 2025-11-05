import {afterEach, beforeEach, describe, it} from "node:test"
import {BinlogTriggers} from "../src/binlogTriggers.ts"
import {dbConfig, killConnection, sql} from "./testUtils.ts"
import {setTimeout} from "node:timers/promises"
import * as assert from "node:assert"
import {_latestZongi} from "../src/binlogMonitor.ts"

describe("binlog continuation", async () => {
  let binlogTriggers: BinlogTriggers

  beforeEach(async () => {
    binlogTriggers = new BinlogTriggers()
  })

  afterEach(() => {
    binlogTriggers.stop()
  })

  it("reconnects on mysql connection close", async () => {
    let events = 0

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      events++
    })

    binlogTriggers.start(dbConfig, 100500)
    await setTimeout(10)

    await sql("insert into test(name) values('name')")
    await setTimeout(10)
    assert.equal(events, 1)

    await killConnection(_latestZongi.connection)
    await setTimeout(3_000)

    await sql("insert into test(name) values('name2')")
    await setTimeout(10)

    assert.equal(events, 2)
  })

  it("restore binlog position on reconnect", async () => {
    let events = 0

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      events++
    })

    binlogTriggers.start(dbConfig, 100500)
    await setTimeout(10)

    await sql("insert into test(name) values('name')")
    await setTimeout(10)
    assert.equal(events, 1)

    sql("insert into test(name) values('name2')")
    await killConnection(_latestZongi.connection)
    await setTimeout(3_000)

    assert.equal(events, 2)
  })

  it("save and restore position on shutdown", async () => {
    let events = 0

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      events++
    })

    binlogTriggers.start(dbConfig, 100500)
    await setTimeout(20)

    await sql("insert into test(name) values('name')")
    await setTimeout(20)
    assert.equal(events, 1)

    sql("insert into test(name) values('name2')")
    const position = binlogTriggers.stop()
    await setTimeout(1_000)

    binlogTriggers.start(dbConfig, 100500, position)
    await setTimeout(100)

    assert.equal(events, 2)
  })

  it("no position without events", async () => {
    let events = 0

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      console.log(_event)

      events++
    })

    binlogTriggers.start(dbConfig, 100500)
    await setTimeout(200)

    const position = binlogTriggers.stop()
    assert.ok(position.filename)
    assert.ok(position.position > 0)
  })
})
