import {afterEach, beforeEach, describe, it} from "node:test"
import * as assert from "node:assert"
import {type BinlogEvent, BinlogTriggers} from "../src/index.ts"
import {dbConfig, sql} from "./testUtils.ts"
import {setTimeout} from "node:timers/promises"

describe("Events", () => {
  let binlogTriggers: BinlogTriggers

  beforeEach(async () => {
    binlogTriggers = new BinlogTriggers()
  })

  afterEach(() => {
    binlogTriggers.stop()
  })

  it("insert", async () => {
    let rows, prevRows, event: BinlogEvent

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      rows = _rows
      prevRows = _prevRows
      event = _event
    })

    binlogTriggers.start(dbConfig, 100500)

    await setTimeout(10)

    await sql("insert into test(name) values('name')")

    await setTimeout(10)

    assert.equal(rows!.length, 1)
    assert.equal(rows![0].name, "name")
    assert.ok(!prevRows)
    assert.equal(event!.name, "writerows")
  })

  it("update", async () => {
    let rows, prevRows, event: BinlogEvent

    const {insertId: id} = await sql("insert into test(name) values('name')")

    binlogTriggers.table("test", {
      update: (_rows, _prevRows, _event) => {
        rows = _rows
        prevRows = _prevRows
        event = _event
      },
    })
    binlogTriggers.start(dbConfig, 100500)

    await setTimeout(10)

    await sql(`update test set name='name2' where id=${id}`)

    await setTimeout(10)

    assert.equal(rows!.length, 1)
    assert.equal(rows![0].name, "name2")
    assert.equal(prevRows!.length, 1)
    assert.equal(prevRows[0]!.name, "name")
    assert.equal(event!.name, "updaterows")
  })

  it("all tables", async () => {
    let rows, prevRows, event: BinlogEvent

    binlogTriggers.allTables((_rows, _prevRows, _event) => {
      rows = _rows
      prevRows = _prevRows
      event = _event
    })

    binlogTriggers.start(dbConfig, 100500)

    await setTimeout(10)

    await sql("insert into test(name) values('name')")

    await setTimeout(10)

    assert.equal(rows!.length, 1)
    assert.equal(rows![0].name, "name")
    assert.ok(!prevRows)
    assert.equal(event!.name, "writerows")
  })
})
