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

  it("receive insert event", async () => {
    let rows, prevRows, event: BinlogEvent

    binlogTriggers.table("test", (_rows, _prevRows, _event) => {
      console.log("AA")

      rows = _rows
      prevRows = _prevRows
      event = _event
    })

    binlogTriggers.start(dbConfig, 100500)

    await setTimeout(100)

    await sql("insert into test(name) values('name')")

    await setTimeout(100)

    assert.equal(rows!.length, 1)
    assert.equal(rows![0].name, "name")
    assert.ok(!prevRows)
    assert.equal(event!.name, "writerows")
  })
})
