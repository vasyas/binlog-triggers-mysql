import {EventEmitter} from "events"

import {DbConfig, startBinlogMonitoring} from "./binlogMonitor"
import {ensureArray} from "./utils"

const log = require("loglevel")

export class BinlogTriggers extends EventEmitter {
  allTables(
    events: Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> | BinlogEventHandlers
  ) {
    if (typeof events != "object") {
      events = {
        all: events,
      }
    }

    const prevEvents = this.allTableEvents || {}

    for (const eventName of Object.keys(events)) {
      prevEvents[eventName] = [...(prevEvents[eventName] || []), ...ensureArray(events[eventName])]
    }

    this.allTableEvents = {...prevEvents}

    return this
  }

  table(
    tableName: string,
    events: Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> | BinlogEventHandlers
  ) {
    if (typeof events != "object") {
      events = {
        all: events,
      }
    }

    const prevEvents = this.tableEvents[tableName] || {}

    for (const eventName of Object.keys(events)) {
      prevEvents[eventName] = [...(prevEvents[eventName] || []), ...ensureArray(events[eventName])]
    }

    this.tableEvents[tableName] = {...prevEvents}

    return this
  }

  start(dbConfig: DbConfig) {
    log.info("Binlog triggers starting")

    startBinlogMonitoring(dbConfig, {
      startAtEnd: true,
      includeEvents: ["rotate", "tablemap", "writerows", "deleterows", "updaterows",],
      includeSchema: {
        [dbConfig.database]: this.allTableEvents ? true : Object.keys(this.tableEvents),
      },
    }, (evt) => {
      this.emit("binlog", evt);

      const eventName = evt.getEventName()
      const table = evt.tableMap && evt.tableMap[evt.tableId]

      if (!table) return

      const handlers = this.getHandlers(table.tableName, eventName)

      let rows = null
      let prevRows = null

      if (eventName == "updaterows") {
        rows = []
        prevRows = []

        evt.rows.forEach(({before, after}) => {
          prevRows.push(before)
          rows.push(after)
        })
      } else {
        rows = evt.rows
      }

      const event = {
        name: eventName,
        tableName: table.tableName,
        evt,
      }

      handlers.forEach((h) => {
        h.call(null, rows, prevRows, event)
      })
    })
  }

  private tableEvents: {[tableName: string]: Partial<BinlogEvents<BinlogEventHandler[]>>} = {}
  private allTableEvents: Partial<BinlogEvents<BinlogEventHandler[]>> = {}

  private getHandlers(tableName, eventName): BinlogEventHandler[] {
    const events = this.tableEvents[tableName] || {}

    if (eventName == "writerows")
      return [
        ...(events.insert || []),
        ...(events.insertDelete || []),
        ...(events.all || []),
        ...(this.allTableEvents.insert || []),
        ...(this.allTableEvents.insertDelete || []),
        ...(this.allTableEvents.all || []),
      ]

    if (eventName == "deleterows")
      return [
        ...(events.delete || []),
        ...(events.insertDelete || []),
        ...(events.all || []),
        ...(this.allTableEvents.delete || []),
        ...(this.allTableEvents.insertDelete || []),
        ...(this.allTableEvents.all || []),
      ]

    if (eventName == "updaterows")
      return [
        ...(events.update || []),
        ...(events.all || []),
        ...(this.allTableEvents.update || []),
        ...(this.allTableEvents.all || []),
      ]

    return []
  }
}

export interface BinlogEvents<T> {
  insert: T
  delete: T

  update: T

  insertDelete: T
  all: T
}

export type BinlogEventHandlers = BinlogEventHandler | BinlogEventHandler[]
export type BinlogEventHandler = (rows: unknown[], prevRows: unknown[], event: BinlogEvent) => void

export interface BinlogEvent {
  name: string
  tableName: string
  evt: unknown
}
