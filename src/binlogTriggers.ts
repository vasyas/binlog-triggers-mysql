import {EventEmitter} from "events"

import {type DbConfig, startBinlogMonitoring, type BinlogPosition} from "./binlogMonitor.ts"
import {convertMysqlTypes, ensureArray} from "./utils.ts"
import ZongJi from "@vlasky/zongji"

function getBinlogEvents(
  events: Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> | BinlogEventHandlers
): Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> {
  if (typeof events != "object") {
    return {
      all: events,
    }
  }

  return events as Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>>
}

export class BinlogTriggers extends EventEmitter {
  allTables(
    events: Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> | BinlogEventHandlers
  ) {
    const binlogEvents = getBinlogEvents(events)

    const prevEvents = this.allTableEvents || {}

    for (const eventName of Object.keys(binlogEvents) as BinlogEventType[]) {
      prevEvents[eventName] = [
        ...(prevEvents[eventName] || []),
        ...ensureArray(binlogEvents[eventName]!),
      ]
    }

    this.allTableEvents = {...prevEvents}

    return this
  }

  table(
    tableName: string,
    events: Partial<BinlogEvents<BinlogEventHandler | BinlogEventHandler[]>> | BinlogEventHandlers
  ) {
    const binlogEvents = getBinlogEvents(events)

    const prevEvents = this.tableEvents[tableName] || {}

    for (const eventName of Object.keys(binlogEvents) as BinlogEventType[]) {
      prevEvents[eventName] = [
        ...(prevEvents[eventName] || []),
        ...ensureArray(binlogEvents[eventName]!),
      ]
    }

    this.tableEvents[tableName] = {...prevEvents}

    return this
  }

  public stop: () => BinlogPosition = () => {
    throw new Error("Not started")
  }

  // save & restore filename & position
  start(dbConfig: DbConfig, serverId: number, position: Partial<BinlogPosition> = {}) {
    console.log("Starting binlog triggers")

    this.stop = startBinlogMonitoring(
      dbConfig,
      {
        ...position,
        includeEvents: ["rotate", "tablemap", "writerows", "deleterows", "updaterows"],
        includeSchema: {
          [dbConfig.database]: Object.keys(this.allTableEvents).length
            ? true
            : Object.keys(this.tableEvents),
        },
        serverId,
      },
      (evt, position) => {
        this.emit("binlog", evt)

        const eventName = evt.getEventName()
        const table = evt.tableMap && evt.tableMap[evt.tableId!]

        if (!table) return

        const handlers = this.getHandlers(table.tableName, eventName)

        let rows: Row[]
        let prevRows = undefined

        if (eventName == "updaterows") {
          rows = []
          prevRows = []

          evt.rows!.forEach(({before, after}) => {
            prevRows.push(before)
            rows.push(after)
          })
        } else {
          rows = evt.rows!
        }

        for (const row of [...(rows || []), ...(prevRows || [])]) {
          convertMysqlTypes(row, table)
        }

        const event: BinlogEvent = {
          name: eventName,
          tableName: table.tableName,
          evt,
          position,
        }

        handlers.forEach((h) => {
          try {
            h.call(null, rows, prevRows, event)
          } catch (e) {
            console.error("Error in binlog event handler", e)
          }
        })
      }
    )
  }

  private tableEvents: {[tableName: string]: Partial<BinlogEvents<BinlogEventHandler[]>>} = {}
  private allTableEvents: Partial<BinlogEvents<BinlogEventHandler[]>> = {}

  private getHandlers(tableName: string, eventName: ZongJi.EventTypes): BinlogEventHandler[] {
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

export type BinlogEventType = keyof BinlogEvents<unknown>

export type Row = Record<string, any>
export type BinlogEventHandlers = BinlogEventHandler | BinlogEventHandler[]
export type BinlogEventHandler = (
  rows: Row[],
  prevRows: Row[] | undefined,
  event: BinlogEvent
) => void

export type BinlogEvent = {
  name: string
  tableName: string
  evt: unknown
  position: BinlogPosition
}
