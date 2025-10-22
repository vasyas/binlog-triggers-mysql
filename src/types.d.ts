declare module "@vlasky/zongji" {
  namespace ZongJi {
    interface StartOptions {
      serverId: number
      startAtEnd: boolean
      filename: string
      position: number
      includeEvents: EventTypes[]
      excludeEvents: EventTypes[]
      includeSchema: Schema
      excludeSchema: Schema
    }

    interface Schema {
      [schema: string]: boolean | string[]
    }

    type EventTypes =
      | "unknown"
      | "query"
      | "intvar"
      | "rotate"
      | "format"
      | "xid"
      | "tablemap"
      | "writerows"
      | "updaterows"
      | "deleterows"

    interface Event {
      [key: string]: unknown
      dump: () => void
      getEventName: () => EventTypes

      // only for some events
      rows?: {before: Row; after: Row}[]
      tableId?: string
      tableMap?: Record<string, Table>
    }

    type Row = Record<string, unknown>

    type Table = {
      tableName: string
      columns: Column[]
      columnSchemas: ColumnSchema[]
    }

    type Column = {
      name: string
      charset: string
      type: number
      metadata: {
        bits?: number
      }

      length: number
    }

    type ColumnSchema = {
      COLUMN_TYPE: string
    }
  }

  class ZongJi {
    constructor(dbConfig: unknown)

    start(options?: Partial<ZongJi.StartOptions>): void
    stop(): void

    on(event: "ready" | "stopped", handler: () => void): void

    on(event: "binlog", handler: (event: ZongJi.Event) => void): void
    removeListener(event: "binlog", handler: (event: ZongJi.Event) => void): void

    on(event: "error", handler: (reason: Error) => void): void
    removeListener(event: "error", handler: (reason: Error) => void): void

    on(event: "child", handler: (child: ZongJi, reason: Error) => void): void
    emit(event: "child", child: ZongJi, reason: Error): void

    options: ZongJi.StartOptions
  }

  export = ZongJi
}

declare module "@vlasky/mysql"
