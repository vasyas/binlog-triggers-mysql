import {Row} from "./binlogTriggers"

export function ensureArray<T>(val: T | Array<T>): Array<T> {
  return Array.isArray(val) ? val : [val]
}

export function deepClone<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj))
}

export function convertMysqlTypes(row: Row, table) {
  const columns = getTableColumns(table)

  if (!columns) return

  columns.forEach(field => {
    const {type, length, name} = field

    // tinyint(1)
    const numberToBoolean = (type == 1 || type == 8) && length == 1

    if (numberToBoolean) {
      row[name] = row[name] === null ? null : row[name] == 1
      return
    }

    // bit(1), will result in Buffer for mysql
    // not an issue for mysql2
    if (type == 16 && length == 1) {
      if (row[name] !== null && Buffer.isBuffer(row[name])) {
        row[name] = row[name][0] == 1
      }
    }
  })
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

function getTableColumns(table): Column[] {
  // length is missing in columns, popuplate it from def
  const columns: Column[] = deepClone(table.columns)

  for (let i = 0; i < table.columnSchemas.length; i ++) {
    const schema = table.columnSchemas[i]
    const type = schema.COLUMN_TYPE || ""

    const lengthRe = /.*\((\d+)\)$/g
    const match = lengthRe.exec(type)

    if (match) {
      columns[i].length = +match[1]
    }
  }

  return columns
}