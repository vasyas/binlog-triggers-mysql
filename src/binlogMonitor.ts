import {BinlogPosition} from "./binlogTriggers"

const ZongJi = require("@vlasky/zongji")

// pass filename & position to rsttart
export function startBinlogMonitoring(dbConfig: DbConfig, options, onBinLog): () => BinlogPosition {
  const zongji = createReconnectingBinlogMonitor(dbConfig, options, onBinLog)

  let newest = zongji

  zongji.on("child", (child, reason) => {
    if (reason) {
      console.log("Creating new binlog monitor:", reason.message)
    }

    newest.stop()
    newest = child
  })

  function stop(): BinlogPosition {
    const position: BinlogPosition = {
      filename: newest.filename,
      position: newest.position,
    }

    console.log("Stopping binlog monitor")
    newest.stop()

    return position
  }

  return stop
}

export interface DbConfig {
  host: string
  user: string
  password: string
  database: string
  port?: number
}

const RETRY_TIMEOUT = 4000

function createReconnectingBinlogMonitor(
  dbConfig: DbConfig,
  options,
  eventHandler: (evt: unknown, position: BinlogPosition) => void
) {
  const newInst = new ZongJi(dbConfig, options)

  console.log(`Creating new binlog monitor for ${dbConfig.host}`)

  function onBinlog(evt: any) {
    console.log(
      `onBinlog ${evt.getEventName()}:`,
      newInst.options.position,
      newInst.options.filename
    )

    eventHandler(evt, {
      filename: newInst.filename,
      position: newInst.position,
    })
  }

  newInst.on("error", (reason: Error) => {
    console.log("Binlog monitor error", reason.message)

    newInst.removeListener("binlog", onBinlog)

    setTimeout(() => {
      // If multiple errors happened, a new instance may have already been created
      if (!("child" in newInst)) {
        console.log("Creating child at position", newInst.position)

        newInst.child = createReconnectingBinlogMonitor(
          dbConfig,
          {
            ...options,
            filename: newInst.filename,
            position: newInst.position,
          },
          eventHandler
        )
        newInst.emit("child", newInst.child, reason)
        newInst.child.on("child", (child) => newInst.emit("child", child))
      }
    }, RETRY_TIMEOUT)
  })

  newInst.on("binlog", onBinlog)
  newInst.start(options)

  return newInst
}
