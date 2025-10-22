import ZongJi from "@vlasky/zongji"

export function startBinlogMonitoring(
  dbConfig: DbConfig,
  options: Options,
  eventHandler: (evt: ZongJi.Event, position: BinlogPosition) => void
): () => BinlogPosition {
  const zongji = createReconnectingBinlogMonitor(dbConfig, options, eventHandler)

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
      filename: newest.options.filename,
      position: newest.options.position,
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
  options: Options,
  eventHandler: (evt: ZongJi.Event, position: BinlogPosition) => void
): ZongJi {
  const newInst: ZongJi & {child?: ZongJi} = new ZongJi(dbConfig)

  console.log(`Creating new binlog monitor for ${dbConfig.host}`)

  function onBinlog(evt: ZongJi.Event) {
    eventHandler(evt, {
      filename: newInst.options.filename,
      position: newInst.options.position,
    })
  }

  newInst.on("error", (reason: Error) => {
    console.log("Binlog monitor error", reason.message)

    newInst.removeListener("binlog", onBinlog)

    setTimeout(() => {
      // If multiple errors happened, a new instance may have already been created
      if (!("child" in newInst)) {
        newInst.child = createReconnectingBinlogMonitor(
          dbConfig,
          {
            ...options,
            filename: newInst.options.filename,
            position: newInst.options.position,
          },
          eventHandler
        )
        newInst.emit("child", newInst.child, reason)
        newInst.child.on("child", (child, reason) => newInst.emit("child", child, reason))
      }
    }, RETRY_TIMEOUT)
  })

  newInst.on("binlog", onBinlog)
  newInst.start(options)

  return newInst
}

export type BinlogPosition = {
  filename: string
  position: number
}

export type Options = {}
