const ZongJi = require("@vlasky/zongji")
const log = require("loglevel")

export function startBinlogMonitoring(dbConfig: DbConfig, options, onBinLog) {
  const zongji = createBinlogMonitor(dbConfig, options, onBinLog)

  let newest = zongji

  zongji.on("child", (child, reason) => {
    if (reason) {
      log.debug("Creating new binlog monitor:", reason.message)
    }

    newest.stop()
    newest = child
  })

  process.on("SIGINT", () => {
    log.debug("Stopping binlog monitor")
    newest.stop()
  })
}

export interface DbConfig {
  host: string
  user: string
  password: string
  database: string
  port?: number
}

const RETRY_TIMEOUT = 4000

function createBinlogMonitor(dbConfig: DbConfig, options, onBinlog) {
  const newInst = new ZongJi(dbConfig, options)

  newInst.on("error", (reason: Error) => {
    newInst.removeListener("binlog", onBinlog)

    setTimeout(() => {
      // If multiple errors happened, a new instance may have already been created
      if (!("child" in newInst)) {
        newInst.child = createBinlogMonitor(
          dbConfig,
          {
            ...options,
            filename: newInst.filename,
            position: newInst.position
          },
          onBinlog
        )
        newInst.emit("child", newInst.child, reason)
        newInst.child.on("child", child => newInst.emit("child", child))
      }
    }, RETRY_TIMEOUT)
  })

  newInst.on("binlog", onBinlog)
  newInst.start(options)

  return newInst
}


