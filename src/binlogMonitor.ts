import ZongJi from "zongji"
const log = require("loglevel").getLogger("binlog-triggers")

export function startBinlogMonitoring(dbConfig: DbConfig, options, onBinLog) {
  const zongji = createBinlogMonitor(dbConfig, options, onBinLog)

  let newest = zongji

  zongji.on("child", (child, reason) => {
    log.debug("New binlog monitor created", reason)
    newest.stop()
    newest = child
  })

  process.on("SIGINT", () => {
    log.debug("Stopping binlog monitor")
    newest.stop()
    process.exit()
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
        console.log(newInst.options)

        newInst.child = createBinlogMonitor(
          dbConfig,
          {
            ...options,
            binlogName: newInst.binlogName,
            binlogNextPos: newInst.binlogNextPos
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


