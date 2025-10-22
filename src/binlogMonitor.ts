import ZongJi from "@vlasky/zongji"

/** Testing-only */
export let _latestZongi: any

export interface DbConfig {
  host: string
  user: string
  password: string
  database: string
  port?: number
}

const RETRY_TIMEOUT = 2_000

export function startBinlogMonitoring(
  dbConfig: DbConfig,
  initialOptions: Partial<ZongJi.StartOptions>,
  eventHandler: (evt: ZongJi.Event, position: BinlogPosition) => void
): () => BinlogPosition {
  let zongji: ZongJi & {child?: ZongJi}

  function onBinlog(evt: ZongJi.Event) {
    // console.log("EVT: ", {name: evt.getEventName(), nextPosition: evt.nextPosition})

    // fix position on rotate
    if (evt.getEventName() == "rotate") {
      zongji.options.position = evt.position as number
    }

    eventHandler(evt, {
      filename: zongji.options.filename,
      position: zongji.options.position,
    })
  }

  function onError(reason: Error) {
    console.log("Binlog monitor error", reason.message)

    zongji.removeListener("binlog", onBinlog)
    zongji.removeListener("error", onError)

    setTimeout(() => {
      connect({
        filename: zongji.options.filename,
        position: zongji.options.position,
      })
    }, RETRY_TIMEOUT)
  }

  function connect(position: Partial<BinlogPosition>) {
    const options = {
      ...initialOptions,
      ...position,
    }

    if (!options.filename) {
      options.startAtEnd = true
    }

    if (zongji) {
      zongji.stop()
    }

    console.log(`Connecting binlog triggers to ${dbConfig.host}`, {...options, ...position})

    zongji = new ZongJi(dbConfig)
    _latestZongi = zongji

    zongji.on("binlog", onBinlog)
    zongji.on("error", onError)

    zongji.start({...options, ...position})
  }

  connect({})

  return () => {
    console.log("Stopping binlog triggers")
    zongji.stop()

    return {
      filename: zongji.options.filename,
      position: zongji.options.position,
    }
  }
}

export type BinlogPosition = {
  filename: string
  position: number
}
