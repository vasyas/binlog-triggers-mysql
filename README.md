API for listening to MySQL binlog events.

### Overview 

This library provides API for listening to binlog events. Events can be filtered by table name
and/or operation type (insert, delete etc.)

### Usage example

```typescript
import {BinlogTriggers, DbConfig} from "binlog-triggers-mysql"

const binlogTriggers = new BinlogTriggers()

binlogTriggers.table("test", {
  insert: (rows) => {
    console.log("Got new rows", rows)
    countRows++
  }
})

binlogTriggers.start({
  database: "binlog_demo",
  host: "localhost",
  password: "test",
  user: "test",
  port: 3306,
})
```

### Implementation details

To parse MySQL binlog messages, it uses https://github.com/nevill/zongji .


In addition to API `binlog-triggers-mysql` also provides MySQL reconnect. It is useful on
network issues or MySQL server restarts.
