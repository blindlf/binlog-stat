package net.iosapk;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;

public class App {
  public static void main(String[] args) throws Exception {
    //readAsSlave();
    App app = new App();

    app.init();
    if (args.length > 0) {
      for (String filename : args) {
        app.readBinlogFile(filename);
      }
    } else {
      app.readBinlogFile("/Users/scoliu/tmp/bigdata-usercenter/mysql-bin.000085");
      //app.readBinlogFile("/Users/scoliu/Downloads/mysql-6-bin.000001");
      //app.readBinlogFile("/Users/scoliu/Downloads/mysql-bin.088559");
      //app.readBinlogFile("/Users/scoliu/Downloads/mysql-bin.088560");
      //app.readBinlogFile("/Users/scoliu/Downloads/mysql-bin.088561");
    }

    app.showResults();
  }

  // MySQL 5.6 binlog QUERY event have extra 4 bytes after SQL.
  boolean isV56 = true;

  private void init() {
  }

  private void readAsSlave() throws Exception {
    BinaryLogClient client = new BinaryLogClient("dbhost", 3306, "root", "password");
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setCompatibilityMode(
        EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
        EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    );
    client.setEventDeserializer(eventDeserializer);
    client.registerEventListener(new BinaryLogClient.EventListener() {

      @Override
      public void onEvent(Event event) {
        processEvent(event);
      }
    });

    client.connect();
  }

  private void readBinlogFile(String filename) throws Exception {
    File binlogFile = new File(filename);
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setCompatibilityMode(
        EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
        EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
    );

    // BUG of BinaryLogFileReader
    // https://github.com/shyiko/mysql-binlog-connector-java/issues/293
    if (isV56) {
      eventDeserializer.setChecksumType(ChecksumType.CRC32);
    }

    log("=== Parse %s", filename);
    try (BinaryLogFileReader reader = new BinaryLogFileReader(binlogFile, eventDeserializer)) {
      for (Event event; (event = reader.readEvent()) != null; ) {
        processEvent(event);
      }
    }
  }

  Set<String> allOp = new HashSet<>(8);
  Map<String, Map<String, Integer>> allTbl = new TreeMap<>();

  private void processEvent(Event event) {
    EventHeaderV4 header = event.getHeader();
    EventType eventType = header.getEventType();

    // Only process SQL
    if (eventType != EventType.QUERY) {
      return;
    }

    QueryEventData data = event.getData();
    String sql = data.getSql();

    String[] parts = sql.split(" ");

    String op = parts[0].toUpperCase();

    // Ignore transaction
    if (op.equals("BEGIN") || op.equals("COMMIT") || op.equals("FLUSH") || op.equals("GRANT")) {
      return;
    }

    String tbl = null;
    try {
      Statement stmt = CCJSqlParserUtil.parse(sql);
      if (stmt instanceof Alter) {
        tbl = ((Alter) stmt).getTable().getName();
      } else if (stmt instanceof CreateTable) {
        tbl = ((CreateTable) stmt).getTable().getName();
      } else if (stmt instanceof Delete) {
        tbl = ((Delete) stmt).getTable().getName();
      } else if (stmt instanceof Drop) {
        tbl = ((Drop) stmt).getName().getName();
      } else if (stmt instanceof Insert) {
        tbl = ((Insert) stmt).getTable().getName();
      } else if (stmt instanceof Merge) {
        tbl = ((Merge) stmt).getTable().getName();
      } else if (stmt instanceof Replace) {
        tbl = ((Replace) stmt).getTable().getName();
      } else if (stmt instanceof Truncate) {
        tbl = ((Truncate) stmt).getTable().getName();
      } else if (stmt instanceof Update) {
        tbl = ((Update) stmt).getTable().getName();
      } else if (stmt instanceof Upsert) {
        tbl = ((Upsert) stmt).getTable().getName();
      }
      if (null != tbl) {
        tbl = tbl.replaceAll("`", "");
      }
    } catch (JSQLParserException e) {
      String msg = e.getMessage();
      log("!!! POS: %d, SQL: %s\nEXP: %s", header.getPosition(), sql, msg);
    }

    // Operation
    allOp.add(op);

    // DB
    String db = data.getDatabase();

    // TABLE

    if (null != tbl) {
      // database.table
      if (null != db && !db.isEmpty()) {
        tbl = String.format("%s.%s", db, tbl);
      }

      // Table
      if (!allTbl.containsKey(tbl)) {
        allTbl.put(tbl, new HashMap<String, Integer>(8));
      }

      // OP
      Map<String, Integer> tblop = allTbl.get(tbl);
      if (!tblop.containsKey(op)) {
        tblop.put(op, 0);
      }

      // OP count
      tblop.put(op, tblop.get(op) + 1);
    }

    // Debug SQL
    if (op.equals("REPLACE") || op.equals("DELETE")) {
      //log("op: %s, kind: %s, SQL: %s", op, kind, data.getSql());
    }

    //log(eventType);
  }

  private void showResults() {
    StringBuilder sb = new StringBuilder();
    // Table Header: OP, OP, ...
    sb.append(String.format("%-48s", "TABLE"));
    for (String op : allOp) {
      sb.append(String.format("%-8s", op));
    }
    sb.append("\n");
    sb.append(new String(new char[allOp.size() * 8 + 50]).replace("\0", "-"));
    sb.append("\n");

    // Table Content: database.table OP_Count, OP_Count, ...
    for (String tbl : allTbl.keySet()) {
      sb.append(String.format("%-48s", tbl));
      for (String op : allOp) {
        Map<String, Integer> tblop = allTbl.get(tbl);
        int cnt = tblop.containsKey(op) ? tblop.get(op) : 0;
        sb.append(String.format("%-8d", cnt));
      }
      sb.append("\n");
    }

    log(sb.toString());
  }

  private void log(String format, Object... args) {
    System.out.println(String.format(format, args));
  }

  private void log(Object msg) {
    System.out.println(msg);
  }
}
