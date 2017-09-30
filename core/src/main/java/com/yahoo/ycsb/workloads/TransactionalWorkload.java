package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Transactional scenario. Represents series of money transfers between two bank accounts.
 * The database consists of two tables. Each table has a string key field that indicate
 * the bank account number and a long value that indicate the account balance.
 * Every transaction perform a configured number of transfers between accounts in two tables.
 */
public class TransactionalWorkload extends Workload {
  /**
   * The names of the database tables to run queries against.
   */
  public static final String TABLE_NAME_PROPERTY = "table";

  /**
   * The default names of the database tables to run queries against.
   */
  public static final String TABLE_NAME_PROPERTY_DEFAULT = "usertable1";

  protected String table;

  /**
   * List of field names of the database tables.
   */
  public static final String FIELDNAME = "balance";
  private Set<String> fieldnames;

  /**
   * How many times to retry when insertion of a single item to a DB fails.
   */
  public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
  public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

  /**
   * On average, how long to wait between the retries, in seconds.
   */
  public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
  public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

  /**
   * Number of accounts updated in one transaction.
   */
  public static final String TRANSACTION_BATCH_SIZE = "transaction_batch_size";
  public static final String TRANSACTION_BATCH_SIZE_DEFAULT = "10";

  protected int recordcount;
  protected NumberGenerator keysequence;
  protected NumberGenerator keychooser;
  protected int insertionRetryLimit;
  protected int insertionRetryInterval;
  protected int batchSize;

  private Measurements measurements = Measurements.getMeasurements();

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLE_NAME_PROPERTY, TABLE_NAME_PROPERTY_DEFAULT);

    fieldnames = new HashSet<>();
    fieldnames.add(FIELDNAME);

    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    int insertStart =
        Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    int insertCount =
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertStart)));
    keysequence = new CounterGenerator(insertStart);
    keychooser = new UniformIntegerGenerator(insertStart, insertStart + insertCount);

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

    batchSize = Integer.parseInt(p.getProperty(TRANSACTION_BATCH_SIZE, TRANSACTION_BATCH_SIZE_DEFAULT));
  }

  protected String buildKeyName(long keynum) {
    return "account" + Long.toString(keynum);
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues() {
    HashMap<String, ByteIterator> values = new HashMap<>();
    for (String fieldkey : fieldnames) {
      LongByteIterator data = new LongByteIterator(ThreadLocalRandom.current().nextLong(0, 1000*1000*1000));
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Insert one randomly generated record into one table.
   * Used during load phase.
   */
  public boolean tableInsert(String table, String dbkey, DB db) {
    HashMap<String, ByteIterator> values = buildValues();

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }


  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    return tableInsert(table, dbkey, db);
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    long ist = measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    int keynumd = keychooser.nextValue().intValue();
    String keynamed = buildKeyName(keynumd);
    try {
      db.startTransaction(keynamed);
      boolean status = true;
      for (int i = 0; i < batchSize; i++) {
        // choose random keys
        int keynum1 = keychooser.nextValue().intValue();
        String keyname1 = buildKeyName(keynum1);

        int keynum2 = keychooser.nextValue().intValue();
        String keyname2 = buildKeyName(keynum2);

        HashMap<String, ByteIterator> cells1 = new HashMap<>();
        cells1.put(FIELDNAME, new LongByteIterator(0));
        HashMap<String, ByteIterator> cells2 = new HashMap<>();
        cells2.put(FIELDNAME, new LongByteIterator(0));

        Status str1 = db.read(table, keyname1, fieldnames, cells1);
        Status str2 = db.read(table, keyname2, fieldnames, cells2);

        long balance1 = ((LongByteIterator) cells1.get(FIELDNAME)).getValue();
        long balance2 = ((LongByteIterator) cells2.get(FIELDNAME)).getValue();
        long change = ThreadLocalRandom.current().nextLong(-1000, 1000);
        cells1.put(FIELDNAME, new LongByteIterator(balance1 + change));
        cells2.put(FIELDNAME, new LongByteIterator(balance2 - change));

        Status stu1 = db.update(table, keyname1, cells1);
        Status stu2 = db.update(table, keyname2, cells1);
        if (str1 != Status.OK || str2 != Status.OK || stu1 != Status.OK || stu2 != Status.OK) {
          status = false;
        }
      }
      if (status) {
        db.commit(keynamed);
      } else {
        db.abort(keynamed);
        System.err.print("Transaction is being rolled back");
      }
    } catch (DBException e) {
      try {
        db.abort(keynamed);
        System.err.print("Transaction is being rolled back");
      } catch (DBException e1) {
        System.err.print("Error while rolling back transaction");
        e1.printStackTrace();
      }
    }

    long en = System.nanoTime();
    measurements.measure("TRANSACTION", (int) ((en - st) / 1000));
    measurements.measureIntended("TRANSACTION", (int) ((en - ist) / 1000));
    return true;
  }
}
