package com.yahoo.ycsb.db;

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocketClient;
import com.google.protobuf.ByteString;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.TransactionalWorkload;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class MyDBClient extends DB {

  protected UnixDomainSocketClient socket;
  protected OutputStream out;
  protected InputStream in;
  protected byte inp[];

  @Override
  public void init() {
    inp = new byte[512];
    try {
      socket = new UnixDomainSocketClient("/tmp/mydbsocket", JUDS.SOCK_STREAM);
      out = socket.getOutputStream();
      in = socket.getInputStream();
    } catch (IOException e) {
      System.err.println("Error occurred during initialization");
      e.printStackTrace();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    inp = new byte[512];
    if (!result.isEmpty()) {
      Messages.Request.Builder b = Messages.Request.newBuilder()
          .setType(Messages.Request.REQUEST_TYPE.READ_LONG)
          .setTable(table)
          .setKey(key);
      if (fields != null) {
        b.addAllFields(fields);
      }
      Messages.Request m = b.build();
      try {
        out.write(m.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      try {
        int len = in.read(inp);
        Messages.Response response = Messages.Response.parseFrom(new String(Arrays.copyOfRange(inp, 0, len)).getBytes());

        if (response.getType() == Messages.Response.RESPONSE_TYPE.STATUS) {
          return Status.ERROR;
        } else {
          result.put(TransactionalWorkload.FIELDNAME, new LongByteIterator(Long.parseLong(response.getLongResult())));
          return Status.OK;
        }
      } catch (IOException e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    } else {
      Messages.Request.Builder b = Messages.Request.newBuilder()
          .setType(Messages.Request.REQUEST_TYPE.READ_TEXT)
          .setTable(table)
          .setKey(key);
      if (fields != null) {
        b.addAllFields(fields);
      }
      Messages.Request m = b.build();
      try {
        out.write(m.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      try {
        int len = in.read(inp);
        Messages.Response response = Messages.Response.parseFrom(new String(Arrays.copyOfRange(inp, 0, len)).getBytes());
        if (response.getType() == Messages.Response.RESPONSE_TYPE.STATUS) {
          return Status.ERROR;
        } else {
          Map<String, ByteString> res = response.getTextResultMap();
          for (String k : res.keySet()) {
            result.put(k, new StringByteIterator(res.get(k).toString(Charset.defaultCharset())));
          }
          return Status.OK;
        }
      } catch (IOException e) {
        e.printStackTrace();
        return Status.ERROR;
      }

    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    inp = new byte[512];
    if (values.values().iterator().next() instanceof LongByteIterator) {
      String field  = values.keySet().iterator().next();
      long value = ((LongByteIterator) values.get(field)).getValue();
      Messages.Request m = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.UPDATE_LONG)
          .setKey(key)
          .setLongField(field)
          .setLongRow(value)
          .build();

      try {
        out.write(m.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    } else {
      HashMap<String, ByteString> row = new HashMap<>();
      for (String field : values.keySet()) {
        row.put(field, ByteString.copyFrom(values.get(field).toArray()));
      }
      Messages.Request m = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.UPDATE_TEXT)
          .setKey(key)
          .putAllTextRow(row)
          .build();

      try {
        out.write(m.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    }
  }
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    inp = new byte[512];
    if (values.values().iterator().next() instanceof LongByteIterator) {
      String field = values.keySet().iterator().next();
      long value = ((LongByteIterator) values.get(field)).getValue();

      Messages.Request m = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.INSERT_LONG)
          .setKey(key)
          .setLongField(field)
          .setLongRow(value)
          .build();

      try {
        out.write(m.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    } else {
      HashMap<String, ByteString> row = new HashMap<>();
      for (String field : values.keySet()) {
        row.put(field, ByteString.copyFrom(values.get(field).toArray()));
      }
      Messages.Request request = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.INSERT_TEXT)
          .setKey(key)
          .putAllTextRow(row)
          .build();

      try {
        out.write(request.toByteArray());
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    }
  }


  @Override
  public Status delete(String table, String key) {
    inp = new byte[512];
    Messages.Request m = Messages.Request.newBuilder()
        .setTable(table)
        .setType(Messages.Request.REQUEST_TYPE.DELETE)
        .setKey(key)
        .build();

    try {
      out.write(m.toByteArray());
    } catch (IOException e) {
      System.err.println("error sending message" + e);
    }
    return processStatus();
  }

  @Override
  public Status startTransaction(String key) throws DBException {
    Messages.Request m = Messages.Request.newBuilder()
        .setType(Messages.Request.REQUEST_TYPE.START_TRANSACTION)
        .setKey(key)
        .build();

    try {
      out.write(m.toByteArray());
    } catch (IOException e) {
      System.err.println("error sending message" + e);
    }
    return processStatus();
  }

  @Override
  public Status commit(String key) throws DBException {
    Messages.Request m = Messages.Request.newBuilder()
        .setType(Messages.Request.REQUEST_TYPE.COMMIT)
        .setKey(key)
        .build();

    try {
      out.write(m.toByteArray());
    } catch (IOException e) {
      System.err.println("error sending message" + e);
    }
    return processStatus();
  }

  @Override
  public Status abort(String key) throws DBException {
    Messages.Request m = Messages.Request.newBuilder()
        .setType(Messages.Request.REQUEST_TYPE.ABORT)
        .setKey(key)
        .build();

    try {
      out.write(m.toByteArray());
    } catch (IOException e) {
      System.err.println("error sending message" + e);
    }
    return processStatus();
  }

  private Status processStatus() {
    try {
      int len = in.read(inp);
      Messages.Response response = Messages.Response.parseFrom(new String(Arrays.copyOfRange(inp, 0, len)).getBytes());
      if (response.getType() != Messages.Response.RESPONSE_TYPE.STATUS) {
        System.err.println("Wrong response type, should be status message");
        return Status.ERROR;
      } else {
        if (response.getIsStatusOk()) {
          return Status.OK;
        } else {
          return Status.ERROR;
        }
      }
    } catch (Exception e) {
      System.err.println("Error occurred during waiting for response");
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}
