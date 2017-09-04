package com.yahoo.ycsb.db;

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocketClient;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.workloads.TransactionalWorkload;

import java.io.*;
import java.util.*;

public class MyDBClient extends DB {

  protected UnixDomainSocketClient socket;
  protected OutputStream out;
  protected InputStream in;
  protected byte inp[];

  @Override
  public void init() {
    inp = new byte[128];
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
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      try {
        System.out.println("waiting for response");
        int len = in.read(inp);
        Messages.Response response = Messages.Response.parseFrom(new String(Arrays.copyOfRange(inp, 0, len)).getBytes());
        System.out.println("response read");
        if (response.getType() == Messages.Response.RESPONSE_TYPE.STATUS) {
          return Status.ERROR;
        } else {
          result.put(TransactionalWorkload.FIELDNAME, new LongByteIterator(response.getLongResult()));
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
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      try {
        System.out.println("waiting for response");
        int len = in.read(inp);
        Messages.Response response = Messages.Response.parseFrom(new String(Arrays.copyOfRange(inp, 0, len)).getBytes());
        System.out.println("response read");
        if (response.getType() == Messages.Response.RESPONSE_TYPE.STATUS) {
          System.out.println("error");
          return Status.ERROR;
        } else {
          Map<String, String> res = response.getTextResultMap();
          for (String k : res.keySet()) {
            result.put(k, new StringByteIterator(res.get(k)));
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
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    } else {
      HashMap<String, String> row = new HashMap<>();
      for (String field : values.keySet()) {
        row.put(field, values.get(field).toString());
      }
      Messages.Request m = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.UPDATE_TEXT)
          .setKey(key)
          .putAllTextRow(row)
          .build();

      try {
        out.write(m.toByteArray());
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    }
  }
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
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
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    } else {
      HashMap<String, String> row = new HashMap<>();
      for (String field : values.keySet()) {
        row.put(field, values.get(field).toString());
      }
      Messages.Request request = Messages.Request.newBuilder()
          .setTable(table)
          .setType(Messages.Request.REQUEST_TYPE.INSERT_TEXT)
          .setKey(key)
          .putAllTextRow(row)
          .build();

      try {
        out.write(request.toByteArray());
        System.out.println("message sent");
      } catch (IOException e) {
        System.err.println("error sending message" + e);
      }
      return processStatus();
    }
  }


  @Override
  public Status delete(String table, String key) {
    Messages.Request m = Messages.Request.newBuilder()
        .setTable(table)
        .setType(Messages.Request.REQUEST_TYPE.DELETE)
        .setKey(key)
        .build();

    try {
      out.write(m.toByteArray());
      System.out.println("message sent");
    } catch (IOException e) {
      System.err.println("error sending message" + e);
    }
    return processStatus();
  }

  private Status processStatus() {
    try {
      System.out.println("waiting for response");
      int len = in.read(inp);
      System.out.println("response read");
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
