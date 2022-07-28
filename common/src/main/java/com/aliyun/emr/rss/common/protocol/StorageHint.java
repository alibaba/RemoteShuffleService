package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.emr.rss.common.protocol.TransportMessages.PbStorageHint;

public class StorageHint implements Serializable {
  public static String UNKNOWN_DISK = "UNKNOWN_DISK";

  public enum Type {
    NON_EXISTS(0), MEMORY(1), HDD(2), SSD(3), HDFS(4), OSS(5);
    private final int type;

    Type(int type) {
      this.type = type;
    }

    public int getType() {
      return type;
    }
  }

  public static Map<Integer, Type> typesMap = new HashMap() {{
      for (Type type : Type.values()) {
        put(type.type, type);
      }
    }};

  private Type type = Type.MEMORY;
  private String mountPoint = UNKNOWN_DISK;
  private boolean finalResult = false;

  public StorageHint() {
  }

  public StorageHint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public StorageHint(Type type, String mountPoint) {
    this.type = type;
    this.mountPoint = mountPoint;
  }

  public StorageHint(Type type, String mountPoint, boolean finalResult) {
    this.type = type;
    this.mountPoint = mountPoint;
    this.finalResult = finalResult;
  }

  public boolean isFinalResult() {
    return finalResult;
  }

  public void setFinalResult(boolean finalResult) {
    this.finalResult = finalResult;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  @Override
  public String toString() {
    return "StorageHint{" +
             "type=" + type +
             ", mountPoint='" + mountPoint + '\'' +
             ", finalResult=" + finalResult +
             '}';
  }

  public static PbStorageHint toPb(StorageHint hint) {
    return PbStorageHint.newBuilder()
             .setType(hint.type.type)
             .setFinalResult(hint.finalResult)
             .setMountPoint(hint.mountPoint)
             .build();
  }

  public static StorageHint fromPb(PbStorageHint pbHint) {
    return new StorageHint(typesMap.get(pbHint.getType()), pbHint.getMountPoint(),
      pbHint.getFinalResult());
  }
}
