package com.bigdatapassion.hbase.api.loader;

public abstract class HBaseLoader {

    final int COMMIT = 10000;

    public abstract void load();

}
