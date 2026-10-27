package io.aggbench.streams;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.WriteBufferManager;

import java.util.Map;

/**
 * Bounds RocksDB off-heap memory for all stores of the instance (the documented Kafka Streams pattern:
 * one shared block cache plus a write buffer manager charged against it). Without this, every store of every
 * active and standby task allocates its own cache and memtables and the pod would exceed its memory limit.
 */
public final class BoundedRocksDbConfig implements RocksDBConfigSetter {

    static volatile long blockCacheBytes = 128L * 1024 * 1024;
    static volatile long writeBufferBytes = 64L * 1024 * 1024;

    private static Cache cache;
    private static WriteBufferManager writeBufferManager;

    private static synchronized void ensureShared() {
        if (cache == null) {
            cache = new LRUCache(blockCacheBytes, -1, false, 0.1);
            writeBufferManager = new WriteBufferManager(writeBufferBytes, cache);
        }
    }

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        ensureShared();
        BlockBasedTableConfig table = (BlockBasedTableConfig) options.tableFormatConfig();
        table.setBlockCache(cache);
        table.setCacheIndexAndFilterBlocks(true);
        table.setCacheIndexAndFilterBlocksWithHighPriority(true);
        table.setPinTopLevelIndexAndFilter(true);
        options.setWriteBufferManager(writeBufferManager);
        options.setMaxWriteBufferNumber(2);
        options.setWriteBufferSize(8L * 1024 * 1024);
        options.setTableFormatConfig(table);
    }

    @Override
    public void close(String storeName, Options options) {
        // Shared cache and write buffer manager live for the whole process.
    }
}
