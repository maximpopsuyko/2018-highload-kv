package ru.mail.polis.maximus;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;


public class MaximusKVDao implements KVDao {

    private static final int FLAG_LENGTH = 1;
    private static final byte EXISTS = FLAG_LENGTH;
    private static final byte REMOVED = 0;
    private static final byte[] EMPTY = new byte[]{};

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDB db;

    public MaximusKVDao(File dir) {
        try {
            if (!dir.exists() || !dir.isDirectory()) throw new IllegalArgumentException();
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, dir.getPath());
        } catch (RocksDBException e) {
            throw new IllegalStateException(e);
        }
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        try {
            byte[] bytes = db.get(key);
            if (bytes == null || bytes.length == 0 || bytes[0] == REMOVED) {
                throw new NoSuchElementException();
            }
            return Arrays.copyOfRange(bytes, FLAG_LENGTH + Long.BYTES, bytes.length);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try {
            db.put(key, getStoredValue(value, EXISTS));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            byte[] removed = getStoredValue(EMPTY, REMOVED);
            db.put(key, removed);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public boolean removeInternal(@NotNull byte[] key) {
        try {
            byte[] removed = getStoredValue(EMPTY, REMOVED);
            db.put(key, removed);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    public MyData getInternal(@NotNull byte[] key) throws IOException {
        byte[] bytes;
        try {
            bytes = db.get(key);
            if (bytes == null || bytes.length == 0) {
                return MyData.absent();
            }
            long time = Utils.getLongFromBytes(Arrays.copyOfRange(bytes, FLAG_LENGTH, Long.BYTES + FLAG_LENGTH));
            if (bytes[0] == REMOVED) {
                return MyData.removed(time);
            }
            byte[] value = Arrays.copyOfRange(bytes, FLAG_LENGTH + Long.BYTES, bytes.length);
            return MyData.exists(value, time);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }

    }

    public boolean upsertInternal(@NotNull byte[] key, @NotNull byte[] value) {
        try {
            db.put(key, getStoredValue(value, EXISTS));
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    private byte[] getStoredValue(@NotNull byte[] value, @NotNull byte flag) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(FLAG_LENGTH + value.length + Long.BYTES);
        byteBuffer.put(flag);
        byteBuffer.putLong(System.currentTimeMillis());
        byteBuffer.put(value);
        return byteBuffer.array();
    }

    @Override
    public void close() {
        db.close();
    }
}
