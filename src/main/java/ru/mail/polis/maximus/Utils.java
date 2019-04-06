package ru.mail.polis.maximus;

import java.nio.ByteBuffer;

class Utils {

    private Utils() {
        throw new IllegalStateException();
    }

    static long getLongFromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }
}
