package ru.mail.polis.maximus;

import java.io.Serializable;

public class MyData implements Serializable {

    private final Condition condition;
    private final long time;
    private final byte[] value;

    private MyData(Condition condition, long time, byte[] value) {
        this.condition = condition;
        this.time = time;
        this.value = value;
    }

    static MyData exists(byte[] value, long time) {
        return new MyData(Condition.EXISTS, time, value);
    }

    static MyData absent() {
        return new MyData(Condition.ABSENT, -1, null);
    }

    public static MyData error() {
        return new MyData(Condition.ERROR, -1, null);
    }

    static MyData removed(long time) {
        return new MyData(Condition.REMOVED, time, null);
    }

    Condition getCondition() {
        return condition;
    }

    long getTime() {
        if (condition == Condition.ABSENT) {
            throw new IllegalStateException();
        }
        return time;
    }

    public byte[] getValue() {
        if (condition == Condition.ABSENT || condition == Condition.REMOVED) {
            throw new IllegalStateException();
        }
        return value;
    }

    enum Condition {
        EXISTS,
        REMOVED,
        ABSENT,
        ERROR
    }
}
