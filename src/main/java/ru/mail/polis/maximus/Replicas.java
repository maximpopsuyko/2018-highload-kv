package ru.mail.polis.maximus;

import org.jetbrains.annotations.NotNull;

class Replicas {
    private final int ack;
    private final int from;

    private Replicas(int ack, int from) {
        if (from <= 0) {
            throw new IllegalArgumentException();
        }
        if (ack <= 0 || from < ack) {
            throw new IllegalArgumentException();
        }
        this.ack = ack;
        this.from = from;
    }

    static Replicas defaultQuorum(int count) {
        return new Replicas(count / 2 + 1, count);
    }

    static Replicas from(@NotNull String replicas) {
        String[] split = replicas.split("/");
        if (split.length != 2) {
            throw new IllegalArgumentException();
        }
        return new Replicas(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
    }

    int getAck() {
        return ack;
    }

    int getFrom() {
        return from;
    }
}
