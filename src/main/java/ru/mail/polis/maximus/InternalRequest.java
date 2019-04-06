package ru.mail.polis.maximus;

import one.nio.http.HttpClient;

import java.util.concurrent.Callable;

abstract class InternalRequest<T> implements Callable<T> {

    private final HttpClient client;
    private final String id;

    InternalRequest(HttpClient client, String id) {
        this.client = client;
        this.id = id;
    }

    HttpClient getClient() {
        return client;
    }

    String getId() {
        return id;
    }
}
