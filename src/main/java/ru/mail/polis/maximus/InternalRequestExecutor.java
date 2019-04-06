package ru.mail.polis.maximus;

import java.util.List;

public interface InternalRequestExecutor<T> {
    List<T> executeRequests(List<InternalRequest<T>> internalRequests);
}
