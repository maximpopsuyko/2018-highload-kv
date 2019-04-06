package ru.mail.polis.maximus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SeveralInternalRequestsExecutor<T> implements InternalRequestExecutor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SeveralInternalRequestsExecutor.class);

    private final ExecutorService executorService;

    SeveralInternalRequestsExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public List<T> executeRequests(List<InternalRequest<T>> internalRequests) {
        List<T> result = new ArrayList<>(internalRequests.size());
        try {
            List<Future<T>> futures = executorService.invokeAll(internalRequests);
            for (Future<T> future : futures) {
                T value = future.get();
                result.add(value);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Error to executeRequests internalRequests", e);
        }
        return result;
    }
}
