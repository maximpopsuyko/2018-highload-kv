package ru.mail.polis.maximus;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.maximus.MaximusKVService.ENTITY_PATH;
import static ru.mail.polis.maximus.MaximusKVService.INTERNAL_HEADER;

public class PutInternalRequest extends InternalRequest<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutInternalRequest.class);

    private final byte[] value;

    public PutInternalRequest(HttpClient client, String id, byte[] value) {
        super(client, id);
        this.value = value;
    }

    @Override
    public Boolean call() {
        try {
            Response response = getClient().put(ENTITY_PATH + "?id=" + getId(), value, INTERNAL_HEADER);
            return (response.getStatus() == 201);
        } catch (Exception e) {
            LOGGER.error("Put internal request failed. ID:  " + getId(), e);
            return false;
        }
    }
}
