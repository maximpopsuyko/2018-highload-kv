package ru.mail.polis.maximus;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.mail.polis.maximus.MaximusKVService.ENTITY_PATH;
import static ru.mail.polis.maximus.MaximusKVService.INTERNAL_HEADER;

public class DeleteInternalRequest extends InternalRequest<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteInternalRequest.class);

    public DeleteInternalRequest(HttpClient client, String id) {
        super(client, id);
    }

    @Override
    public Boolean call() {
        try {
            Response response = getClient().delete(ENTITY_PATH + "?id=" + getId(), INTERNAL_HEADER);
            return (response.getStatus() == 202);
        } catch (Exception e) {
            LOGGER.error("Delete internal request failed. ID:  " + getId(), e);
            return false;
        }
    }
}
