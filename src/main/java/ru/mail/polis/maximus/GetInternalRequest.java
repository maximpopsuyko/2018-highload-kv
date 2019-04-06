package ru.mail.polis.maximus;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static one.nio.serial.Serializer.deserialize;
import static ru.mail.polis.maximus.MaximusKVService.ENTITY_PATH;
import static ru.mail.polis.maximus.MaximusKVService.INTERNAL_HEADER;

public class GetInternalRequest extends InternalRequest<MyData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetInternalRequest.class);

    public GetInternalRequest(HttpClient client, String id) {
        super(client, id);
    }

    @Override
    public MyData call() {
        try {
            Response response = getClient().get(ENTITY_PATH + "?id=" + getId(), INTERNAL_HEADER);
            byte[] body = response.getBody();
            return (MyData) deserialize(body);
        } catch (Exception e) {
            LOGGER.error("Get internal request failed. ID:  " + getId(), e);
            return MyData.error();
        }
    }
}
