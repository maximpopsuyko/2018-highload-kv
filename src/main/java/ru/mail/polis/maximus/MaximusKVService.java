package ru.mail.polis.maximus;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static one.nio.serial.Serializer.serialize;

/**
 * one-nio KV service impl
 */
public class MaximusKVService extends HttpServer implements KVService {
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";
    public static final String INTERNAL_HEADER = "X-Internal-header: true";
    private static final Logger LOGGER = LoggerFactory.getLogger(MaximusKVService.class);
    @NotNull
    private final Map<String, HttpClient> clients;
    @NotNull
    private final List<String> hosts;
    @NotNull
    private final ExecutorService executorService;
    @NotNull
    private final MaximusKVDao dao;
    @NotNull
    private final Replicas replicas;
    private final InternalRequestExecutor<Boolean> putRequestExecutor;
    private final InternalRequestExecutor<Boolean> deleteRequestExecutor;
    private final InternalRequestExecutor<MyData> getRequestExecutor;

    public MaximusKVService(int port, @NotNull KVDao dao, @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.dao = (MaximusKVDao) dao;
        this.replicas = Replicas.defaultQuorum(topology.size());
        clients = new HashMap<>(topology.size() - 1);
        hosts = new ArrayList<>(topology.size() - 1);
        for (String host : topology) {
            int indexOf = host.lastIndexOf(':');
            if (indexOf == -1) {
                throw new IllegalArgumentException();
            }
            if (Integer.valueOf(host.substring(indexOf + 1)) == port) {
                continue;
            }
            hosts.add(host);
            clients.put(host, new HttpClient(new ConnectionString(host)));
        }
        hosts.sort(String::compareTo);
        executorService = Executors.newWorkStealingPool();
        putRequestExecutor = new SeveralInternalRequestsExecutor<>(executorService);
        deleteRequestExecutor = new SeveralInternalRequestsExecutor<>(executorService);
        getRequestExecutor = new SeveralInternalRequestsExecutor<>(executorService);
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Path(value = STATUS_PATH)
    public Response status(Request request) {
        return request.getMethod() == Request.METHOD_GET
                ? Response.ok(Response.EMPTY)
                : new Response(Response.BAD_REQUEST, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Replicas rf = replicas;
        boolean internal = request.getHeader(INTERNAL_HEADER) != null;

        if (!request.getPath().equals(ENTITY_PATH)) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }

        String replicas = request.getParameter("replicas=");
        if (replicas != null) {
            try {
                rf = Replicas.from(replicas);
            } catch (IllegalArgumentException e) {
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
                return;
            }
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                if (internal) {
                    getInternal(session, id);
                } else {
                    get(session, id, rf);
                }
                break;
            case Request.METHOD_PUT:
                put(request, session, id, internal, rf);
                break;
            case Request.METHOD_DELETE:
                delete(session, id, internal, rf);
                break;
            default:
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        }
    }

    private void getInternal(final @NotNull HttpSession session, final @NotNull String id) throws IOException {
        try {
            MyData value = dao.getInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
            switch (value.getCondition()) {
                case ABSENT:
                case REMOVED:
                    session.sendResponse(new Response(Response.NOT_FOUND, serialize(value)));
                    break;
                case EXISTS:
                    session.sendResponse(new Response(Response.OK, serialize(value)));
                    break;
                case ERROR:
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, serialize(value)));
                    break;
            }
        } catch (Exception e) {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void get(final @NotNull HttpSession session,
                     final @NotNull String id,
                     @NotNull Replicas replicas) throws IOException {
        try {
            List<InternalRequest<MyData>> requests = getHosts(replicas)
                    .stream()
                    .map(h -> new GetInternalRequest(clients.get(h), id))
                    .collect(toList());
            List<MyData> results = getRequestExecutor.executeRequests(requests);
            results.add(dao.getInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name()))));
            MyData value = results.stream()
                    .filter(s -> s.getCondition() != MyData.Condition.ABSENT)
                    .max(Comparator.comparingLong(MyData::getTime))
                    .orElseGet(MyData::absent);
            long ackCount = results.stream().filter(s -> s.getCondition() != MyData.Condition.ERROR).count();
            switch (value.getCondition()) {
                case EXISTS:
                    session.sendResponse(
                            ackCount < replicas.getAck()
                                    ? new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
                                    : new Response(Response.OK, value.getValue())
                    );
                    return;
                case ABSENT:
                case REMOVED:
                    session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
                    return;
                case ERROR:
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    return;
                default:
                    throw new IllegalStateException();
            }

        } catch (Exception e) {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void delete(final @NotNull HttpSession session,
                        final @NotNull String id,
                        final boolean internal,
                        final @NotNull Replicas replicas) throws IOException {
        try {
            if (!internal && !hosts.isEmpty()) {
                List<InternalRequest<Boolean>> requests = getHosts(replicas)
                        .stream()
                        .map(h -> new DeleteInternalRequest(clients.get(h), id))
                        .collect(toList());
                List<Boolean> result = deleteRequestExecutor.executeRequests(requests);
                result.add(dao.removeInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name()))));
                long successCount = result.stream().filter(Predicate.isEqual(true)).count();
                if (successCount < replicas.getAck()) {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                }
            } else {
                dao.remove(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
            }
        } catch (Exception e) {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private void put(
            final @NotNull Request request,
            final @NotNull HttpSession session,
            final @NotNull String id,
            final boolean internal,
            final Replicas replicas) throws IOException {
        try {
            byte[] body = request.getBody();
            if (!internal) {
                List<InternalRequest<Boolean>> requests = getHosts(replicas)
                        .stream()
                        .map(h -> new PutInternalRequest(clients.get(h), id, body))
                        .collect(toList());
                List<Boolean> result = putRequestExecutor.executeRequests(requests);
                result.add(dao.upsertInternal(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())), body));
                long successCount = result.stream().filter(Predicate.isEqual(true)).count();
                if (successCount < replicas.getAck()) {
                    session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                } else {
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                }
            } else {
                dao.upsert(id.getBytes(Charset.forName(StandardCharsets.UTF_8.name())), body);
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
            }
        } catch (Exception e) {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private List<String> getHosts(Replicas replicas) {
        return hosts.subList(0, replicas.getFrom() - 1);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executorService.shutdownNow();
    }
}
