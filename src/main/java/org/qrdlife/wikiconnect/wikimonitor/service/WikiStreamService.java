package org.qrdlife.wikiconnect.wikimonitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.qrdlife.wikiconnect.wikimonitor.WikiMonitorApplication;
import org.qrdlife.wikiconnect.wikimonitor.model.RecentChange;
import org.qrdlife.wikiconnect.wikimonitor.model.StreamEventDTO;
import org.qrdlife.wikiconnect.wikimonitor.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service responsible for connecting to the Wikimedia EventStreams SSE endpoint,
 * processing events, and broadcasting them to connected clients.
 * Supports per-user filtering via AbuseFilterService.
 */
@Slf4j
@Service
public class WikiStreamService {

    private static final String EVENT_CACHE_KEY_SUFFIX = ":sse:events";
    private static final String STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    private final OkHttpClient client;
    private final ObjectMapper mapper;
    private final AbuseFilterService abuseFilter;
    private final UserService userService;
    private final RedisCache redisCache;

    private final long sseTimeoutMs;
    private final long heartbeatMs;
    private final int eventCacheMaxSize;
    private final String eventCacheKey;

    private final Map<SseEmitter, StreamContext> emitters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private EventSource eventSource;

    private volatile String lastEventId;
    private volatile boolean shuttingDown = false;
    private volatile boolean heartbeatScheduled = false;

    private final MeterRegistry registry;
    private final Counter eventsReceived;
    private final Counter eventsMatched;
    private final Timer eventProcessDuration;
    private final Timer cacheWriteDuration;
    private final Timer broadcastDuration;
    private final Counter streamReconnects;
    private final DistributionSummary replaySize;

    public WikiStreamService(ObjectMapper mapper,
                             AbuseFilterService abuseFilter,
                             UserService userService,
                             RedisCache redisCache,
                             MeterRegistry registry,
                             @Value("${sse.timeout.ms:1800000}") long sseTimeoutMs,
                             @Value("${sse.heartbeat.ms:15000}") long heartbeatMs,
                             @Value("${sse.event-cache.max-size:1000}") int eventCacheMaxSize,
                             @Value("${sse.redis.key-prefix:wikimonitor}") String redisKeyPrefix) {
        this.registry = registry;

        this.eventsReceived = Counter.builder("wiki.events.received")
                .description("Total events received from Wikimedia stream")
                .register(registry);

        this.eventsMatched = Counter.builder("wiki.events.matched.deliveries")
                .description("Matched event deliveries to connected emitters")
                .register(registry);

        this.eventProcessDuration = Timer.builder("wiki.event.process.duration")
                .description("Time from parse to broadcast dispatch")
                .register(registry);

        this.cacheWriteDuration = Timer.builder("wiki.cache.write.duration")
                .register(registry);

        this.broadcastDuration = Timer.builder("wiki.broadcast.duration")
                .description("Per-emitter send duration")
                .register(registry);

        this.streamReconnects = Counter.builder("wiki.stream.reconnects")
                .register(registry);

        this.replaySize = DistributionSummary.builder("wiki.replay.size")
                .description("Number of events replayed per reconnecting client")
                .register(registry);

        Gauge.builder("wiki.emitters.active", emitters, Map::size)
                .description("Currently connected authenticated registered emitters")
                .register(registry);

        this.mapper = mapper;
        this.abuseFilter = abuseFilter;
        this.userService = userService;
        this.redisCache = redisCache;
        this.sseTimeoutMs = sseTimeoutMs;
        this.heartbeatMs = heartbeatMs;
        this.eventCacheMaxSize = eventCacheMaxSize;
        this.eventCacheKey = redisKeyPrefix + EVENT_CACHE_KEY_SUFFIX;
        this.client = new OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.SECONDS) // No timeout for SSE
                .build();
    }

    /**
     * Connects to the Wikimedia SSE stream and starts listening for events.
     * Automatically handles reconnections on failure.
     */
    @PostConstruct
    public void startStream() {
        if (shuttingDown) {
            return;
        }
        ensureHeartbeatScheduled();
        if (eventSource != null) {
            eventSource.cancel();
        }

        Request.Builder builder = new Request.Builder()
                .url(STREAM_URL)
                .header("User-Agent", WikiMonitorApplication.userAgent);

        if (lastEventId != null) {
            log.info("Resuming stream from last event ID: {}", lastEventId);
            builder.header("Last-Event-ID", lastEventId);
        }
        Request request = builder.build();
        EventSource.Factory factory = EventSources.createFactory(client);
        this.eventSource = factory.newEventSource(request, new EventSourceListener() {
            @Override
            public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
                log.info("Connected to Wikimedia Stream");
            }

            @Override
            public void onEvent(@NotNull EventSource eventSource, @Nullable String id, @Nullable String type,
                    @NotNull String data) {
                if (shuttingDown) {
                    return;
                }
                eventsReceived.increment();
                if (id != null) {
                    lastEventId = id;
                }
                final String currentId = id != null ? id : lastEventId;
                try {
                    executor.submit(() -> {
                        if (shuttingDown) {
                            return;
                        }
                        Timer.Sample sample = Timer.start(registry);
                        try {
                            RecentChange rc = mapper.readValue(data, RecentChange.class);
                            // Cache before broadcasting so a client subscribing between the two ops finds the event.
                            cacheEvent(currentId, rc);
                            broadcastAsync(rc, currentId);
                        } catch (Exception e) {
                            log.error("Error processing event: {}", e.getMessage());
                        } finally {
                            sample.stop(eventProcessDuration);
                        }
                    });
                } catch (RejectedExecutionException ignored) {
                }
            }

            @Override
            public void onClosed(@NotNull EventSource eventSource) {
                if (shuttingDown) {
                    return;
                }
                log.info("Stream closed");
                scheduleReconnect();
            }

            @Override
            public void onFailure(@NotNull EventSource eventSource, @Nullable Throwable t,
                    @Nullable Response response) {
                if (shuttingDown) {
                    return;
                }
                log.error("Stream failure: {}", t != null ? t.toString() : "Unknown");
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (shuttingDown || scheduler.isShutdown()) {
            return;
        }
        streamReconnects.increment();
        try {
            scheduler.schedule(this::startStream, 5, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("Scheduler rejected task, likely shutting down.");
        }
    }

    /** Schedules the recurring heartbeat once; startStream() re-runs on every reconnect. */
    private synchronized void ensureHeartbeatScheduled() {
        if (heartbeatScheduled || shuttingDown || scheduler.isShutdown()) {
            return;
        }
        try {
            scheduler.scheduleAtFixedRate(this::sendHeartbeats, heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
            heartbeatScheduled = true;
        } catch (RejectedExecutionException e) {
            log.debug("Scheduler rejected heartbeat task, likely shutting down.");
        }
    }

    /** Keepalive ping to every client: keeps proxies open, signals liveness, and reaps dead emitters. */
    private void sendHeartbeats() {
        if (shuttingDown || emitters.isEmpty()) {
            return;
        }
        emitters.forEach((emitter, context) -> {
            try {
                executor.execute(() -> sendHeartbeat(emitter));
            } catch (RejectedExecutionException ignored) {
            }
        });
    }

    private void sendHeartbeat(SseEmitter emitter) {
        if (shuttingDown) {
            return;
        }
        try {
            // Named event (not onmessage data); payload advertises the interval so the client can size its timeout.
            emitter.send(SseEmitter.event().name("heartbeat").data(String.valueOf(heartbeatMs)));
        } catch (Exception e) {
            // Client is gone; stop tracking it.
            emitters.remove(emitter);
            try {
                emitter.complete();
            } catch (Exception ignored) {
            }
        }
    }

    private void cacheEvent(String id, RecentChange rc) {
        if (shuttingDown || id == null) {
            return;
        }
        Timer.Sample sample = Timer.start(registry);
        try {
            ObjectNode node = mapper.createObjectNode();
            node.put("id", id);
            node.set("data", mapper.valueToTree(rc));
            redisCache.appendToList(eventCacheKey, node, eventCacheMaxSize);
        } finally {
            sample.stop(cacheWriteDuration);
        }
    }

    private void broadcastAsync(RecentChange rc, String eventId) {
        if (emitters.isEmpty())
            return;

        emitters.forEach((emitter, context) -> {
            if (context.paused)
                return;

            try {
                executor.execute(() -> {
                    Timer.Sample sample = Timer.start(registry);
                    try {
                        // While replay is in progress, queue live events for drainPendingEvents().
                        synchronized (context) {
                            if (context.replaying) {
                                context.pending.add(new PendingEvent(eventId, rc));
                                return;
                            }
                        }
                        sendIfMatched(emitter, context, rc, eventId);
                    } catch (JsonProcessingException e) {
                        log.error("Error serializing RecentChange", e);
                    } catch (IOException e) {
                        log.warn("Client disconnected", e);
                        emitter.complete();
                        emitters.remove(emitter);
                    } catch (Exception e) {
                        log.error("Unexpected error while broadcasting", e);
                        emitter.completeWithError(e);
                        emitters.remove(emitter);
                    } finally {
                        sample.stop(broadcastDuration);
                    }
                });
            } catch (RejectedExecutionException ignored) {
                // Executor is shutting down; drop this dispatch, same pattern as onEvent().
            }
        });
    }

    private void sendIfMatched(SseEmitter emitter, StreamContext context, RecentChange rc, String eventId)
            throws IOException {
        List<String> matchedFilters = abuseFilter.matches(rc, context.user);
        if (matchedFilters.isEmpty()) {
            return;
        }
        eventsMatched.increment();
        StreamEventDTO dto = StreamEventDTO.fromRecentChange(rc, matchedFilters);
        String userPayload = mapper.writeValueAsString(dto);

        SseEmitter.SseEventBuilder event = SseEmitter.event()
                .id(eventId)
                .data(userPayload);

        emitter.send(event);
    }

    /**
     * Subscribes a client to the event stream.
     *
     * @param principal The authenticated user principal, or null if anonymous.
     * @return An SseEmitter instance for receiving events.
     */
    public SseEmitter subscribe(Principal principal) {
        return subscribe(principal, null);
    }

    /**
     * Subscribes a client to the event stream, optionally replaying events the
     * client missed while disconnected.
     *
     * @param principal         The authenticated user principal, or null if anonymous.
     * @param clientLastEventId The {@code Last-Event-ID} header sent by the browser, or null.
     * @return An SseEmitter instance for receiving events.
     */
    public SseEmitter subscribe(Principal principal, String clientLastEventId) {
        SseEmitter emitter = new SseEmitter(sseTimeoutMs);

        // Wire callbacks before registration so a replay-time error can clean up.
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(t -> emitters.remove(emitter));

        User user = resolveUser(principal);
        if (user == null) {
            log.info("Client subscribed: Anonymous");
            return emitter;
        }

        boolean shouldReplay = clientLastEventId != null && !clientLastEventId.isEmpty();
        StreamContext context = new StreamContext(user, shouldReplay);

        // Register first so live events arriving during replay are queued, not dropped.
        emitters.put(emitter, context);
        log.info("Client subscribed: {}{}", user.getUsername(),
                shouldReplay ? " (resuming from " + clientLastEventId + ")" : "");

        // Immediate ping so the client turns "connected" without waiting for the first edit or heartbeat tick.
        sendHeartbeat(emitter);

        if (shouldReplay) {
            try {
                replayMissedEvents(emitter, context, clientLastEventId);
            } finally {
                drainPendingEvents(emitter, context);
            }
        }
        return emitter;
    }

    private User resolveUser(Principal principal) {
        if (principal == null) {
            return null;
        }
        try {
            return (User) ((org.springframework.security.authentication.UsernamePasswordAuthenticationToken) principal)
                    .getPrincipal();
        } catch (Exception e) {
            try {
                return (User) userService.loadUserByUsername(principal.getName());
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    private void replayMissedEvents(SseEmitter emitter, StreamContext context, String clientLastEventId) {
        List<JsonNode> entries = redisCache.rangeFromList(eventCacheKey);
        if (entries.isEmpty()) {
            replaySize.record(0);
            return;
        }
        boolean found = false;
        int replayed = 0;
        for (JsonNode entry : entries) {
            JsonNode idNode = entry.get("id");
            JsonNode dataNode = entry.get("data");
            if (idNode == null || dataNode == null) {
                continue;
            }
            String id = idNode.asText();
            if (!found) {
                if (clientLastEventId.equals(id)) {
                    found = true;
                }
                continue;
            }
            try {
                RecentChange rc = mapper.treeToValue(dataNode, RecentChange.class);
                sendIfMatched(emitter, context, rc, id);
                replayed++;
            } catch (IOException e) {
                log.warn("Replay aborted for {}: client disconnected ({})",
                        context.user.getUsername(), e.getMessage());
                emitter.complete();
                emitters.remove(emitter);
                replaySize.record(replayed);
                return;
            } catch (Exception e) {
                log.warn("Skipping malformed replay entry: {}", e.getMessage());
            }
        }
        replaySize.record(replayed);
        if (!found) {
            log.warn("Last-Event-ID [{}] not found in cache for user {}; gap in coverage possible",
                    clientLastEventId, context.user.getUsername());
        } else if (replayed > 0) {
            log.info("Replayed {} missed events for user {}", replayed, context.user.getUsername());
        }
    }

    private void drainPendingEvents(SseEmitter emitter, StreamContext context) {
        Deque<PendingEvent> snapshot;
        synchronized (context) {
            context.replaying = false;
            snapshot = new ArrayDeque<>(context.pending);
            context.pending.clear();
        }
        for (PendingEvent pe : snapshot) {
            try {
                sendIfMatched(emitter, context, pe.rc, pe.id);
            } catch (IOException e) {
                log.warn("Client disconnected while draining pending events", e);
                emitter.complete();
                emitters.remove(emitter);
                return;
            } catch (Exception e) {
                log.warn("Error draining pending event {}: {}", pe.id, e.getMessage());
            }
        }
    }

    /**
     * Pauses or resumes the stream for a specific user.
     *
     * @param principal The authenticated user.
     * @param paused    True to pause, false to resume.
     */
    public void setPaused(Principal principal, boolean paused) {
        if (principal == null)
            return;
        String username = principal.getName();
        emitters.values().stream()
                .filter(ctx -> ctx.user.getUsername().equals(username))
                .forEach(ctx -> {
                    ctx.paused = paused;
                    log.debug("Stream paused for {}: {}", username, paused);
                });
    }

    public boolean isPaused(Principal principal) {
        if (principal == null)
            return false;
        String username = principal.getName();
        return emitters.values().stream()
                .filter(ctx -> ctx.user.getUsername().equals(username))
                .findFirst()
                .map(ctx -> ctx.paused)
                .orElse(false);
    }

    public void updateUser(User updatedUser) {
        if (updatedUser == null || updatedUser.getId() == null) {
            return;
        }
        emitters.values().stream()
                .filter(ctx -> ctx.user.getId().equals(updatedUser.getId()))
                .forEach(ctx -> ctx.user = updatedUser);
    }

    @PreDestroy
    public void cleanup() {
        // Set first so in-flight callbacks bail out before touching Redis.
        shuttingDown = true;
        if (eventSource != null) {
            eventSource.cancel();
        }
        scheduler.shutdownNow();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        client.dispatcher().executorService().shutdown();
    }

    private static class StreamContext {
        User user;
        boolean paused;
        boolean replaying;
        final Deque<PendingEvent> pending = new ArrayDeque<>();

        StreamContext(User user, boolean replaying) {
            this.user = user;
            this.paused = false;
            this.replaying = replaying;
        }
    }

    private record PendingEvent(String id, RecentChange rc) {}
}