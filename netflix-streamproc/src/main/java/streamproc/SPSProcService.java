package streamproc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A FlowableEmitter implementation that processes the SPS events as prescribed
 */
public class SPSProcService {

    private static final Logger logger = LoggerFactory.getLogger(SPSProcService.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private WebTarget sseTarget;
    private SseEventSource sse;

    private SPSEventQueue queue = new SPSEventQueue();

    private final AtomicBoolean isCancelled = new AtomicBoolean(false);


    public SPSProcService() {
        Client sseClient = ClientBuilder.newClient();
        sseTarget = sseClient.target("https://tweet-service.herokuapp.com/sps");
    }

    public void start() {
        sse = SseEventSource
                .target(sseTarget)
                .reconnectingEvery(2, SECONDS)
                .build();
        sse.register(this::onMessage);
        sse.open();
    }


    private void onMessage(InboundSseEvent message) {
        SPSEvent event = getEvent(message);
        logger.debug("{}", event);
        enqueue(event);
    }

    private void enqueue(SPSEvent event) {
        queue.enqueue(event);
    }

    /**
     * Transforms the message to an {@link SPSEvent} object
     * @param message
     * @return an SPSEvent object, or null if the input was malformed
     */
    private @Nullable SPSEvent getEvent(InboundSseEvent message) {
        String content = message.readData();
        SPSEvent event = null;
        try {
            event = mapper.readValue(content, SPSEvent.class);
        } catch (IOException e) {
            logger.warn("Unrecoverable error on value {}", content, e);
        }
        return event;
    }

    public static void main(String[] args) throws Exception {
        new SPSProcService().start();
    }



}
