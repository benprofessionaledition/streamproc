package streamproc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class SPSEventQueueTest {

    private static final Logger logger = LoggerFactory.getLogger(SPSEventQueueTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private Client sseClient;
    private WebTarget sseTarget;

    private List<SPSEvent> eventsSeen = new LinkedList<>();

    @Before
    public void setupClient() {
        sseClient = ClientBuilder.newClient();
        sseTarget = sseClient.target("https://tweet-service.herokuapp.com/sps");
    }

    /**
     * Tests to make sure the number seen in the aggregation is the same number of individual events
     */
    @Test
    public void testAggregation() {
        SseEventSource sse = SseEventSource
                .target(sseTarget)
                .reconnectingEvery(2, SECONDS)
                .build();
        sse.register(this::onMessage);
        sse.open();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error("Unrecoverable error", e);
            throw new RuntimeException(e);
        } finally {
            sse.close();
        }

        SPSEventQueue q = new SPSEventQueue();

        List<SPSSummary> aggregate = q.aggregate(eventsSeen);

        long successfulEvents = eventsSeen.stream().filter(e -> e.getSev().equals("success")).count();
        long queueAggregation = aggregate.stream().mapToInt(SPSSummary::getSps).sum();

        assertEquals(successfulEvents, queueAggregation);
    }

    private void onMessage(InboundSseEvent message) {
        SPSEvent event;
        String s = message.readData();
        try {
            event = mapper.readValue(s, SPSEvent.class);
            logger.debug("Deserialized event {}", event);
            eventsSeen.add(event);
        } catch (IOException e) {
            logger.error("Unrecoverable error: {}", s, e);
        }
    }


}
