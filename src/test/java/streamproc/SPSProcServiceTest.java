package streamproc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This was just to make sure the code to consume SSE streams worked
 */
public class SPSProcServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(SPSProcServiceTest.class);

    private Client sseClient;
    private WebTarget sseTarget;

    @Before
    public void setupClient() {
        sseClient = ClientBuilder.newClient();
        sseTarget = sseClient.target("https://tweet-service.herokuapp.com/sps");
    }

    @Test
    public void testNotNull() {
        Assert.assertNotNull(sseClient);
        Assert.assertNotNull(sseTarget);
    }

    @Ignore
    @Test
    public void testDoStuff() {
        SseEventSource sse = SseEventSource
                .target(sseTarget)
                .reconnectingEvery(2, SECONDS)
                .build();
        sse.register(this::onMessage);
        sse.open();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            logger.error("Unrecoverable error", e);
            throw new RuntimeException(e);
        } finally {
            sse.close();
        }
    }

    private void onMessage(InboundSseEvent message) {
        System.out.println(message.isEmpty());
    }

}
