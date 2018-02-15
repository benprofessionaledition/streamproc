package streamproc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Cancellable;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A FlowableEmitter implementation that processes the SPS events as prescribed
 */
public class SPSProcService implements FlowableEmitter<SPSEvent> {

    private static final Logger logger = LoggerFactory.getLogger(SPSProcService.class);
    private static final String KAFKA_TOPIC_NAME = "netflix_sps";
    private static final ObjectMapper mapper = new ObjectMapper();
    private WebTarget sseTarget;
    private SseEventSource sse;
    private List<Observer<? super SPSEvent>> observables = new LinkedList<>();

    private final AtomicBoolean isCancelled = new AtomicBoolean(false);
    private final AtomicReference<Disposable> disposable = new AtomicReference<>();
    private final AtomicReference<Cancellable> cancellable = new AtomicReference<>();
    private final AtomicLong requested = new AtomicLong(0L);
    private final AtomicLong sentCounter = new AtomicLong(0);

    static {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;
        try {
            String zookeeperHosts = "localhost:2181"; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            String topicName = KAFKA_TOPIC_NAME;
            int noOfPartitions = 1;
            int noOfReplication = 1;
            Properties topicConfiguration = new Properties();

            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, null);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        System.exit(0);
    }

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

    public void stop() {
        sse.close();
    }

    private void onMessage(InboundSseEvent message) {
        SPSEvent event = getEvent(message);
        onNext(event);
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
        SPSProcService emitter = new SPSProcService();
        FlowableOnSubscribe<SPSEvent> subscriber = (s) -> {
        };
        emitter.start();
        Flowable<SPSEvent> f = Flowable.create(subscriber, BackpressureStrategy.BUFFER);
        f.filter(s -> s.getSev().equals("success")).forEach(s -> System.out.println(s.toString()));
        // 1 sec buckets
        //                .groupBy(s -> s.getTime() / 1000 * 1000)
        //                .groupBy((l, s) -> {
        //
        //                }
    }

    @Override public void setDisposable(Disposable disposable) {
        synchronized (this.disposable) {
            try {
                this.disposable.get().dispose();
            } catch (Exception e) {
                logger.warn("Exception encountered while invoking disposable", e);
            } finally {
                this.disposable.set(disposable);
            }
        }
    }

    @Override public void setCancellable(Cancellable cancellable) {
        synchronized (this.cancellable) {
            try {
                this.cancellable.get().cancel();
            } catch (Exception e) {
                logger.warn("Exception encountered while invoking cancellable", e);
            } finally {
                this.cancellable.set(cancellable);
            }
        }
    }

    @Override public long requested() {
        return requested.get();
    }

    @Override public boolean isCancelled() {
        return isCancelled.get();
    }

    @Override public FlowableEmitter<SPSEvent> serialize() {
        return this; // i don't really know what this is supposed to do
    }

    @Override public boolean tryOnError(Throwable throwable) {
        return !isCancelled.get();
    }

    @Override public void onNext(SPSEvent spsEvent) {
        logger.debug("{}", spsEvent);
    }

    @Override public void onError(Throwable throwable) {
        logger.error("Error in SPS proc service", throwable);
    }

    @Override public void onComplete() {
        stop();
    }

    public void setSPSEventQueue(SPSEventQueue q) {

    }

    private class SPSKafkaProducer {

        final KafkaProducer<Long, String> producer;

        SPSKafkaProducer() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "SPSKafka");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            this.producer = new KafkaProducer<>(props);
        }

        Future<RecordMetadata> send(long index, String message) {
            ProducerRecord<Long, String> record = new ProducerRecord<>(KAFKA_TOPIC_NAME, index, message);
            return producer.send(record);
        }

    }

}
