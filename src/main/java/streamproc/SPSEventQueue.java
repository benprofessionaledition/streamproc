package streamproc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A thin wrapper around a queue (list actually) that ingests events and aggregates them once the timestamp changes.
 *
 * Note there are a couple caveats:
 * - this class is not elastic and excessively high volume will blow through the heap
 * - this implementation isn't scalable as-is, we can't just add more event queues to listen to the server
 *
 * To do the above I would write this stuff to a kafka topic, partitioned by device/title/country, to which I'd
 * add subscribers that would aggregate as necessary depending on how granular the partitions needed to be. Code for that
 * is partially implemented in the git history, but assumes a local Kafka broker and a whole host
 * of other unwieldy nonsense.
 */
public class SPSEventQueue {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(SPSEventQueue.class);
    // linkedlist for O(1) appending
    private final List<SPSEvent> queue = new LinkedList<>();
    private final Executor executor = Executors.newCachedThreadPool();

    private long currentTimestamp = 0L;

    /**
     * Returns the underlying list of events awaiting processing
     * @return the underlying list of events
     */
    public List<SPSEvent> getQueue() {
        return queue;
    }

    /**
     * Adds the event provided to the underlying collection. If this event's timestamp
     * is greater than the timestamp of those events currently in the queue, this
     * method performs an aggregation before clearing the work queue and adding the item.
     *
     * Note that this is a simplistic implementation that only works assuming the timestamps are in order. Slightly
     * out of order timestamps could be handled either with an intermediate buffer or with multiple buckets and something
     * to evict them after a set time period.
     *
     * @param s an SPSEvent
     * @return true as specified in {@link LinkedList#add(Object)}
     */
    public boolean enqueue(SPSEvent s) {
        long timestamp = s.getTime();
        if (timestamp > currentTimestamp) {
            LinkedList<SPSEvent> oneSecond = new LinkedList<>(queue);
            executor.execute(getAggregator(oneSecond));
            clear();
            currentTimestamp = timestamp;
        }
        return queue.add(s);
    }

    /**
     * Clears the underlying data structure
     */
    public void clear() {
        queue.clear();
    }

    /**
     * "Aggregates" the batch provided in-memory
     * @param oneSecond
     */
    @VisibleForTesting
    public List<SPSSummary> aggregate(List<SPSEvent> oneSecond) {
        IncrementingHashMapCounter ih = new IncrementingHashMapCounter();
        oneSecond.stream()
                .filter(s -> s.getSev().contains("success"))
                .forEach(ih::add);
        return ih.countMap.entrySet().stream().map(e -> {
            SPSEvent event = e.getKey();
            int count = e.getValue();
            return new SPSSummary(event.getDevice(), count, event.getTitle(), event.getCountry());
        }).collect(Collectors.toList());

    }

    /**
     * Returns a runnable that will perform the aggregation method. Since the
     * SSE implementation being used in the service has no notion of backpressure, I opted
     * to perform aggregation on a separate thread to avoid excessive blocking in
     * whatever thread is enqueueing (consuming) stuff
     * @param batch a 1-second batch of events
     * @return a runnable whose run() method performs an aggregation
     */
    private Runnable getAggregator(List<SPSEvent> batch) {
        return () -> aggregate(batch)
                .stream()
                .map(s -> {
                    try {
                        return mapper.writeValueAsString(s);
                    } catch (JsonProcessingException e) {
                        return "Malformed input: " + s;
                    }
                })
                .forEach(System.out::println);
    }

    /**
     * Simple counter
     */
    private static class IncrementingHashMapCounter {

        final HashMap<SPSEvent, Integer> countMap = new HashMap<>();

        Integer add(SPSEvent key) {
            if (countMap.containsKey(key)) {
                Integer i = countMap.get(key);
                return countMap.put(key, i + 1);
            } else {
                return countMap.put(key, 1);
            }
        }

    }

}
