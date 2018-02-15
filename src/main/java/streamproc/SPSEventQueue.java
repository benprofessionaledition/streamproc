package streamproc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class SPSEventQueue {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(SPSEventQueue.class);
    private final List<SPSEvent> queue = new LinkedList<>();

    private long currentTimestamp = 0L;

    public List<SPSEvent> getQueue() {
        return queue;
    }

    public boolean enqueue(SPSEvent s) {
        long timestamp = s.getTime();
        if (timestamp > currentTimestamp) {
            LinkedList<SPSEvent> oneSecond = new LinkedList<>(queue);
            aggregate(oneSecond);
            clear();
            currentTimestamp = timestamp;
        }
        return queue.add(s);
    }

    public void clear() {
        queue.clear();
    }

    void aggregate(List<SPSEvent> oneSecond) {
        IncrementingHashMap ih = new IncrementingHashMap();
        oneSecond.stream()
                .filter(s -> s.getSev().contains("success"))
                .forEach(ih::add);
        ih.countMap.entrySet().stream().map(e -> {
            SPSEvent event = e.getKey();
            int count = e.getValue();
            return new SPSSummary(event.getDevice(), count, event.getTitle(), event.getCountry());
        })
                .map(s -> {
                    try {
                        return mapper.writeValueAsString(s);
                    } catch (JsonProcessingException e) {
                        return "Malformed input: " + s;
                    }
                })
                .forEach(System.out::println);
    }

    private static class IncrementingHashMap {

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
