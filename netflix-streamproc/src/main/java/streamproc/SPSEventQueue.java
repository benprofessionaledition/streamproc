package streamproc;

import io.reactivex.disposables.Disposable;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SPSEventQueue implements Disposable {

    private final ConcurrentLinkedQueue<SPSEvent> queue = new ConcurrentLinkedQueue<>();

    public ConcurrentLinkedQueue<SPSEvent> getQueue() {
        return queue;
    }

    public boolean enqueue(SPSEvent s) {
        return queue.add(s);
    }

    public SPSEvent poll() {
        return queue.poll();
    }

    public void clear() {
        queue.clear();
    }

    @Override public void dispose() {
        clear();
    }

    @Override public boolean isDisposed() {
        return queue.isEmpty();
    }
}
