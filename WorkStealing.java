import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable{
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor{}
    static class ThreadPerTaskExecutor implements ShutdownableExecutor{
        List<Thread> threads = new ArrayList<>();
        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }
        public void shutdown(){
            threads.forEach(t -> {try {t.join();} catch (InterruptedException e) {throw new RuntimeException(e);}});
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {};

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads){
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x->x<threads, x->x+1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x-> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id){
            return new Thread(() ->
                {
                    while (true) {
                        Runnable task;
                        try {
                            task = tasks.get(id).takeFirst();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == EXIT_TASK)
                            return;
                        task.run();
                    }
                }
            );
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }
            return null;
        }
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService pool;

        FixedThreadPoolExecutor(int threads) {
            this.pool = Executors.newFixedThreadPool(threads);
        }

        @Override
        public void execute(Runnable command) {
            pool.execute(command);
        }

        @Override
        public void shutdown() {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(1, TimeUnit.DAYS)) {
                    pool.shutdownNow();
                    pool.awaitTermination(1, TimeUnit.DAYS);
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    // Это вещи, которые нам нужны
    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000; //ms = 10 s

    // Это всякое вспомогательное побочное
    static final int MEAN_TASK_TIME =  (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER); //ms
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2+1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    static final long BLACK_HOLE_ITERATIONS_PER_UNIT = 100_000L;
    static volatile long blackHoleSink;

    static void blackHole(long difficulty) {
        long iterations = difficulty * BLACK_HOLE_ITERATIONS_PER_UNIT;
        long x = 0x9E3779B97F4A7C15L ^ iterations;

        for (long i = 0; i < iterations; i++) {
            x ^= (x << 13);
            x ^= (x >>> 7);
            x ^= (x << 17);
            x += i * 0x9E3779B97F4A7C15L;
        }

        blackHoleSink = x;
    }

    static Runnable createTask(int duration) {
        return () -> blackHole(duration);
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        // var random = java.util.concurrent.ThreadLocalRandom.current();
        var durations = new int[TASK_NUMBER];

        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution){
        var durations = createTaskDurations(distribution);
        return new Vector<>(
                IntStream
                    .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                    .mapToObj(x -> createTask(durations[x]))
                    .toList()
        );
    }

    public record Pair<A,B>(A first, B second){};

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution){
        var tasks = createTasks(distribution);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submitionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submitionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);

        // В теории, на винде вот эта строчка может навернуть систему, будьте аккуратны!
//        var threadPerTaskExecutorResult = measureExecutor(new ThreadPerTaskExecutor(), TaskDistribution.UNIFORM);
//
//        System.out.println("Thread per task executor submtition time: " + threadPerTaskExecutorResult.first/1_000_000d);
//        System.out.println("Thread per task execution time: " + threadPerTaskExecutorResult.second/1_000_000d);

        for (var distribution : TaskDistribution.values()) {
            System.out.println("Task distribution: " + distribution);

            var fixedThreadPoolResult = measureExecutor(
                    new FixedThreadPoolExecutor(THREAD_NUMBER),
                    distribution
            );
            System.out.println("Fixed Thread Pool submition time: "
                    + fixedThreadPoolResult.first() / 1_000_000d);
            System.out.println("Fixed Thread Pool execution time: "
                    + fixedThreadPoolResult.second() / 1_000_000d);

            var roundRobinExecutorResult = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution);

            System.out.println("Round Robin Executor submition time: " + roundRobinExecutorResult.first/1_000_000d);
            System.out.println("Round Robin Executor execution time: " + roundRobinExecutorResult.second/1_000_000d);

            var workStealingExecutorResult = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution);

            System.out.println("Work Stealing Executor submition time: " + workStealingExecutorResult.first/1_000_000d);
            System.out.println("Work Stealing Executor execution time: " + workStealingExecutorResult.second/1_000_000d);
        }
    }
}
