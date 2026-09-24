import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Practice01LocksExercises {
    /*
     * ДОМАШНЕЕ ЗАДАНИЕ. Обедающие философы
     *
     * Философ с номером i использует вилки i и (i + 1) % philosopherCount.
     * Реализуйте Table.eat(philosopherId).
     *
     * Требования:
     *   1) перед eatWithBothForks должны быть захвачены обе нужные вилки;
     *   2) вилки всегда захватываются в порядке возрастания fork.id;
     *   3) вилки освобождаются в finally в обратном порядке;
     *   4) соседние философы не могут есть одновременно;
     *   5) программа должна завершиться без deadlock;
     *   6) tryLock с таймаутом использовать не нужно.
     *
     * Метод eatWithBothForks уже реализован. Он изображает приём пищи и
     * проверяет, что одну вилку не используют одновременно два философа.
     */
    private static final class Fork {
        private final int id;
        private final Lock lock = new ReentrantLock(true);

        private Fork(int id) {
            this.id = id;
        }
    }

    private static final class Table {
        private final Fork[] forks;
        private final AtomicIntegerArray forkUsers;
        private final AtomicBoolean conflictDetected = new AtomicBoolean();

        private Table(int philosopherCount) {
            forks = new Fork[philosopherCount];
            for (int id = 0; id < philosopherCount; id++) {
                forks[id] = new Fork(id);
            }
            forkUsers = new AtomicIntegerArray(philosopherCount);
        }

        void eat(int philosopherId) {
            Fork left = forks[philosopherId];
            Fork right = forks[(philosopherId + 1) % forks.length];

            try{
                left.lock.lock();
                right.lock.lock();
                eatWithBothForks(left, right);
            } finally{
                right.lock.unlock();
                left.lock.unlock();
            }



        }

        private void eatWithBothForks(Fork left, Fork right) {
            int leftUsers = forkUsers.incrementAndGet(left.id);
            int rightUsers = forkUsers.incrementAndGet(right.id);
            if (leftUsers != 1 || rightUsers != 1) {
                conflictDetected.set(true);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                forkUsers.decrementAndGet(right.id);
                forkUsers.decrementAndGet(left.id);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int philosopherCount = 5;
        int mealsPerPhilosopher = 100;
        Table table = new Table(philosopherCount);
        ExecutorService pool = Executors.newFixedThreadPool(philosopherCount);
        List<Future<Integer>> results = new ArrayList<>();

        try {
            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int philosopherId = philosopher;
                results.add(pool.submit(() -> {
                    int meals = 0;
                    while (meals < mealsPerPhilosopher) {
                        table.eat(philosopherId);
                        meals++;
                        Thread.yield();
                    }
                    return meals;
                }));
            }

            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int meals = results.get(philosopher).get(10, TimeUnit.SECONDS);
                if (meals != mealsPerPhilosopher) {
                    throw new AssertionError(
                            "Philosopher " + philosopher + " ate " + meals + " times");
                }
            }
        } catch (TimeoutException e) {
            throw new AssertionError("Possible deadlock: philosophers did not finish", e);
        } finally {
            pool.shutdownNow();
        }

        if (table.conflictDetected.get()) {
            throw new AssertionError("Two philosophers used the same fork");
        }

        System.out.println("OK: every philosopher ate " + mealsPerPhilosopher
                + " times, no deadlock detected");
    }

    /*
     * Вывод, означающий, что задача, скорее всего, решена правильно:
     * OK: every philosopher ate 100 times, no deadlock detected
     */
}

//
//Ответы на вопросы:
//1) Нет, взаимную блокировку организовать нельзя, т.к. ресурсы захватываются в одном глобальном порядке,
// а в это время обратный порядок освобождения на него никак не влияет
//
//2) Нет, взаимную блокировку организовать нельзя, т.к. ресурсы захватываются в одном глобальном порядке
// и из-за этого цикл ожидания ресурсов не получается
//
