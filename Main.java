import java.util.concurrent.atomic.DoubleAdder;

public class Main {
    public static final int SIZE = 10000000;
    public static final int THREADS = 10;
    public static final int ITEMS_PER_THREAD = SIZE/THREADS;

    public static final double A = 0;
    public static final double B = 5;
    public static final double STEP = (B-A)/SIZE;

    static class Acc{
        volatile double acc = 0;
        synchronized public void addToAcc(double n){
            acc += n;
        }
    }

    public static double F(double x){
        return Math.sin(x)*Math.exp(-x/10)+Math.cos(3*x);
    }

    public static double integralSTEP(int i){
        double x=A+(i+0.5)*STEP;
        return F(x)*STEP;
    }

    public static Thread taskThread(int n, int[] schedule, double[] results){
        return new Thread(()-> {
            int start = schedule[n];
            int finish = schedule[n] + ITEMS_PER_THREAD;
            double acc = 0;
            for (int i = start; i < finish; i++) {
                acc += integralSTEP(i);
            }
            results[n] = acc;
        });
    }
    public static Thread taskMonitorThread(int n, int[] schedule, Acc acc){
        return new Thread(()-> {
            int start = schedule[n];
            int finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.addToAcc(integralSTEP(i));
            }
        });
    }
    public static Thread taskAtomicThread(int n, int[] schedule, DoubleAdder acc){
        return new Thread(()-> {
            int start = schedule[n];
            int finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.add(integralSTEP(i));
            }
        });
    }

    public static void measureP() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] results = new double[THREADS];

        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, results);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        double pResult = 0;
        for (int i = 0; i < THREADS; i++) {
            pResult += results[i];
        }
        var pFinish = System.nanoTime();
        System.out.println("Parallel result");
        System.out.println(pResult);
        System.out.println("Parallel time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureMon() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.acc;
        var pFinish = System.nanoTime();
        System.out.println("Monitor result");
        System.out.println(pResult);
        System.out.println("Monitor time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void measureAtomic() throws InterruptedException {

        var threadsStart = new int[THREADS];

        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        var acc = new DoubleAdder();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, acc);
        }

        for (int i = 0; i < THREADS; i++)
            threads[i].start();

        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        var pResult = acc.sum();
        var pFinish = System.nanoTime();
        System.out.println("Atomic result");
        System.out.println(pResult);
        System.out.println("Atomic time (ms)");
        System.out.println((double)(pFinish - pStart)/1000000);
    }

    public static void main(String[] args) throws InterruptedException {
        var start = System.nanoTime();
        double acc = 0;
        for (int i = 0; i < SIZE; i++) {
            acc += integralSTEP(i);
        }

        var finish = System.nanoTime();

        System.out.println("Sequential result");
        System.out.println(acc);
        System.out.println("Sequential time (ms)");
        System.out.println((double)(finish - start)/1000000);

        measureP();
        measureAtomic();
        measureMon();
    }
}
