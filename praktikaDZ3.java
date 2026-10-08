import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class praktikaDZ3 {

    //Размеры изображения
    public static final int WIDTH = 160;
    public static final int HEIGHT = 120;
    public static final int MAX_ITER = 255;

    //Границы x, y
    public static final double X_MIN = -2.0;
    public static final double X_MAX = 1.0;
    public static final double Y_MIN = -1.2;
    public static final double Y_MAX = 1.2;

    public static final double DX = (X_MAX - X_MIN) / WIDTH;
    public static final double DY = (Y_MAX - Y_MIN) / HEIGHT;

    //Количество потоков
    public static final int THREADS = Runtime.getRuntime().availableProcessors();

    public static int mandelbrot(double x0, double y0, int maxIter) {
        double zx = 0.0;
        double zy = 0.0;
        for (int t = 0; t < maxIter; t++) {
            if (zx * zx + zy * zy > 4.0) {
                return t;
            }
            double tmp = zx * zx - zy * zy + x0;
            zy = 2.0 * zx * zy + y0;
            zx = tmp;
        }
        return maxIter;
    }

    static class MandelbrotTask implements Callable<int[][]> {
        private final int rowStart;
        private final int rowEnd;

        MandelbrotTask(int rowStart, int rowEnd) {
            this.rowStart = rowStart;
            this.rowEnd = rowEnd;
        }

        @Override
        public int[][] call() {
            int rows = rowEnd - rowStart;
            int[][] local = new int[rows][WIDTH];
            for (int row = rowStart; row < rowEnd; row++) {
                double y0 = Y_MIN + row * DY;
                for (int col = 0; col < WIDTH; col++) {
                    double x0 = X_MIN + col * DX;
                    local[row - rowStart][col] = mandelbrot(x0, y0, MAX_ITER);
                }
            }
            return local;
        }
    }

    public static int[][] computeParallel() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<int[][]>> futures = new ArrayList<>();

        int rowsPerThread = HEIGHT / THREADS;
        int remainder = HEIGHT % THREADS;

        int start = 0;
        for (int i = 0; i < THREADS; i++) {
            int count = rowsPerThread + (i < remainder ? 1 : 0);
            int end = start + count;
            futures.add(pool.submit(new MandelbrotTask(start, end)));
            start = end;
        }

        int[][] result = new int[HEIGHT][WIDTH];
        int rowOffset = 0;
        for (Future<int[][]> f : futures) {
            int[][] part = f.get(); //Это ждет завершения задачи
            for (int r = 0; r < part.length; r++) {
                System.arraycopy(part[r], 0, result[rowOffset + r], 0, WIDTH);
            }
            rowOffset += part.length;
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        return result;
    }

    public static void printAscii(int[][] image) {
        String chars = " .:-=+*#%@";
        for (int row = 0; row < HEIGHT; row++) {
            StringBuilder sb = new StringBuilder();
            for (int col = 0; col < WIDTH; col++) {
                int iter = image[row][col];
                //Нормируем: 0 будет пробелом, MAX_ITER будет @ (такая вот фигня, собачка)
                int idx = (int) ((double) iter / MAX_ITER * (chars.length() - 1));
                sb.append(chars.charAt(idx));
            }
            System.out.println(sb);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("\nMandelbrot set:");
        int[][] image = computeParallel();
        printAscii(image);
    }
}