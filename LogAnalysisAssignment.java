import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    public record AnalysisSummary(
            Report report,
            long processedFiles,
            long failedFiles,
            long readLines,
            long validRecords,
            long corruptedRecords,
            long readErrors,
            int maxActiveFiles
    ) {}

    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {

        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    //Задание 1
    public static LogEntry parseLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Line is null");
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Empty line, expected 5 fields"            );
        }

        String[] fields = trimmed.split("\\s+");

        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 fields but got " + fields.length
                            + " in line: '" + line + "'"
            );
        }

        String method = fields[0];
        String path   = fields[1];

        int status;
        long responseBytes;
        long durationMillis;
        try {
            status = Integer.parseInt(fields[2]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Bad status field: '" + fields[2]
                            + "' in line: '" + line + "'",
                    exception
            );
        }
        try {
            responseBytes = Long.parseLong(fields[3]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Bad responseBytes field: '" + fields[3]
                            + "' in line: '" + line + "'",
                    exception
            );
        }
        try {
            durationMillis = Long.parseLong(fields[4]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Bad durationMillis field: '" + fields[4]
                            + "' in line: '" + line + "'",
                    exception
            );
        }

        return new LogEntry(method, path, status, responseBytes, durationMillis);
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile)

                throws IOException {
            writeFiles(outputDirectory, fileCount, linesPerFile, false);
        }

        public static void generateSkewed(Path outputDirectory, int fileCount, int linesPerFile)
                throws IOException {
            writeFiles(outputDirectory, fileCount, linesPerFile, true);
        }

        private static void writeFiles(
                Path outputDirectory,
                int fileCount,
                int linesPerFile,
                boolean skewed
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            String hotPath = "/api/users";

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    //Задание 4
    static final class MutableStats {

        private long requests;
        private long successful;
        private long errors;
        private long responseBytes;
        private long totalDurationMillis;
        private long maxDurationMillis;

        synchronized void add(LogEntry entry) {
            requests = requests + 1;
            if (entry.status() >= 200 && entry.status() <= 399) {
                successful = successful + 1;
            } else {
                errors = errors + 1;
            }
            responseBytes = responseBytes + entry.responseBytes();
            totalDurationMillis = totalDurationMillis + entry.durationMillis();
            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        synchronized PathStatistics snapshot() {
            return new PathStatistics(
                    requests, successful, errors,
                    responseBytes, totalDurationMillis, maxDurationMillis
            );
        }
    }

    static final class StopFlag {
        private volatile boolean value;
        void stop()           { value = true; }
        boolean isStopped()   { return value; }
    }

    static final class StatisticsAccumulator {

        private final Map<String, MutableStats> global = new HashMap<>();

        private final ConcurrentHashMap<String, MutableStats> perPath =
                new ConcurrentHashMap<>();

        final AtomicLong processedFiles   = new AtomicLong();  //сколько файлов дочитали до конца
        final AtomicLong failedFiles      = new AtomicLong();  //сколько файлов не открылись/упали
        final AtomicLong readLines        = new AtomicLong();  //сколько строк прочитали
        final AtomicLong validRecords     = new AtomicLong();  //сколько строк успешно разобрали
        final AtomicLong corruptedRecords = new AtomicLong();  //сколько строк битые
        final AtomicLong readErrors       = new AtomicLong();  //сколько было ошибок чтения
        final AtomicInteger activeFiles   = new AtomicInteger(); //сколько файлов сейчас читается
        final AtomicInteger maxActiveFiles = new AtomicInteger(); //пик одновременно читаемых

        synchronized void addGlobal(LogEntry entry) {
            MutableStats stats = global.computeIfAbsent(
                    entry.path(), k -> new MutableStats()
            );
            stats.add(entry);
        }

        synchronized void addPerPath(LogEntry entry) {
            MutableStats stats = perPath.computeIfAbsent(
                    entry.path(), k -> new MutableStats()
            );
            stats.add(entry);
        }

        //Задание 6
        void fileStarted() {
            int current = activeFiles.incrementAndGet();
            while (true) {
                int previous = maxActiveFiles.get();
                if (current <= previous) {
                    return;
                }
                if (maxActiveFiles.compareAndSet(previous, current)) {
                    return;
                }
            }
        }

        void fileFinished() {
            activeFiles.decrementAndGet();
        }

        Map<String, PathStatistics> snapshot(boolean useGlobal) {
            Map<String, PathStatistics> result = new HashMap<>();
            if (useGlobal) {
                synchronized (this) {
                    for (Map.Entry<String, MutableStats> e : global.entrySet()) {
                        result.put(e.getKey(), e.getValue().snapshot());
                    }
                }
            } else {
                // ConcurrentHashMap можно обходить без блокировки
                for (Map.Entry<String, MutableStats> e : perPath.entrySet()) {
                    result.put(e.getKey(), e.getValue().snapshot());
                }
            }
            return result;
        }

        Report buildReport(boolean useGlobal) {
            long total = 0, ok = 0, err = 0, bytes = 0, maxDur = 0;
            for (PathStatistics p : snapshot(useGlobal).values()) {
                total   = total   + p.requests();
                ok      = ok      + p.successful();
                err     = err     + p.errors();
                bytes   = bytes   + p.responseBytes();
                if (p.maxDurationMillis() > maxDur) {
                    maxDur = p.maxDurationMillis();
                }
            }
            return new Report(total, ok, err, bytes, maxDur);
        }

        AnalysisSummary summary(boolean useGlobal) {
            return new AnalysisSummary(
                    buildReport(useGlobal),
                    processedFiles.get(),
                    failedFiles.get(),
                    readLines.get(),
                    validRecords.get(),
                    corruptedRecords.get(),
                    readErrors.get(),
                    maxActiveFiles.get()
            );
        }
    }

    //Задание 2
    private static void processFileSimple(
            Path path,
            StatisticsAccumulator acc,
            boolean useGlobal
    ) {
        acc.fileStarted();
        try {
            Iterator<String> iterator = LogFileLines.iterator(path);

            while (iterator.hasNext()) {
                String line = iterator.next();
                acc.readLines.incrementAndGet();
                try {
                    LogEntry entry = parseLine(line);
                    acc.validRecords.incrementAndGet();
                    if (useGlobal) {
                        acc.addGlobal(entry);
                    } else {
                        acc.addPerPath(entry);
                    }
                } catch (IllegalArgumentException ex) {
                    //битая строка — просто пропускаем её
                    acc.corruptedRecords.incrementAndGet();
                }
            }
            acc.processedFiles.incrementAndGet();
        } catch (RuntimeException ex) {
            acc.failedFiles.incrementAndGet();
            acc.readErrors.incrementAndGet();
        } finally {
            acc.fileFinished();
        }
    }

    public static AnalysisSummary analyzeSequential(Path dir) throws IOException {
        List<Path> files;
        try (var stream = Files.list(dir)) {
            files = stream.filter(Files::isRegularFile).sorted().toList();
        }

        StatisticsAccumulator acc = new StatisticsAccumulator();
        for (Path file : files) {
            processFileSimple(file, acc, false);
        }
        return acc.summary(false);
    }

    private static void processFileParallel(
            Path path,
            StatisticsAccumulator acc,
            Semaphore semaphore,
            CountDownLatch latch,
            StopFlag stopFlag,
            boolean useGlobal
    ) {
        boolean acquired = false;
        try {
            //Задание 5
            semaphore.acquire();
            acquired = true;
            acc.fileStarted();

            Iterator<String> iterator = LogFileLines.iterator(path);

            //Задание 8. Часть здесь
            while (!stopFlag.isStopped() && iterator.hasNext()) {
                String line = iterator.next();
                acc.readLines.incrementAndGet();

                //Задание 9
                try {
                    LogEntry entry = parseLine(line);
                    acc.validRecords.incrementAndGet();
                    if (useGlobal) {
                        acc.addGlobal(entry);
                    } else {
                        acc.addPerPath(entry);
                    }
                } catch (IllegalArgumentException ex) {
                    acc.corruptedRecords.incrementAndGet();
                }
            }
            acc.processedFiles.incrementAndGet();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException ex) {
            acc.failedFiles.incrementAndGet();
            acc.readErrors.incrementAndGet();
        } finally {
            if (acquired) {
                acc.fileFinished();
                semaphore.release();
            }
            latch.countDown();
        }
    }

    public static AnalysisSummary analyzeParallel(
            Path dir,
            int threadCount,
            int semLimit,
            boolean useGlobal
    ) throws Exception {
        return analyzeParallelWithStop(dir, threadCount, semLimit, useGlobal, -1);
    }

    public static AnalysisSummary analyzeParallelWithStop(
            Path dir,
            int threadCount,
            int semLimit,
            boolean useGlobal,
            long stopAfterMs
    ) throws Exception {

        List<Path> files;
        try (var stream = Files.list(dir)) {
            files = stream.filter(Files::isRegularFile).sorted().toList();
        }
        if (files.isEmpty()) {
            throw new IllegalStateException("В каталоге " + dir + " нет файлов");
        }

        StatisticsAccumulator acc = new StatisticsAccumulator();
        Semaphore semaphore = new Semaphore(semLimit);
        CountDownLatch latch = new CountDownLatch(files.size());
        StopFlag stopFlag = new StopFlag();

        //Задание 8
        if (stopAfterMs > 0) {
            Thread stopper = new Thread(() -> {
                try {
                    Thread.sleep(stopAfterMs);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                stopFlag.stop();
            });
            stopper.setDaemon(true);
            stopper.start();
        }

        //Задание 3
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
            for (Path file : files) {
                pool.submit(() -> processFileParallel(
                        file, acc, semaphore, latch, stopFlag, useGlobal
                ));
            }
            latch.await();
        } finally {
            stopFlag.stop();
            pool.shutdown();
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        }
        return acc.summary(useGlobal);
    }

    private static void printSummary(String title, AnalysisSummary s) {
        System.out.println("=== " + title + " ===");
        System.out.printf("  requests:           %d%n", s.report().totalRequests());
        System.out.printf("  successful:         %d%n", s.report().successfulRequests());
        System.out.printf("  errors:             %d%n", s.report().errorRequests());
        System.out.printf("  responseBytes:      %d%n", s.report().responseBytes());
        System.out.printf("  maxDurationMillis:  %d%n", s.report().maxDurationMillis());
        System.out.printf("  files:              processed=%d failed=%d%n",
                s.processedFiles(), s.failedFiles());
        System.out.printf("  lines:              read=%d valid=%d corrupted=%d%n",
                s.readLines(), s.validRecords(), s.corruptedRecords());
        System.out.printf("  readErrors:         %d%n", s.readErrors());
        System.out.printf("  max concurrent:     %d file(s)%n", s.maxActiveFiles());
    }

    private static boolean sameReport(Report a, Report b) {
        return a.totalRequests()      == b.totalRequests()
                && a.successfulRequests() == b.successfulRequests()
                && a.errorRequests()      == b.errorRequests()
                && a.responseBytes()      == b.responseBytes()
                && a.maxDurationMillis()  == b.maxDurationMillis();
    }




    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0];

        if (mode.equals("generate")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int fileCount = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int linesPerFile = args.length > 3 ? Integer.parseInt(args[3]) : 1000;
            GenerateLogs.generate(dir, fileCount, linesPerFile);
            System.out.println("Сгенерировано " + fileCount + " файлов в " + dir);
            return;
        }

        if (mode.equals("generate-skewed")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs-skewed");
            int fileCount = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int linesPerFile = args.length > 3 ? Integer.parseInt(args[3]) : 1000;
            GenerateLogs.generateSkewed(dir, fileCount, linesPerFile);
            System.out.println("Сгенерировано " + fileCount
                    + " skewed-файлов в " + dir);
            return;
        }

        if (mode.equals("check")) {
            selfCheck();
            return;
        }

        if (mode.equals("parse")) {
            String line = String.join(" ",
                    java.util.Arrays.copyOfRange(args, 1, args.length));
            System.out.println(parseLine(line));
            return;
        }

        if (mode.equals("sequential")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            printSummary("sequential", analyzeSequential(dir));
            return;
        }

        if (mode.equals("parallel")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int limit = args.length > 3 ? Integer.parseInt(args[3]) : 4;
            boolean useGlobal = args.length > 4 && args[4].equals("global");
            printSummary(
                    "parallel " + (useGlobal ? "(вариант A)" : "(вариант B)"),
                    analyzeParallel(dir, threads, limit, useGlobal)
            );
            return;
        }

        if (mode.equals("stop")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int limit = args.length > 3 ? Integer.parseInt(args[3]) : 4;
            boolean useGlobal = args.length > 4 && args[4].equals("global");
            long stopAfter = args.length > 5 ? Long.parseLong(args[5]) : 50;
            printSummary(
                    "остановка через " + stopAfter + " мс",
                    analyzeParallelWithStop(dir, threads, limit, useGlobal, stopAfter)
            );
            return;
        }

        if (mode.equals("compare")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int limit = args.length > 3 ? Integer.parseInt(args[3]) : 4;

            long t0 = System.nanoTime();
            AnalysisSummary seq = analyzeSequential(dir);
            long t1 = System.nanoTime();

            AnalysisSummary parA = analyzeParallel(dir, threads, limit, true);
            long t2 = System.nanoTime();

            AnalysisSummary parB = analyzeParallel(dir, threads, limit, false);
            long t3 = System.nanoTime();

            printSummary("sequential", seq);
            System.out.printf("  время: %.1f мс%n%n", (t1 - t0) / 1e6);

            printSummary("parallel (вариант A, глобальная блокировка)", parA);
            System.out.printf("  время: %.1f мс%n%n", (t2 - t1) / 1e6);

            printSummary("parallel (вариант B, per-URL блокировка)", parB);
            System.out.printf("  время: %.1f мс%n%n", (t3 - t2) / 1e6);

            boolean ok = sameReport(seq.report(), parA.report())
                    && sameReport(seq.report(), parB.report());
            System.out.println(ok
                    ? "MATCH: все три отчёта совпадают"
                    : "MISMATCH: отчёты различаются!");
            return;
        }

        printUsage();
    }

    private static void printUsage() {
        System.out.println("Использование:");
        System.out.println("  generate <dir> <files> <lines>");
        System.out.println("  generate-skewed <dir> <files> <lines>");
        System.out.println("  check");
        System.out.println("  parse <line...>");
        System.out.println("  sequential <dir>");
        System.out.println("  parallel <dir> [threads] [semLimit] [global|perPath]");
        System.out.println("  stop <dir> [threads] [semLimit] [global|perPath] [ms]");
        System.out.println("  compare <dir> [threads] [semLimit]");
    }
}
