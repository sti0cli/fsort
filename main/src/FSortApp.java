import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

/**
 * Created by sti0cli on 26/12/2018.
 */
public class FSortApp {
    private int maxSegmentLength = 32 * (1 << 20);
    private Path tempDir;

    public FSortApp() {
        tempDir = Paths.get(".");
    }

    public FSortApp setMaxSegmentLength(int maxSegmentLength) {
        this.maxSegmentLength = maxSegmentLength;
        return this;
    }

    // Сегмент в памяти
    class Segment {
        List<String> lines;
        int totalLength;

        public Segment() {
            this.lines = new ArrayList<>();
        }

        boolean tryAddLine(String line) {
            if (totalLength + line.length() > maxSegmentLength) {
                if (totalLength == 0) {
                    throw new IllegalStateException("Line is too long");
                }
                return false;
            }
            lines.add(line);
            totalLength += line.length();
            return true;
        }

        List<String> dispose() {
            List<String> temp =  lines;
            lines = null;
            totalLength = -1;
            return temp;
        }
    }

    // Отсортированный сегмент во временном файле
    class SortedSegment {
        File contents;

        SortedSegment() throws IOException {
            contents = Files.createTempFile(tempDir, "segment", ".sort").toFile();
        }

        void writeLines(List<String> lines) throws IOException {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(contents))) {
                for (String line : lines) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        }

        SortedSegment merge(SortedSegment lhs, SortedSegment rhs) throws IOException {
            File lhsf = lhs.dispose();
            File rhsf = rhs.dispose();
            try (BufferedReader lhsReader = new BufferedReader(new FileReader(lhsf));
                BufferedReader rhsReader = new BufferedReader(new FileReader(rhsf));
                BufferedWriter writer = new BufferedWriter(new FileWriter(this.contents));
            ) {
                String left = lhsReader.readLine();
                String right = rhsReader.readLine();
                while (left != null || right != null) {
                    if (right == null) {
                        writer.write(left);
                        left = lhsReader.readLine();
                    } else if (left == null) {
                        writer.write(right);
                        right = rhsReader.readLine();
                    } else {
                        if (left.compareTo(right) < 0) {
                            writer.write(left);
                            left = lhsReader.readLine();
                        } else {
                            writer.write(right);
                            right = rhsReader.readLine();
                        }
                    }
                    writer.newLine();
                }
                return this;
            } finally {
                lhsf.delete();
                rhsf.delete();
            }
        }

        File dispose() {
            assert(contents != null);
            File tmp = contents;
            contents = null;
            return tmp;
        }
    }

    // Сортировка сегмента в памяти
    class SortSegmentTask extends RecursiveTask<SortedSegment> {
        Segment source;

        SortSegmentTask(Segment source) {
            this.source = source;
        }

        @Override
        protected SortedSegment compute() {
            try {
                SortedSegment result = new SortedSegment();
                List<String> lines = source.dispose();
                Collections.sort(lines);
                result.writeLines(lines);
                return result;

            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    // Параллельное разбиение исходного файла на сегменты
    class SplitSegments extends RecursiveTask<List<SortedSegment>> {
        BufferedReader inputReader;
        int maxParallelism;

        SplitSegments(BufferedReader inputReader, int maxParallelism) {
            this.inputReader = inputReader;
            this.maxParallelism = maxParallelism;
        }

        @Override
        protected List<SortedSegment> compute() {
            try {
                List<SortedSegment> sortedSegments = new ArrayList<>();

                // Читаем строки, формируем сегменты
                List<SortSegmentTask> sortTasks = new ArrayList<>();
                Segment segment = new Segment();
                while (true) {
                    String line = inputReader.readLine();

                    if (line != null) {
                        boolean segmentFull = !segment.tryAddLine(line);
                        if (segmentFull) {
                            sortTasks.add(new SortSegmentTask(segment));
                            segment = new Segment();
                            segment.tryAddLine(line);
                        }
                    } else {
                        sortTasks.add(new SortSegmentTask(segment));
                    }

                    if (sortTasks.size() >= maxParallelism || line == null) {
                        // Сортируем сегменты
                        invokeAll(sortTasks);

                        // Добавляем их в список отсортированных сегментов
                        sortedSegments.addAll(
                                sortTasks.stream()
                                        .map(SortSegmentTask::getRawResult)
                                        .collect(Collectors.toList()));

                        sortTasks.clear();
                    }
                    if (line == null) {
                        break;
                    }
                }
                return sortedSegments;

            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    // Параллельное слияние отсортированных сегментов
    class MergeTask extends RecursiveTask<SortedSegment> {
        List<SortedSegment> segments;
        int left;
        int right;

        MergeTask(List<SortedSegment> segments, int left, int right) {
            this.segments = segments;
            this.left = left;
            this.right = right;
        }

        @Override
        protected SortedSegment compute() {
            if (left == right) {
                return segments.get(left);
            } else {
                int mid = (left + right) / 2;
                MergeTask leftTask = new MergeTask(segments, left, mid);
                MergeTask rightTask = new MergeTask(segments, mid + 1, right);

                invokeAll(leftTask, rightTask);

                SortedSegment leftSorted = leftTask.join();
                SortedSegment rightSorted = rightTask.join();

                try {
                    return new SortedSegment().merge(leftSorted, rightSorted);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        }
    }

    public void runSort(File input, File dest, Integer maxParallelism) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(input));

        ForkJoinPool executor = (maxParallelism != null)
                ? new ForkJoinPool(maxParallelism)
                : new ForkJoinPool();

        // 1. Разбиваем исходный файл на отсортированные сегменты
        SplitSegments splitTask = new SplitSegments(reader, executor.getParallelism());
        executor.execute(splitTask);
        List<SortedSegment> segments = splitTask.join();

        // 2. Объединяем осортированные сегменты в результирующий файл
        MergeTask mergeTask = new MergeTask(segments, 0, segments.size()-1);
        executor.execute(mergeTask);
        SortedSegment result = mergeTask.join();

        if (!result.contents.renameTo(dest)) {
            throw new IOException("Failed to rename output file");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Parameters: input_file output_file [max_parallelism] [max_segment_size_mb]");
            return;
        }
        Path input = Paths.get(args[0]);
        Path output = Paths.get(args[1]);
        Integer maxParallelism = args.length >= 3 ? Integer.parseUnsignedInt(args[2]) : null;
        Integer maxSegmentSize = args.length >= 4 ? Integer.parseUnsignedInt(args[3]) : null;

        FSortApp app = new FSortApp();
        if (maxSegmentSize != null) {
            app.setMaxSegmentLength(maxSegmentSize * (1 << 20));
        }

        app.runSort(input.toFile(), output.toFile(), maxParallelism);
    }
}
