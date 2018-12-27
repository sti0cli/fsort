import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by sti0cli on 25/12/2018.
 */
public class TestApp {
    @Test
    public void testEmptyFile() throws Exception {
        File empty = Files.createTempFile("empty", null).toFile();
        empty.deleteOnExit();

        File result = Files.createTempFile("result", null).toFile();
        result.deleteOnExit();

        FSortApp app = new FSortApp();
        app.runSort(empty, result, null);

        try (FileReader reader = new FileReader(empty)) {
            Assert.assertTrue(reader.read() == -1);
        }
    }

    @Test
    public void testSingleSegment() throws Exception {
        Path path = Files.createTempFile("test", null);

        String testStr = "adsfsdadfdsafdsaf";

        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(testStr);
            writer.write("\n");
        }

        File input = path.toFile();
        Path outPath = Files.createTempFile("result", ".txt");

        FSortApp app = new FSortApp().setMaxSegmentLength(testStr.length() * 10);
        app.runSort(input, outPath.toFile(), null);

        BufferedReader reader = Files.newBufferedReader(outPath);
        Assert.assertEquals(testStr, reader.readLine());
        Assert.assertTrue(reader.read() == -1);
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testErrorLongString() throws Exception {
        Path path = Files.createTempFile("test", null);
        new FGen().runGen(path, 120, 3000);

        File input = path.toFile();
        Path outPath = Files.createTempFile("result", ".txt");

        FSortApp app = new FSortApp().setMaxSegmentLength(100);

        exceptionRule.expect(IllegalStateException.class);
        app.runSort(input, outPath.toFile(), null);
    }

    @Test
    public void testFileIsSorted() throws Exception {
        Path path = Files.createTempFile("test", null);
        new FGen().runGen(path, 1000, 300);

        File input = path.toFile();
        Path outPath = Files.createTempFile("result", ".txt");

        FSortApp app = new FSortApp().setMaxSegmentLength(30000);
        app.runSort(input, outPath.toFile(), null);

        try (BufferedReader reader = Files.newBufferedReader(outPath)) {
            String prevLine = "";
            String line;
            while ((line = reader.readLine()) != null) {
                Assert.assertTrue(prevLine.compareTo(line) <= 0);
                prevLine = line;
            }
        }
    }

    private int computeDigest(MessageDigest digest, Path path) throws Exception {
        int sum = 0;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                byte[] bytes = line.getBytes();
                sum = sum ^ digest.digest(bytes, 0, bytes.length);
            }
        }
        return sum;
    }

    @Test
    public void testResultConsistency() throws  Exception {
        Path path = Files.createTempFile("test", null);
        new FGen().setMinLength(16).runGen(path, 10000, 300);

        MessageDigest digest = MessageDigest.getInstance("MD5");

        int srcDigest = computeDigest(digest, path);

        File input = path.toFile();
        Path outPath = Files.createTempFile("result", ".txt");

        FSortApp app = new FSortApp();
        app.runSort(input, outPath.toFile(), null);

        Assert.assertEquals(computeDigest(digest, outPath), srcDigest);
    }

    @Test
    public void testFileNotFound() throws Exception {
        Path path = Files.createTempFile("test", null);
        Files.delete(path);

        File input = path.toFile();
        Path outPath = Files.createTempFile("result", ".txt");

        exceptionRule.expect(FileNotFoundException.class);
        FSortApp app = new FSortApp();
        app.runSort(input, outPath.toFile(), null);
    }

    @Test
    public void testSortGeneric() throws Exception {
        FSortApp app = new FSortApp();
        Path temp = Files.createTempFile(Paths.get("."), "src", ".txt");

        FGen.main(new String[]{Long.valueOf(10_000_000).toString(), "100", temp.toString()});

        long before = System.currentTimeMillis();
        app.runSort(
                temp.toFile(),
                Files.createTempFile(Paths.get("."), "result", ".txt").toFile(),
                4);
        System.out.println("Time: " + (System.currentTimeMillis() - before));

    }
}
