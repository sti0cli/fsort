import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Created by sti0cli on 26/12/2018.
 */
public class FGen {
    static final String carrier = "ABCDEFGHIJKLMNOPQRSTYVWXTZabcdefghijklmnopqrstyvwxyz0123456789";
    static final int carrierLen = carrier.length();
    static final Random random  = new Random();

    static char randomChar() {
        return carrier.charAt(random.nextInt(carrierLen));
    }

    int minLength = 1;

    public FGen setMinLength(int minLength) {
        this.minLength = minLength;
        return this;
    }

    public void runGen(Path path, long numLines, int maxLineLen) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            while (--numLines >= 0) {
                int lineLen = random.nextInt(maxLineLen-minLength)+minLength;
                for (int i=0; i < lineLen; ++i) {
                    writer.write(randomChar());
                }
                writer.write('\n');
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Insufficient arguments. Usage fgen num_lines max_line_len file_path");
            return;
        }

        long numLines = Long.valueOf(args[0], 10);
        int maxLineLen = Integer.valueOf(args[1], 10);
        String fileName = args[2];

        new FGen().runGen(Paths.get(fileName), numLines, maxLineLen);
    }
}
