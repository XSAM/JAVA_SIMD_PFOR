package io.dashbase.local;

import io.dashbase.codec.utils.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

// Learn from https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_24_Inheritance.java
public class IntCompressionBenchmark {
    @State(Scope.Thread)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(3)
    @BenchmarkMode(Mode.Throughput)
    public static abstract class AbstractBenchmark {
        int BLOCK_SIZE = 128;
        int SIZE = 5120;
        long[][] mockData;
        final Directory d = new ByteBuffersDirectory();
        long[] tmpInput = new long[BLOCK_SIZE];
        long[][] tmpOutput = new long[SIZE][BLOCK_SIZE];
        BasePForUtil util;

        final String tmpFileName = "test.bin";

        public long[][] mockData(int size, int maxBit) {
            long[][] out = new long[size][BLOCK_SIZE];
            Random random = ThreadLocalRandom.current();
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < BLOCK_SIZE; j++) {
                    out[i][j] = random.nextInt(maxBit);
                }
            }
            return out;
        }

        public abstract void init();

        public void decode(BasePForUtil util, String fileName) throws IOException {
            var in = d.openInput(fileName, IOContext.DEFAULT);
            for (int i = 0; i < SIZE; i++) {
                util.decode(in, tmpOutput[i]);
            }
            in.close();
        }

        public void encode(BasePForUtil base, String filename) throws IOException {
            IndexOutput out = d.createOutput(filename, IOContext.DEFAULT);
            for (int i = 0; i < SIZE; i++) {
                System.arraycopy(mockData[i], 0, tmpInput, 0, BLOCK_SIZE);
                base.encode(tmpInput, out);
            }
            out.close();
        }

        @Setup
        public void setup() throws IOException {
            init();

            mockData = mockData(SIZE, 18);

            encode(util, "test2.bin");
            decode(util, "test2.bin");

            for (int i = 0; i < SIZE; i++) {
                assertArrayEquals(mockData[i], tmpOutput[i]);
            }
        }

        @TearDown
        public void tearDownDecodeFile() throws IOException {
            try {
                d.deleteFile("test2.bin");
            } catch (Exception e) {
                // ignore
            }
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            try {
                d.deleteFile(tmpFileName);
            } catch (Exception e) {
                // ignore
            }
        }

        @Benchmark
        public void encode() throws IOException {
            encode(util, tmpFileName);
        }

        @Benchmark
        public void decode() throws IOException {
            decode(util, "test2.bin");
        }
    }

    public static class Lucene extends AbstractBenchmark {
        @Override
        public void init() {
            util = new PForUtil(new ForUtil());
        }
    }

    public static class FastPFOR extends AbstractBenchmark {
        @Override
        public void init() {
            util = new PForUtilV2();
        }
    }

//    public static class VectorFastPFOR extends AbstractBenchmark {
//        @Override
//        public void init() {
//            util = new PForUtilV3();
//        }
//    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntCompressionBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

