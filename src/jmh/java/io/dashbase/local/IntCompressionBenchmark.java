package io.dashbase.local;

import io.dashbase.codec.TurboPFORUtil;
import io.dashbase.codec.utils.*;
import org.apache.lucene.store.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

// Learn from https://github.com/openjdk/jmh/blob/master/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_24_Inheritance.java
public class IntCompressionBenchmark {
    @State(Scope.Thread)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(3)
    @BenchmarkMode(Mode.Throughput)
    public static abstract class AbstractBenchmark extends BaseBenchmark {
        @Param({"true"})
        public boolean outlierValue;
        final Directory d = new ByteBuffersDirectory();
        BasePForUtil util;

        final String tmpFileName = "test.bin";

        float compressionRatio = 0;

        public abstract void init();

        public void decode(BasePForUtil util, String fileName) throws IOException {
            var in = d.openInput(fileName, IOContext.DEFAULT);
            if (util instanceof PForUtil) {
                for (int i = 0; i < SIZE; i++) {
                    util.decode(in, tmpOutput[i]);
                }
            } else {
                util.decode(in, tmpFlatOutput);
            }
            in.close();
        }

        public void encode(BasePForUtil base, String filename) throws IOException {
            IndexOutput out = d.createOutput(filename, IOContext.DEFAULT);

            if (base instanceof PForUtil) {
                for (int i = 0; i < SIZE; i++) {
                    System.arraycopy(mockData[i], 0, tmpInput, 0, BLOCK_SIZE);
                    base.encode(tmpInput, out);
                }
            } else {
               base.encode(flatMockData, out);
            }

            compressionRatio = ((float)out.getFilePointer())/ (SIZE*BLOCK_SIZE*4)*100;
            out.close();
        }

        @Setup
        public void setup() throws IOException {
            init();

            generateMockData(SIZE, 18, outlierValue);

            encode(util, "test2.bin");
            decode(util, "test2.bin");

            if (!(util instanceof PForUtil)) {
                assertArrayEquals(flatMockData, tmpFlatOutput);
            } else {
                for (int i = 0; i < SIZE; i++) {
                    assertArrayEquals(mockData[i], tmpOutput[i]);
                }
            }
        }

        @TearDown
        public void tearDownDecodeFile() throws IOException {
            try {
                d.deleteFile("test2.bin");
            } catch (Exception e) {
                // ignore
            }

            System.out.println("Compression Ratio: "+ compressionRatio);
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
            util = new PForUtilV2(BLOCK_SIZE*SIZE);
        }
    }

    public static class VectorFastPFOR extends AbstractBenchmark {
        @Override
        public void init() {
            util = new PForUtilV3(BLOCK_SIZE*SIZE);
        }
    }

    public static class TurboPFOR extends AbstractBenchmark {
        @Override
        public void init() {
            util = new TurboPFORUtil(BLOCK_SIZE*SIZE);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntCompressionBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

