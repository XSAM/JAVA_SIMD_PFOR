package io.dashbase.local;

import io.dashbase.codec.utils.*;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import org.apache.lucene.store.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import powturbo.turbo.jic;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

// Directly invoke encode/decode functions without any further calculation for the input/output data.
public class IntCompressionRawBenchmark {
    @State(Scope.Thread)
    @Warmup(iterations = 5, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(3)
    @BenchmarkMode(Mode.Throughput)
    public static abstract class AbstractBenchmark extends BaseBenchmark {
        @Param({"true"})
        public boolean outlierValue;

        public abstract void init();

        public abstract void verify() throws IOException;

        @Setup
        public void setup() throws IOException {
            init();

            generateMockData(SIZE, 18, outlierValue);

            verify();
        }

        @Benchmark
        public void encode() throws IOException {
        }

        @Benchmark
        public void decode() throws IOException {

        }
    }

    public static class Lucene extends AbstractBenchmark {
        final Directory d = new ByteBuffersDirectory();
        PForUtil util = new PForUtil(new ForUtil());

        String tmpFilename = "test.bin";

        @Override
        public void init() {
        }

        @Override
        public void verify() throws IOException {
            tmpFilename = "test2.bin";
            encode();
            decode();

            for (int i = 0; i < SIZE; i++) {
                assertArrayEquals(mockData[i], tmpOutput[i]);
            }

            tmpFilename = "test.bin";
        }

        @Override
        public void encode() throws IOException {
            IndexOutput out = d.createOutput(tmpFilename, IOContext.DEFAULT);

            for (int i = 0; i < SIZE; i++) {
                System.arraycopy(mockData[i], 0, tmpInput, 0, BLOCK_SIZE);
                util.encode(tmpInput, out);
            }
            out.close();
        }

        @Override
        public void decode() throws IOException {
            var in = d.openInput("test2.bin", IOContext.DEFAULT);

            for (int i = 0; i < SIZE; i++) {
                util.decode(in, tmpOutput[i]);
            }
            in.close();
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            try {
                d.deleteFile(tmpFilename);
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public static class FastPFOR extends AbstractBenchmark {
        IntegerCODEC codec = new me.lemire.integercompression.FastPFOR();
        public IntWrapper inOffset = new IntWrapper(0);
        public IntWrapper outOffset = new IntWrapper(0);

        int[] compressed = new int[BLOCK_SIZE*SIZE];
        int[] uncompressed = new int[BLOCK_SIZE*SIZE];

        @Override
        public void init() {
        }

        @Override
        public void verify() throws IOException {
            encode();
            decode();

            assertArrayEquals(flatMockIntData, uncompressed);
        }

        @Override
        public void encode() throws IOException {
            inOffset.set(0);
            outOffset.set(0);

            codec.compress(flatMockIntData, inOffset, flatMockIntData.length, compressed, outOffset);
        }

        @Override
        public void decode() throws IOException {
            final int length = outOffset.intValue();
            inOffset.set(0);
            outOffset.set(0);

            codec.uncompress(compressed, inOffset, length, uncompressed, outOffset);
        }
    }

    public static class VectorFastPFOR extends FastPFOR {
        @Override
        public void init() {
            codec = new me.lemire.integercompression.vector.VectorFastPFOR();
        }
    }

    public static class TurboPFOR extends AbstractBenchmark {
        byte[] compressed = new byte[BLOCK_SIZE*SIZE];
        int[] uncompressed = new int[BLOCK_SIZE*SIZE];
        int compressedLength;

        @Override
        public void init() {
        }

        @Override
        public void verify() throws IOException {
            encode();
            decode();

            assertArrayEquals(flatMockIntData, uncompressed);
        }

        @Override
        public void encode() throws IOException {
            compressedLength = jic.p4nenc32(flatMockIntData, flatMockIntData.length, compressed);
        }

        @Override
        public void decode() throws IOException {
            jic.p4ndec32(compressed, compressedLength, uncompressed);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntCompressionRawBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

