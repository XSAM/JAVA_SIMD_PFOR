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
        byte[] buffer;
        ByteArrayDataInput input;
        ByteArrayDataOutput output;
        PForUtil util = new PForUtil(new ForUtil());

        @Override
        public void init() {
        }

        @Override
        public void verify() throws IOException {
            buffer = new byte[SIZE*BLOCK_SIZE*8];
            input = new ByteArrayDataInput(buffer);
            output = new ByteArrayDataOutput(buffer);
            encode();
            decode();

            for (int i = 0; i < SIZE; i++) {
                assertArrayEquals(mockData[i], tmpOutput[i]);
            }
        }

        @Override
        public void encode() throws IOException {
            output.reset(buffer);

            for (int i = 0; i < SIZE; i++) {
                System.arraycopy(mockData[i], 0, tmpInput, 0, BLOCK_SIZE);
                util.encode(tmpInput, output);
            }
        }

        @Override
        public void decode() throws IOException {
            input.reset(buffer);

            for (int i = 0; i < SIZE; i++) {
                util.decode(input, tmpOutput[i]);
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
        byte[] compressed = new byte[BLOCK_SIZE*SIZE*4];
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
            jic.p4ndec32(compressed, uncompressed.length, uncompressed);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(IntCompressionRawBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

