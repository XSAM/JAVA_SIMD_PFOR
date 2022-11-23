package io.dashbase.local;

import io.dashbase.codec.utils.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
public class PForUtilBenchmark {

    int BLOCK_SIZE = 256;
    int SIZE = 5120;
    long[][] mockData;
    final Directory d = new ByteBuffersDirectory();
    long[] tmpInput = new long[BLOCK_SIZE];
    long[] outArr = new long[BLOCK_SIZE];
    private PForUtil luceneUtil;
    private PForUtilV2 fastPFORUtil;
    BasePForUtil vectorFastPFORUtil;

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

    @Setup(Level.Trial)
    public void setup() throws IOException {
        mockData = mockData(SIZE, 18);

        luceneUtil = new PForUtil(new ForUtil());
        fastPFORUtil = new PForUtilV2(BLOCK_SIZE*SIZE);
        vectorFastPFORUtil = new PForUtilV3(BLOCK_SIZE*SIZE);

        encode(luceneUtil, "lucene_test.bin");
        encode(fastPFORUtil, "fastpfor_test.bin");
        encode(vectorFastPFORUtil, "vector_fastpfor_test.bin");
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        try {
            d.deleteFile("test.bin");
        } catch (Exception e) {
            // ignore
        }
    }

    @Benchmark
    public void test_lucene_encode() throws IOException {
        encode(luceneUtil, "test.bin");
    }

    @Benchmark
    public void test_fastpfor_encode() throws IOException {
        encode(fastPFORUtil, "test.bin");
    }

    @Benchmark
    public void test_vector_fastpfor_encode() throws IOException {
        encode(vectorFastPFORUtil, "test.bin");
    }


    @Benchmark
    public void test_lucene_decode() throws IOException {
        decode(luceneUtil, "lucene_test.bin");
    }

    @Benchmark
    public void test_fastpfor_decode() throws IOException {
        decode(fastPFORUtil, "fastpfor_test.bin");
    }

    @Benchmark
    public void test_vector_fastpfor_decode() throws IOException {
        decode(vectorFastPFORUtil, "vector_fastpfor_test.bin");
    }

    public void decode(BasePForUtil util, String fileName) throws IOException {
        var in = d.openInput(fileName, IOContext.DEFAULT);
        for (int i = 0; i < SIZE; i++) {
            util.decode(in, outArr);
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
}
