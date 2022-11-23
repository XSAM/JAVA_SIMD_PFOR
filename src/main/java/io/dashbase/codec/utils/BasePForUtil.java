package io.dashbase.codec.utils;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

public abstract class BasePForUtil {
    public final int BLOCK_SIZE;

    private static final Unsafe theUnsafe;

    public void convertDataInputToInts(DataInput in, int length, byte[] buffer, int[] output) throws IOException {
        in.readBytes(buffer, 0, length);
        theUnsafe.copyMemory(buffer, Unsafe.ARRAY_BYTE_BASE_OFFSET, output, Unsafe.ARRAY_INT_BASE_OFFSET, length);
    }

    public void convertIntToLong(int[] input, long[] output) {
        for (int i = 0; i < input.length; i++) {
            output[i] = input[i];
        }
    }

    protected BasePForUtil(int block_size) {
        BLOCK_SIZE = block_size;
    }

    public void encode(long[] longs, DataOutput out) throws IOException{
        throw new RuntimeException("not implemented");
    }

    public void decode(DataInput in, long[] longs) throws IOException {

    }

    public void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {

    }

    public void skip(DataInput in) throws IOException {

    }

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
