package io.dashbase.codec.utils;

import io.dashbase.codec.v3.VectorFastPFOR;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;


public class PForUtilV3 extends BasePForUtil {
    public static final int BLOCK_SIZE = 256;
    public int[] outArr = new int[BLOCK_SIZE];

    public VectorFastPFOR vectorFastPFOR = new VectorFastPFOR();

    public int[] intArr = new int[BLOCK_SIZE];
    public int[] compressedArr = new int[BLOCK_SIZE];

    public IntWrapper inOffset = new IntWrapper(0);
    public IntWrapper outOffset = new IntWrapper(0);

    IntegerCODEC codec = new VectorFastPFOR();

    public static byte[] tempByte = new byte[4 * BLOCK_SIZE + 32];


    public PForUtilV3() {

        super(BLOCK_SIZE);
    }

    @Override
    public void encode(long[] longs, DataOutput out) throws IOException {
        inOffset.set(0);
        outOffset.set(0);
        // TODO: fix this
        int[] data = new int[longs.length];
        int[] compressed = new int[longs.length];
        for (int i = 0; i < longs.length; i++) {
            data[i] = (int) longs[i];
        }
        codec.compress(data, inOffset, data.length, compressed, outOffset);

        for (int i = 0; i < outOffset.intValue(); i++) {
            out.writeInt(compressed[i]);
        }
    }

    public int readInt(int pos) {
        var v = tempByte[pos] & 0xFF;
        v |= (tempByte[pos + 1] & 0xFF) << 8;
        v |= (tempByte[pos + 2] & 0xFF) << 16;
        v |= (tempByte[pos + 3] & 0xFF) << 24;
        return v;
    }

    @Override
    public void decode(DataInput in, long[] longs) throws IOException {
        inOffset.set(0);
        outOffset.set(0);

        // TODO: fix this
        int[] compressed = new int[longs.length];
        int[] output = new int[longs.length];
        try {
            in.readInts(compressed, 0, compressed.length);
        } catch (java.lang.IndexOutOfBoundsException e) {
            // Ignore
        }

        codec.uncompress(compressed, inOffset, compressed.length, output, outOffset);
        for (int i = 0; i < compressed.length; i++) {
            longs[i] = output[i];
        }
    }

    @Override
    public void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
        decode(in, longs);
        longs[0] += base;
        for (int i = 1; i < BLOCK_SIZE; i++) {
            longs[i] += longs[i - 1];
        }
    }

    @Override
    public void skip(DataInput in) throws IOException {
        var len = in.readInt() & 0xFFFFFFFFL;

        in.skipBytes( len * 4);
    }
}
