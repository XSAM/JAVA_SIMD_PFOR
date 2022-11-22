package io.dashbase.codec.utils;

import io.dashbase.codec.v2.FastPFOR128;
import me.lemire.integercompression.*;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;


// JavaFastPFOR's FastPFOR128
public class PForUtilV2 extends BasePForUtil {
    public static final int BLOCK_SIZE = 128;

    public FastPFOR128 fastPFOR128 = new FastPFOR128();
    public int[] outArr = new int[BLOCK_SIZE];

    public int[] intArr = new int[BLOCK_SIZE];
    public int[] compressedArr = new int[BLOCK_SIZE];
    IntegerCODEC codec = new FastPFOR();

    public IntWrapper inOffset = new IntWrapper(0);
    public IntWrapper outOffset = new IntWrapper(0);

    public static byte[] tempByte = new byte[4 * BLOCK_SIZE + 32];


    public PForUtilV2() {

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


    public void addInt(int v, int pos) {
        tempByte[pos] = (byte) v;
        tempByte[pos + 1] = (byte) (v >> 8);
        tempByte[pos + 2] = (byte) (v >> 16);
        tempByte[pos + 3] = (byte) (v >> 24);
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
