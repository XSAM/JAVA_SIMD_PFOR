package io.dashbase.codec.utils;

import io.dashbase.codec.v2.FastPFOR128;
import me.lemire.integercompression.*;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;


// JavaFastPFOR's FastPFOR128
public class PForUtilV2 extends BasePForUtil {
    public static final int BLOCK_SIZE = 128;

    public int[] data;
    public int[] compressed;
    public byte[] compressedBytes;

    IntegerCODEC codec = new FastPFOR();

    public IntWrapper inOffset = new IntWrapper(0);
    public IntWrapper outOffset = new IntWrapper(0);

    public static byte[] tempByte = new byte[4 * BLOCK_SIZE + 32];


    // The size of the input should not greater than the size of the buffer.
    public PForUtilV2(int bufferSize) {
        super(BLOCK_SIZE);

        // Init buffer
        data = new int[bufferSize];
        compressed = new int[bufferSize];
        compressedBytes = new byte[bufferSize*4];
    }

    @Override
    public void encode(long[] longs, DataOutput out) throws IOException {
        inOffset.set(0);
        outOffset.set(0);

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

        int length;
        // TODO: this is a hack
        if (in instanceof ByteBuffersIndexInput) {
            length = (int) ((ByteBuffersIndexInput) in).length();
        } else {
            length = longs.length;
        }

        convertDataInputToInts(in, length, compressedBytes, compressed);

        codec.uncompress(compressed, inOffset, length/4, data, outOffset);

        convertIntToLong(data, longs);
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
