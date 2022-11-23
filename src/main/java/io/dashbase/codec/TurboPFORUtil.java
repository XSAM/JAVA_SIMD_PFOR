package io.dashbase.codec;

import io.dashbase.codec.utils.BasePForUtil;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import powturbo.turbo.jic;

import java.io.IOException;

public class TurboPFORUtil extends BasePForUtil {
    public int[] data;
    public byte[] compressed;


    // The size of the input should not greater than the size of the buffer.
    public TurboPFORUtil(int bufferSize) {
        super(128);

        // Init buffer
        data = new int[bufferSize];
        compressed = new byte[bufferSize*4];
    }

    @Override
    public void encode(long[] longs, DataOutput out) throws IOException {
        convertLongToInt(longs, data);

        int length = jic.p4nenc32(data, longs.length, compressed);

        // Write length of bytes array
        out.writeInt(length);

        out.writeBytes(compressed, length);
    }

    @Override
    public void decode(DataInput in, long[] longs) throws IOException {
        final int length = in.readInt();

        in.readBytes(compressed, 0, length);
        jic.p4ndec32(compressed, longs.length, data);

        convertIntToLong(data, longs);
    }
}
