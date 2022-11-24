package io.dashbase.local;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BaseBenchmark {
    int BLOCK_SIZE = 128;
    int SIZE = 10240;

    long[][] mockData;
    long[] tmpInput = new long[BLOCK_SIZE];
    long[][] tmpOutput = new long[SIZE][BLOCK_SIZE];

    long[] flatMockData;
    long[] tmpFlatOutput = new long[BLOCK_SIZE * SIZE];

    int[] flatMockIntData;

    public void generateMockData(int size, int commonBit, boolean outlierValue) {
        int largeBit = commonBit + 5;
        int maxBit = commonBit + 10;

        mockData = new long[size][BLOCK_SIZE];
        flatMockData = new long[size*BLOCK_SIZE];
        flatMockIntData = new int[size*BLOCK_SIZE];
        Random random = ThreadLocalRandom.current();
        int k = 0;
        int tmp;
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < BLOCK_SIZE; j++) {
                if (outlierValue) {
                    if (k%5==0) {
                        // Throwing some large values
                        tmp = random.nextInt(largeBit);
                    } else if (k%533 ==0) {
                        // Throwing some large values
                        tmp = random.nextInt(maxBit);
                    } else {
                        // Normal value
                        tmp = random.nextInt(commonBit);
                    }
                } else {
                    // Normal value
                    tmp = random.nextInt(commonBit);
                }

                mockData[i][j] = tmp;
                flatMockData[k] = mockData[i][j];
                flatMockIntData[k] = (int) mockData[i][j];
                k++;
            }
        }
    }
}
