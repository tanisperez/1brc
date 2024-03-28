/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Tests on my MacBook Pro 2020 with i5 and 16 GB of RAM.
 *
 * 1. baseline with zulu 21 -> 3:53
 * 2. my implementation with zulu 21 -> 2:02
 * 3. baseline with GraalVM 21.0.2 -> 3:33
 * 4. my implementation with GraalVM 21.0.2 -> 2:09
 * 5. native image -> 5:18
 * 6. Multithread implementation with zulu 21 -> 1:14
 * 7. Multithread implementation with GraalVM 21.0.2 -> 1:04
 * 8. Improved multithread implementation with GraalVM 21.0.2 -> 0:51
 */
public class CalculateAverage_tanisperez {
    private static final String FILE = "./measurements-1br.txt";

    private static final class Station {
        final byte[] buffer;
        final int hashCode;

        private Station(byte[] buffer) {
            this.buffer = buffer;
            this.hashCode = Arrays.hashCode(this.buffer);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Station station = (Station) o;
            return Arrays.equals(buffer, station.buffer);
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public String toString() {
            return new String(this.buffer);
        }
    }

    private static record Measurement(Station station, int value) {
        public static Measurement from(final byte[] line, final int length) {
            int separatorPosition = 0;
            while (line[separatorPosition] != ';') {
                separatorPosition++;
            }
            byte[] stationBuffer = new byte[separatorPosition];
            System.arraycopy(line, 0, stationBuffer, 0, separatorPosition);
            Station station = new Station(stationBuffer);

            byte[] value = new byte[length - separatorPosition - 2];
            for (int index = 0, i = separatorPosition + 1; i < length; i++) {
                final byte character = line[i];
                if (character != '.') {
                    value[index++] = character;
                }
            }

            final String measure = new String(value);
            return new Measurement(station, Integer.parseInt(measure));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private long min;
        private long max;
        private long sum;
        private long count;

        private MeasurementAggregator() {
        }

        private MeasurementAggregator(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        static MeasurementAggregator merge(MeasurementAggregator agg1, MeasurementAggregator agg2) {
            MeasurementAggregator result = new MeasurementAggregator();
            result.min = Math.min(agg1.min, agg2.min);
            result.max = Math.max(agg1.max, agg2.max);
            result.sum = agg1.sum + agg2.sum;
            result.count = agg1.count + agg2.count;
            return result;
        }

        ResultRow toResultRow() {
            final double min = this.min / 10.0;
            final double max = this.max / 10.0;
            final double sum = this.sum / 10.0;
            return new ResultRow(min, (Math.round(sum * 10.0) / 10.0) / this.count, max);
        }
    }

    private static List<MappedByteBuffer> splitFileInChunks(RandomAccessFile file, int numberOfCores) throws IOException {
        FileChannel fileChannel = file.getChannel();

        long fileSize = fileChannel.size();
        long chunkSize = fileSize / numberOfCores;

        List<MappedByteBuffer> chunks = new ArrayList<>((int) (fileSize / chunkSize) + 1);

        long chunkStart = 0L;
        while (chunkStart < fileSize) {
            long currentPosition = chunkStart + chunkSize;
            if (currentPosition < fileSize) {
                file.seek(currentPosition);
                while (file.read() != '\n') {
                    currentPosition++;
                }
                currentPosition++; // Skip the \n position
            }
            else {
                currentPosition = fileSize;
            }

            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStart, currentPosition - chunkStart);
            buffer.order(ByteOrder.nativeOrder());
            chunks.add(buffer);

            chunkStart = currentPosition;
        }

        return chunks;
    }

    private static final class ChunkWorker implements Runnable {
        private final MappedByteBuffer mappedByteBuffer;
        private final Map<Station, MeasurementAggregator> results;

        private ChunkWorker(MappedByteBuffer mappedByteBuffer, Map<Station, MeasurementAggregator> results) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.results = results;
        }

        @Override
        public void run() {
            byte[] buffer = new byte[255];
            while (mappedByteBuffer.position() < mappedByteBuffer.limit()) {
                int bufferLength = 0;

                byte byteRead = mappedByteBuffer.get();
                while (byteRead != '\n') {
                    buffer[bufferLength++] = byteRead;
                    byteRead = mappedByteBuffer.get();
                }

                final Measurement measurement = Measurement.from(buffer, bufferLength);

                results.merge(measurement.station, new MeasurementAggregator(measurement.value), MeasurementAggregator::merge);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int numberOfCores = Runtime.getRuntime().availableProcessors();

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        List<MappedByteBuffer> chunks = splitFileInChunks(file, numberOfCores);

        Thread[] workers = new Thread[numberOfCores];
        Map<Integer, Map<Station, MeasurementAggregator>> results = new HashMap<>(numberOfCores);
        for (int i = 0; i < numberOfCores; i++) {
            Map<Station, MeasurementAggregator> workerResults = new HashMap<>();
            results.put(Integer.valueOf(i), workerResults);

            workers[i] = new Thread(new ChunkWorker(chunks.get(i), workerResults));
            workers[i].start();
        }

        for (int i = 0; i < numberOfCores; i++) {
            workers[i].join();
        }

        Map<Station, MeasurementAggregator> accumulatedMeassures = new HashMap<>();
        for (Map<Station, MeasurementAggregator> meassures : results.values()) {
            for (java.util.Map.Entry<Station, MeasurementAggregator> entry : meassures.entrySet()) {
                accumulatedMeassures.merge(entry.getKey(), entry.getValue(), MeasurementAggregator::merge);
            }
        }

        Map<String, ResultRow> sortedResults = new TreeMap<>();
        for (java.util.Map.Entry<Station, MeasurementAggregator> entry: accumulatedMeassures.entrySet()) {
            sortedResults.put(entry.getKey().toString(), entry.getValue().toResultRow());
        }

        // Map<Station, MeasurementAggregator> accumulatedMeasures = results.values().stream()
        //     .flatMap(measures -> measures.entrySet().stream())
        //     .collect(Collectors.toMap(
        //         Map.Entry::getKey,
        //         Map.Entry::getValue,
        //         MeasurementAggregator::merge
        //     ));

        // Map<String, ResultRow> sortedResults = accumulatedMeasures.entrySet().stream()
        //     .collect(Collectors.toMap(
        //         entry -> entry.getKey().toString(),
        //         entry -> entry.getValue().toResultRow(),
        //         (existing, replacement) -> existing,
        //         TreeMap::new
        //     ));

        System.out.println(sortedResults);
    }

}
