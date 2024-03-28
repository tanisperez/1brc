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
import java.util.stream.Collectors;

/**
 * Tests on my MacBook Pro 2020 with i5 and 16 GB of RAM.
 *
 * 1. baseline with zulu 21 -> 3:53
 * 2. my implementation with zulu 21 -> 2:02
 * 3. baseline with GraalVM 21.0.2 -> 3:33
 * 4. my implementation with GraalVM 21.0.2 -> 2:09
 * 5. native image -> 5:18
 */
public class CalculateAverage_tanisperez {
    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, int value) {
        public static Measurement from(final String line) {
            int separatorPosition = 0;
            while (line.charAt(separatorPosition) != ';') {
                separatorPosition++;
            }
            String station = line.substring(0, separatorPosition);

            int lineLength = line.length();
            char[] value = new char[lineLength - separatorPosition - 2];
            for (int index = 0, i = separatorPosition + 1; i < lineLength; i++) {
                final char character = line.charAt(i);
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
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private long sum;
        private long count;

        static MeasurementAggregator from(Measurement measurement) {
            final MeasurementAggregator measurementAggregator = new MeasurementAggregator();
            measurementAggregator.min = measurement.value;
            measurementAggregator.max = measurement.value;
            measurementAggregator.sum = measurement.value;
            measurementAggregator.count = 1;
            return measurementAggregator;
        }

        static MeasurementAggregator combine(MeasurementAggregator agg1, MeasurementAggregator agg2) {
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

        private MappedByteBuffer mappedByteBuffer;
        private Map<String, MeasurementAggregator> results;

        public ChunkWorker(MappedByteBuffer mappedByteBuffer, Map<String, MeasurementAggregator> results) {
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

                final String line = new String(buffer, 0, bufferLength);
                final Measurement measurement = Measurement.from(line);

                results.merge(measurement.station, MeasurementAggregator.from(measurement), MeasurementAggregator::combine);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int numberOfCores = Runtime.getRuntime().availableProcessors();

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        List<MappedByteBuffer> chunks = splitFileInChunks(file, numberOfCores);

        Thread[] workers = new Thread[numberOfCores];
        Map<Integer, Map<String, MeasurementAggregator>> results = new HashMap<>(numberOfCores);
        for (int i = 0; i < numberOfCores; i++) {
            Map<String, MeasurementAggregator> workerResults = new HashMap<>();
            results.put(Integer.valueOf(i), workerResults);

            workers[i] = new Thread(new ChunkWorker(chunks.get(i), workerResults));
            workers[i].start();
        }

        for (int i = 0; i < numberOfCores; i++) {
            workers[i].join();
        }

        Map<String, MeasurementAggregator> accumulatedMeassures = new HashMap<>();
        for (Map<String, MeasurementAggregator> meassures : results.values()) {
            for (java.util.Map.Entry<String, MeasurementAggregator> entry : meassures.entrySet()) {
                accumulatedMeassures.merge(entry.getKey(), entry.getValue(), MeasurementAggregator::combine);
            }
        }

        Map<String, ResultRow> accumulatedMeasures = new TreeMap<>(
                accumulatedMeassures.entrySet().stream()
                        .map(e -> new AbstractMap.SimpleEntry<String, ResultRow>(e.getKey(), e.getValue().toResultRow()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        System.out.println(accumulatedMeasures);
    }

}
