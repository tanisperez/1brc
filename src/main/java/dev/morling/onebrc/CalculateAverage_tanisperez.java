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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.nio.file.Files.newBufferedReader;
import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_tanisperez {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }

        public static Measurement from(final String line) {
            boolean readingValue = false;
            final StringBuilder station = new StringBuilder(line.length());
            final StringBuilder value = new StringBuilder(line.length());
            for (int i = 0; i < line.length(); i++) {
                final char character = line.charAt(i);
                if (character == ';') {
                    readingValue = true;
                    continue;
                }
                if (readingValue) {
                    value.append(character);
                } else {
                    station.append(character);
                }
            }
            return new Measurement(station.toString(), Double.parseDouble(value.toString()));
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
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        final int numberOfCores = Runtime.getRuntime().availableProcessors();
        var executorsService = Executors.newFixedThreadPool(numberOfCores + 1);

        final AtomicBoolean eof = new AtomicBoolean(false);
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        Map<String, MeasurementAggregator> results = new ConcurrentHashMap<>(10_000);

        Runnable producer = () -> {
            try (BufferedReader reader = newBufferedReader(Paths.get(FILE), StandardCharsets.UTF_8)) {
                for (;;) {
                    String line = reader.readLine();
                    if (line == null) {
                        eof.getAndSet(true);
                        break;
                    }
                    queue.add(line);
                }
            }
            catch (final Exception exception) {
                exception.printStackTrace();
            }
        };

        Runnable consumer = () -> {
            try {
                while (!eof.get()) {
                    final String line = queue.take();
                    final Measurement measurement = Measurement.from(line);

                    MeasurementAggregator measurementAggregator = new MeasurementAggregator();
                    measurementAggregator.min = measurement.value;
                    measurementAggregator.max = measurement.value;
                    measurementAggregator.sum = measurement.value;
                    measurementAggregator.count = 1;

                    results.merge(measurement.station, measurementAggregator,
                            (currentValue, newValue) -> {
                                MeasurementAggregator newMeasurementAggregator = new MeasurementAggregator();
                                newMeasurementAggregator.min = Math.min(currentValue.min, newValue.min);
                                newMeasurementAggregator.max = Math.max(currentValue.max, newValue.max);
                                newMeasurementAggregator.sum = currentValue.sum + newValue.sum;
                                newMeasurementAggregator.count = currentValue.count + 1;
                                return newMeasurementAggregator;
                            });
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        try {
            executorsService.submit(producer);
            for (int i = 0; i < numberOfCores; i++) {
                executorsService.submit(consumer);
            }
            executorsService.shutdown();
            executorsService.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (final Exception exception) {
            exception.printStackTrace();
        }

        Map<String, ResultRow> copy = results.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new ResultRow(e.getValue().min, (Math.round(e.getValue().sum * 10.0) / 10.0) / e.getValue().count, e.getValue().max))
                );

        TreeMap<String, ResultRow> out = new TreeMap<>();
        for (java.util.Map.Entry<String, ResultRow> result : copy.entrySet()) {
            out.put(result.getKey(), result.getValue());
        }

        System.out.println(out);
    }
}
