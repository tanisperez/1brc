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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_tanisperez {
    private static final String FILE = "./measurements-1br.txt";

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

        MeasurementAggregator accumulate(Measurement measure) {
            this.max = Math.max(measure.value, this.max);
            this.min = Math.min(measure.value, this.min);
            this.sum += measure.value;
            this.count++;
            return this;
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

    public static void main(String[] args) throws IOException {
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                MeasurementAggregator::accumulate,
                MeasurementAggregator::combine,
                MeasurementAggregator::toResultRow);

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                .parallel()
                .map(Measurement::from)
                .collect(groupingBy(Measurement::station, collector)));

        System.out.println(measurements);
    }

}
