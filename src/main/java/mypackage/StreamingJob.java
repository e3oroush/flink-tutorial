/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mypackage;

import mypackage.events.Sensor;
import mypackage.sources.TemperatureSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// set time characteristic to be event timestamp
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// assign fictious stream of data as our data source and extract timestamp field
		DataStream<Sensor> inputStream = env.addSource(new TemperatureSensor()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Sensor>() {
			@Override
			public long extractAscendingTimestamp(Sensor sensor) {
				return sensor.getTimestamp();
			}
		});

		DataStream<Double> temps = inputStream.map(new MapFunction<Sensor, Double>() {
			@Override
			public Double map(Sensor sensor) throws Exception {
				return sensor.getTemperature();
			}
		});


		// define a simple pattern and condition to detecto from data stream
		Pattern<Sensor,?> highTempPattern = Pattern.<Sensor>begin("first").where(new SimpleCondition<Sensor>() {
			@Override
			public boolean filter(Sensor sensor) throws Exception {
				return sensor.getTemperature() > 60;
			}
		});
		// get resulted data stream from input data stream based on the defined CEP pattern
		DataStream<Sensor> result = CEP.pattern(inputStream.keyBy(new KeySelector<Sensor, Integer>() {
			@Override
			public Integer getKey(Sensor sensor) throws Exception {
				return sensor.getDeviceId();
			}
		}), highTempPattern).process(new PatternProcessFunction<Sensor, Sensor>() {
			@Override
			public void processMatch(Map<String, List<Sensor>> map, Context context, Collector<Sensor> collector) throws Exception {
				collector.collect(map.get("first").get(0));
			}
		});


		SingleOutputStreamOperator<Tuple3<Integer, Long, Integer>> aggregatedMatch = result.keyBy(new KeySelector<Sensor, Integer>() {
			@Override
			public Integer getKey(Sensor sensor) throws Exception {
				return sensor.getDeviceId();
			}
		}).window(TumblingProcessingTimeWindows.of(Time.minutes(1))).aggregate(new AggregateFunction<Sensor, Tuple3<Integer, Long, Integer>, Tuple3<Integer, Long, Integer>>() {
			@Override
			public Tuple3<Integer, Long, Integer> createAccumulator() {
				Tuple3<Integer, Long, Integer> acc = new Tuple3<>();
				acc.f0 = -1;
				return acc;
			}

			@Override
			public Tuple3<Integer, Long, Integer> add(Sensor sensor, Tuple3<Integer, Long, Integer> integerLongtuple2) {
				if(integerLongtuple2.f0 == -1){
					integerLongtuple2.f0 = sensor.getDeviceId();
					integerLongtuple2.f1 = sensor.getTimestamp();
					integerLongtuple2.f2 = 0;
				}
				integerLongtuple2.f2++;
				return integerLongtuple2;
			}

			@Override
			public Tuple3<Integer, Long, Integer> getResult(Tuple3<Integer, Long, Integer> integerLongtuple2) {
				return integerLongtuple2;
			}

			@Override
			public Tuple3<Integer, Long, Integer> merge(Tuple3<Integer, Long, Integer> integerLongtuple2, Tuple3<Integer, Long, Integer> acc1) {
				acc1.f2 += integerLongtuple2.f2;
				return acc1;
			}
		});

		DataStream<Tuple2<Integer,Long>> aggregatedResult = CEP.pattern(aggregatedMatch,Pattern.<Tuple3<Integer,Long, Integer>>begin("aggs").where(new SimpleCondition<Tuple3<Integer, Long, Integer>>() {
			@Override
			public boolean filter(Tuple3<Integer, Long, Integer> integerLongTuple3) throws Exception {
				return integerLongTuple3.f2 > 4;
			}
		})).process(new PatternProcessFunction<Tuple3<Integer, Long, Integer>, Tuple2<Integer, Long>>() {
			@Override
			public void processMatch(Map<String, List<Tuple3<Integer, Long, Integer>>> map, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
				Tuple2<Integer, Long> result = new Tuple2<>();
				result.f0 = map.get("aggs").get(0).f0;
				result.f1 = map.get("aggs").get(0).f1;
				collector.collect(result);
			}
		});

		aggregatedResult.print("Aggregated result");
//		result.print("result");
//		temps.print("temp_processed");
//		inputStream.print("input_stream");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
