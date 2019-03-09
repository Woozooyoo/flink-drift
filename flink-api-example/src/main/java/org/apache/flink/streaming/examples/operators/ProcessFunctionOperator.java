package org.apache.flink.streaming.examples.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionOperator {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// the source data stream
		DataStream<Tuple2<String, String>> stream = env.addSource(new DataSource ());

		// apply the process function onto a keyed stream
		DataStream<Tuple2<String, Long>> results = stream
				.keyBy(0)
				.process(new CountWithTimeoutFunction());

		env.execute("Window example");
	}

}

/**
 * The data type stored in the state
 */
class CountWithTimestamp {

	public String key;
	public long count;
	public long lastModified;
}

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

	/** The state that is maintained by this process function */
	private ValueState<CountWithTimestamp> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
	}

	@Override
	public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out)
			throws Exception {

		// retrieve the current count
		CountWithTimestamp current = state.value();
		if (current == null) {
			current = new CountWithTimestamp();
			current.key = value.f0;
		}

		// update the state's count
		current.count++;

		// set the state's timestamp to the record's assigned event time timestamp
		current.lastModified = ctx.timestamp();

		// write the state back
		state.update(current);

		// schedule the next timer 60 seconds from the current event time
		ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
			throws Exception {

		// get the state for the key that scheduled the timer
		CountWithTimestamp result = state.value();

		// check if this is an outdated timer or the latest timer
		if (timestamp == result.lastModified + 60000) {
			// emit the state on timeout
			out.collect(new Tuple2<String, Long>(result.key, result.count));
		}
	}
}




/**
 * Parallel data source that serves a list of key-value pairs.
 */
class DataSource extends RichParallelSourceFunction<Tuple2<String, String>> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

		final long startTime = System.currentTimeMillis();

		final long numElements = 20000000;
		final long numKeys = 10000;
		long val = 1L;
		long count = 0L;

		while (running && count < numElements) {
			count++;
			ctx.collect(new Tuple2<>(String.valueOf(val++), "1"));

			if (val > numKeys) {
				val = 1L;
			}
		}

		final long endTime = System.currentTimeMillis();
		System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
	}

	@Override
	public void cancel() {
		running = false;
	}
}
