package com.xiaoxiang.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/*
 * without implements checkpoint
 *
 * FlatMapFunction 为无状态函数
 *
 * RichFlatMapFunction 为有状态函数，因为可以获取getRuntimeContext().getstate
 *
 * KeyedState 数据value list map的状态都是传到本地rocksDb（因为会有OperatorState*key个数的state，内存翻倍存放不行）
 * OperatorState写进本地内存 适合保存不是很大的状态场景
 */
public class CountWithKeyedState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state  UPdate rocksDB
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 3) {
            out.collect(new Tuple2<Long,Long>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

/** function触发的时候会调用一次*/
    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){})); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);

        /*如果是implement CheckpointedFunction*/
//	    functionInitializationContext.getKeyedStateStore().getState(listStateDescriptor);

    }
}