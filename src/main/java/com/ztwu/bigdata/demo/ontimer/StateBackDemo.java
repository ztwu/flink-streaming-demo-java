package com.ztwu.bigdata.demo.ontimer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Collection;
import java.util.Random;

/**
 * 只有keyedStream在使用ProcessFunction时可以使用State和Timer定时器
 */
public class StateBackDemo {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//		env.setParallelism(10);
		//1000,hello
//		DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

		bsEnv.enableCheckpointing(3*60000);
		//设置statebackend存储方式
		RocksDBStateBackend stateBackend = new RocksDBStateBackend("file:///flink/checkpoint2", true);
//		RocksDBStateBackend stateBackend = new RocksDBStateBackend("hdfs://nameservice1/user/admin/user/checkpoint2", true);
		//默认情况下将使用 tmp 目录。理想情况下，这应该是最快的可用磁盘
//        stateBackend.setDbStoragePath("");
		stateBackend.setRocksDBOptions(new ConfigurableRocksDBOptionsFactory() {
			private static final long DEFAULT_SIZE = 256 * 1024 * 1024;  // 256 MB
			private long blockCacheSize = DEFAULT_SIZE;

			@Override
			public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions.setIncreaseParallelism(4)
						.setUseFsync(false);
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(
					ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions.setTableFormatConfig(
						new BlockBasedTableConfig()
								.setBlockCacheSize(blockCacheSize)     // 256 MB
								.setBlockSize(128 * 1024));            // 128 KB
			}

			@Override
			public RocksDBOptionsFactory configure(ReadableConfig configuration) {
				return null;
			}
		});

		//        StateBackend stateBackend = new FsStateBackend("file:///flink/checkpoint");
		bsEnv.setStateBackend(stateBackend);
		bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		//3. 设置重启策略
//        如果没有启用 checkpointing，则使用无重启 (no restart) 策略。
//        如果启用了 checkpointing，但没有配置重启策略，则使用固定间隔 (fixed-delay) 策略，其中 Integer.MAX_VALUE 参数是尝试重启次数
		//3.1 固定间隔重启：最多重启五次，重启间隔2000毫秒
//        bsEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,2000));
		//3.2 失败率：failureRate是每个测量时间间隔最大失败次数
		//第二个参数failureInterval失败率测量的时间间隔;
		// 第三个参数delayInterval是两次连续重启尝试的时间间隔
//        bsEnv.setRestartStrategy(RestartStrategies.failureRateRestart(5,
//                Time.of(5, TimeUnit.MINUTES),Time.of(10, TimeUnit.SECONDS)));
		//3.3 无重启
//        bsEnv.setRestartStrategy(RestartStrategies.noRestart());
		bsEnv.setRestartStrategy(RestartStrategies.fallBackRestart());

		// 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
//        用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint
		bsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
		// 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
		bsEnv.getCheckpointConfig().setCheckpointTimeout(2*60000);
		// 同一时间只允许进行一个检查点
		bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// 最多允许 checkpoint 失败 3 次
		bsEnv.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
		// 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
		//ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
		//ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
		bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<String> lines = bsEnv.addSource(new RichSourceFunction<String>() {

			private static final long serialVersionUID = 1L;
			boolean flag = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (flag) {
					int n = new Random().nextInt(4);
					if(n==1){
						ctx.collect(System.currentTimeMillis()+","+"ABCDEa123abc");
					}else if(n==2){
						ctx.collect(System.currentTimeMillis()+","+"ABCDFB123abc");
					}else if(n==3){
						ctx.collect(System.currentTimeMillis()+","+"999999999999");
					}else if(n==0){
						ctx.collect(System.currentTimeMillis()+","+"ztwuwwwwwwww");
					}
					Thread.sleep(1000);
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String line) throws Exception {
				String word = line.split(",")[1];
				return Tuple2.of(word, 1);
			}
		});

		//调用keyBy进行分组
		KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne
				.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
					@Override
					public String getKey(Tuple2<String, Integer> value) throws Exception {
//						return value.f0;
						return new StringBuffer(value.f0).reverse().toString().substring(0,2);
					}
				});

		//没有划分窗口，直接调用底层的process方法
		keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			private transient MapState<String, Integer> bufferState;
			// 定义状态描述器
			@Override
			public void open(Configuration parameters) throws Exception {
				MapStateDescriptor<String, Integer> listStateDescriptor = new MapStateDescriptor<>(
						"mapstate", String.class, Integer.class);
				bufferState = getRuntimeContext().getMapState(listStateDescriptor);
			}

			@Override
			public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
				String nkey = ctx.getCurrentKey();
				String key = value.f0;
				System.out.println(nkey+" : "+key);
				bufferState.put(key,1);

				System.out.println(bufferState.keys());
				out.collect(value);
			}
		}).print();

		bsEnv.execute();


	}
}
