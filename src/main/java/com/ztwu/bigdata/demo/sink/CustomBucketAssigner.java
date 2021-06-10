package com.ztwu.bigdata.demo.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@PublicEvolving
public class CustomBucketAssigner<IN> implements BucketAssigner<IN, String> {

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

	private final   String formatString;

	private final ZoneId zoneId;

	private transient DateTimeFormatter dateTimeFormatter;

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"}.
	 */
	public CustomBucketAssigner() {
		this(DEFAULT_FORMAT_STRING);
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string.
	 *
	 * @param formatString The format string that will be given to {@code SimpleDateFormat} to determine
	 *                     the bucket id.
	 */
	public CustomBucketAssigner(String formatString) {
		this(formatString, ZoneId.systemDefault());
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"} using the given timezone.
	 *
	 * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
	 */
	public CustomBucketAssigner(ZoneId zoneId) {
		this(DEFAULT_FORMAT_STRING, zoneId);
	}

	/**
	 * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string using the given timezone.
	 *
	 * @param formatString The format string that will be given to {@code DateTimeFormatter} to determine
	 *                     the bucket path.
	 * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
	 */
	public CustomBucketAssigner(String formatString, ZoneId zoneId) {
		this.formatString = Preconditions.checkNotNull(formatString);
		this.zoneId = Preconditions.checkNotNull(zoneId);
	}

	//将分桶的规则写成按照事件时间；
	@Override
	public String getBucketId(IN element, BucketAssigner.Context context) {
		if (dateTimeFormatter == null) {
			dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
		}
		//固定格式命名文件夹名称
		return "p_data_day="+dateTimeFormatter.format(Instant.ofEpochMilli(context.timestamp()));
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "DateTimeBucketAssigner{" +
				"formatString='" + formatString + '\'' +
				", zoneId=" + zoneId +
				'}';
	}

}