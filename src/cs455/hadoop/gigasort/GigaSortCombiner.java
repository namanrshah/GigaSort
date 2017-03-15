package cs455.hadoop.gigasort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs. Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class GigaSortCombiner extends
		Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
	// int count = 1;
	NullWritable nullWritable = NullWritable.get();

	@Override
	protected void reduce(LongWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		for (NullWritable val : values) {
			context.write(key, nullWritable);
		}
	}
}
