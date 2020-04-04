// cc LetGoCarsReducer Reducer for Cars Procing
// vv LetGoCarsReducer 

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetGoCarsReducer 
  extends Reducer<Text, IntWritable, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;
    int count = 0, sum = 0;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
      minValue = Math.min(minValue, value.get());
      sum += value.get();
      count++;		
    }
    double avgValue = sum/(double)(count);
    String str = "Min. price: " + minValue + ", Avg Price: " + avgValue + ", Max price: " + maxValue;
    context.write(key, new Text(str));
  }
}
// ^^ LetGoCarsReducer 