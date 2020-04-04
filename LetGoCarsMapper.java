// cc LetGoCarsMapper Mapper for Cars
// vv LetGoCarsMapper 

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetGoCarsMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable> {

    
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString().toLowerCase();
    String[] cars = {"acura","audi","bmw","buick","cadillac","chevrolet","chrysler","dodge","ferrari",
		"fiat","fisker","ford","gmc","honda","hyundai","infiniti","jaguar","jeep","kia",
		"land rover","lexus","lincoln","maserati","mazda","mercedes-benz","mini","mitsubishi",
		"mustang","nissan","porsche","rolls royce","scion","subaru","suzuki","tesla","toyota",
		"volkswagon","volvo"}; 
    
    for (String car : cars) {
	int price = 0;
	if (line.contains(car)) {
		String[] str = line.split(",");
		int length = str.length;
		String price_txt = str[length-4];

		price = Integer.valueOf(price_txt);
		context.write(new Text(car), new IntWritable(price)); 
				}
  			    }
					       }
							}
// ^^ LetGoCarsMapper 