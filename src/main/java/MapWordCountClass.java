import javafx.scene.control.TextFormatter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MapWordCountClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    IntWritable oneValue = new IntWritable(1);

    Logger logger = LoggerFactory.getLogger(MapWordCountClass.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] line = value.toString().split("\\s");

        for (int i = 0; i < line.length; i++) {

            logger.info(line[i] + "     ======1======");

            word.set(line[i]);
            context.write(word, oneValue);
        }


    }
}
