import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountImpl extends Configured implements Tool {

    Logger logger = LoggerFactory.getLogger(WordCountImpl.class);

    public static void main(String[] args) throws Exception {



        int exitCode = ToolRunner.run(new WordCountImpl(),args);
        System.exit(exitCode);
    }


    public int run(String[] strings) throws Exception {

        if (strings.length != 2) {
            logger.info("Usage: %s needs two arguments <input> <output> files\n",
                    getClass().getSimpleName());
            return -1;
        }

        logger.info("========Started=====");

        Job job=new Job();
        job.setJobName("WordCountImpl");
        job.setJarByClass(WordCountImpl.class);


        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        job.setMapperClass(MapWordCountClass.class);
        job.setReducerClass(ReducerWordCountClass.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
