package edu.reduce.map.fun;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class CSV2Parquet {
    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = configureJob("funWithMapReduce");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static Job configureJob(final String jobName) throws IOException {
        Job job = Job.getInstance(new Configuration(), jobName);
        job.setMapperClass(IdentityMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(ChessFileInputFormat.class);
        AvroParquetOutputFormat.setSchema(job, loadSchema());
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        job.setJarByClass(IdentityMapper.class);
        return job;
    }

    public static Schema loadSchema() throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return new Schema.Parser().parse(classLoader.getResourceAsStream("schema.avsc"));
    }
}
