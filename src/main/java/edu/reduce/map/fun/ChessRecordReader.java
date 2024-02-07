package edu.reduce.map.fun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class ChessRecordReader extends RecordReader<LongWritable, ChessGame> {

    LineRecordReader lineRecordReader = new LineRecordReader();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        lineRecordReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    @Override
    public ChessGame getCurrentValue() throws IOException, InterruptedException {
        if ( getCurrentKey().get() == 0) {
            return null;
        }

        return ChessGame.of(lineRecordReader.getCurrentValue());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
