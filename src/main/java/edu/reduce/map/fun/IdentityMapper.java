package edu.reduce.map.fun;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IdentityMapper extends Mapper<LongWritable, ChessGame, Void, GenericRecord> {
    @Override
    protected void map(LongWritable key, ChessGame value, Context context) throws IOException, InterruptedException {
        if (value == null || ChessGames.EMPTY.equals(value))
            return;
        context.write(null, ChessGames.record(value));
    }
}
