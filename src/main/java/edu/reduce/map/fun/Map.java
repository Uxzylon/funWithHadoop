package edu.reduce.map.fun;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, ChessGame, Text, IntWritable> {
    final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, ChessGame value, Context context) throws IOException, InterruptedException {
        if (value == null)
            return;
        context.write(new Text(davidOrGoliath(value.getWinner(), value.getWhite_rating(), value.getBlack_rating())), ONE);
    }

    protected String davidOrGoliath(final String winner, final int whiteRating, final int blackRating) {
        if (blackRating == whiteRating && !winner.equals("draw")) {
            return "Other";
        }
        switch (winner) {
            case "black":
                return blackRating > whiteRating ? "Goliath" : "David";
            case "white":
                return whiteRating > blackRating ? "Goliath" : "David";
            default:
                return "Draw";
        }
    }
}
