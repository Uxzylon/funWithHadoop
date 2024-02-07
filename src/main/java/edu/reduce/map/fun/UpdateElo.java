package edu.reduce.map.fun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class UpdateElo extends Mapper<LongWritable, ChessGame, Text, IntWritable> {

    final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, ChessGame value, Context context) throws IOException, InterruptedException {
        if (value == null)
            return;

        context.write(
                new Text(String.format("%s %d | %s %d",
                        value.getWhite_id(),
                        computeElo(value.getWinner(), value.getWhite_rating(), value.getBlack_rating(), "white"),
                        value.getBlack_id(),
                        computeElo(value.getWinner(), value.getBlack_rating(), value.getWhite_rating(), "black"))
                ), ONE
        );
    }

    protected int computeElo(String winner, int rating, int opponentRating, String player) {
        final int K = 32;
        double expectedScore = 1 / (1 + Math.pow(10, (opponentRating - rating) / 400.0));
        double actualScore;

        if (winner.equals(player)) {
            actualScore = 1.0;
        } else if (winner.equals("draw")) {
            actualScore = 0.5;
        } else {
            actualScore = 0.0;
        }

        return rating + (int) (K * (actualScore - expectedScore));
    }
}
