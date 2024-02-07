package edu.reduce.map.fun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChessGame implements Writable {

    Text id;
    Text rated;
    Text created_at;
    Text last_move_at;
    Text turns;
    Text victory_status;
    Text winner;
    Text increment_code;
    Text white_id;
    IntWritable white_rating;
    Text black_id;
    IntWritable black_rating;
    Text moves;
    Text opening_eco;
    Text opening_name;
    Text opening_ply;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        rated.write(dataOutput);
        created_at.write(dataOutput);
        last_move_at.write(dataOutput);
        turns.write(dataOutput);
        victory_status.write(dataOutput);
        winner.write(dataOutput);
        increment_code.write(dataOutput);
        white_id.write(dataOutput);
        white_rating.write(dataOutput);
        black_id.write(dataOutput);
        black_rating.write(dataOutput);
        moves.write(dataOutput);
        opening_eco.write(dataOutput);
        opening_name.write(dataOutput);
        opening_ply.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        rated.readFields(dataInput);
        created_at.readFields(dataInput);
        last_move_at.readFields(dataInput);
        turns.readFields(dataInput);
        victory_status.readFields(dataInput);
        winner.readFields(dataInput);
        increment_code.readFields(dataInput);
        white_id.readFields(dataInput);
        white_rating.readFields(dataInput);
        black_id.readFields(dataInput);
        black_rating.readFields(dataInput);
        moves.readFields(dataInput);
        opening_eco.readFields(dataInput);
        opening_name.readFields(dataInput);
        opening_ply.readFields(dataInput);
    }

    public static ChessGame of(Text t) {
        String[] lines = t.toString().split(",");
        ChessGame c = new ChessGame();
        try {
            c.id = new Text(lines[0]);
            c.rated = new Text(lines[1]);
            c.created_at = new Text(lines[2]);
            c.last_move_at = new Text(lines[3]);
            c.turns = new Text(lines[4]);
            c.victory_status = new Text(lines[5]);
            c.winner = new Text(lines[6]);
            c.increment_code = new Text(lines[7]);
            c.white_id = new Text(lines[8]);
            c.white_rating = new IntWritable(Integer.parseInt(lines[9]));
            c.black_id = new Text(lines[10]);
            c.black_rating = new IntWritable(Integer.parseInt(lines[11]));
            c.moves = new Text(lines[12]);
            c.opening_eco = new Text(lines[13]);
            c.opening_name = new Text(lines[14]);
            c.opening_ply = new Text(lines[15]);
        } catch (NumberFormatException ignored) {}

        return c;
    }

    public String getId() {
        return id.toString();
    }

    public void setId(Text id) {
        this.id = id;
    }

    public String getRated() {
        return rated.toString();
    }

    public void setRated(Text rated) {
        this.rated = rated;
    }

    public String getCreated_at() {
        return created_at.toString();
    }

    public void setCreated_at(Text created_at) {
        this.created_at = created_at;
    }

    public String getLast_move_at() {
        return last_move_at.toString();
    }

    public void setLast_move_at(Text last_move_at) {
        this.last_move_at = last_move_at;
    }

    public String getTurns() {
        return turns.toString();
    }

    public void setTurns(Text turns) {
        this.turns = turns;
    }

    public String getVictory_status() {
        return victory_status.toString();
    }

    public void setVictory_status(Text victory_status) {
        this.victory_status = victory_status;
    }

    public String getWinner() {
        return winner.toString();
    }

    public void setWinner(Text winner) {
        this.winner = winner;
    }

    public String getIncrement_code() {
        return increment_code.toString();
    }

    public void setIncrement_code(Text increment_code) {
        this.increment_code = increment_code;
    }

    public String getWhite_id() {
        return white_id.toString();
    }

    public void setWhite_id(Text white_id) {
        this.white_id = white_id;
    }

    public int getWhite_rating() {
        return white_rating.get();
    }

    public void setWhite_rating(IntWritable white_rating) {
        this.white_rating = white_rating;
    }

    public String getBlack_id() {
        return black_id.toString();
    }

    public void setBlack_id(Text black_id) {
        this.black_id = black_id;
    }

    public int getBlack_rating() {
        return black_rating.get();
    }

    public void setBlack_rating(IntWritable black_rating) {
        this.black_rating = black_rating;
    }

    public String getMoves() {
        return moves.toString();
    }

    public void setMoves(Text moves) {
        this.moves = moves;
    }

    public String getOpening_eco() {
        return opening_eco.toString();
    }

    public void setOpening_eco(Text opening_eco) {
        this.opening_eco = opening_eco;
    }

    public String getOpening_name() {
        return opening_name.toString();
    }

    public void setOpening_name(Text opening_name) {
        this.opening_name = opening_name;
    }

    public String getOpening_ply() {
        return opening_ply.toString();
    }

    public void setOpening_ply(Text opening_ply) {
        this.opening_ply = opening_ply;
    }
}
