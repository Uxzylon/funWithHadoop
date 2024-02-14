package edu.reduce.map.fun;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;

import static edu.reduce.map.fun.CSV2Parquet.loadSchema;

public class ChessGames {
    public static final ChessGame EMPTY = new ChessGame();
    public static Record record(ChessGame chessGame) throws IOException {
        Record record = new Record(loadSchema());
        record.put("id", chessGame.getId());
        record.put("rated", chessGame.getRated());
        record.put("created_at", chessGame.getCreated_at());
        record.put("last_move_at", chessGame.getLast_move_at());
        record.put("turns", chessGame.getTurns());
        record.put("victory_status", chessGame.getVictory_status());
        record.put("winner", chessGame.getWinner());
        record.put("increment_code", chessGame.getIncrement_code());
        record.put("white_id", chessGame.getWhite_id());
        record.put("white_rating", chessGame.getWhite_rating());
        record.put("black_id", chessGame.getBlack_id());
        record.put("black_rating", chessGame.getBlack_rating());
        record.put("moves", chessGame.getMoves());
        record.put("opening_eco", chessGame.getOpening_eco());
        record.put("opening_name", chessGame.getOpening_name());
        record.put("opening_ply", chessGame.getOpening_ply());
        return record;
    }
}
