package entities.results;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class UserRank {

    public Integer count = 0;
    public ArrayList<Tuple2<Long, Integer>> users;

    public UserRank() {
        users = new ArrayList<>();
    }

    public void incrementCount() {
        this.count ++;
    }

    public void addUser(Tuple2<Long, Integer> user) {
        users.add(user);
    }
}
