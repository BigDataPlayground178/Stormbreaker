package entities.results;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class UserRank {

    public Integer count = 0;
    public Long ts;
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

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getTs() {
        return this.ts;
    }


    public String toString() {
        StringBuilder result = new StringBuilder(ts + " , ");
        for (Tuple2<Long, Integer> user : users) {
            result.append(user.f0).append(", ").append(user.f1).append(", ");
        }
        result.delete(result.length() - 3, result.length());
        return result.toString();
    }

}
