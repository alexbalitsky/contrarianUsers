package obalitskyi.contrarianusers;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;


public class ContrarianUsersComparator implements Serializable, Comparator<Tuple2<Integer, Double>> {
    @Override
    public int compare(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) {
        int res = t1._2().compareTo(t2._2());
        if (res == 0) {
            res = t1._1().compareTo(t2._1());
        }
        return res;
    }
}