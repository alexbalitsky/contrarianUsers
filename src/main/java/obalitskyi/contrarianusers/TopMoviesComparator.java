package obalitskyi.contrarianusers;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;


public class TopMoviesComparator implements Serializable, Comparator<Tuple2<Integer, Tuple2<Double, MovieTitlesRecord>>> {

    @Override
    public int compare(Tuple2<Integer, Tuple2<Double, MovieTitlesRecord>> t1, Tuple2<Integer, Tuple2<Double, MovieTitlesRecord>> t2) {
        int res = t1._2._1().compareTo(t2._2()._1());
        if (res == 0) {
            res = -t1._2()._2().getYear().compareTo(t2._2()._2().getYear());
            if (res == 0) {
                res = t1._2()._2().getTitle().compareTo(t2._2()._2().getTitle());
            }
        }
        return res;
    }
}