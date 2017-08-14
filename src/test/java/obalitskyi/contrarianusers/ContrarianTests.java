package obalitskyi.contrarianusers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.*;

/**
 * Created by mac on 14.08.2017.
 */
public class ContrarianTests implements Serializable{

    private JavaSparkContext sc;

    @Before
    public void init(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ContrarianUsersTest");
        sc = new JavaSparkContext(conf);
    }

    @After
    public void cleanup(){
        sc.stop();
    }

    @Test
    public void simpleFlowTest(){
        ContrarianUsers contrarianUsers = new ContrarianUsers();
        JavaPairRDD<Integer, Set<MovieRatingRecord>> allMovies = contrarianUsers.readAllMovies(
                sc, Thread.currentThread().getContextClassLoader().getResource("dataset").getPath() + "\\*");

        testReadAllMovies(allMovies);

        JavaPairRDD<Integer, MovieTitlesRecord> titles = contrarianUsers.readTitles(
                sc, Thread.currentThread().getContextClassLoader().getResource("movie_titles.txt").getPath());

        testReadTitles(titles);

        Set<Integer> topMovies = contrarianUsers.findTopMovies(allMovies, titles, 10, 3);

        testTopMovies(topMovies);

        Set<Integer> cUsers = contrarianUsers.findContrarianUsers(
                contrarianUsers.mapToUserRating(allMovies.filter(x -> topMovies.contains(x._1()))), 3, 3);

        testContrarianUsers(cUsers);

        Map<Integer, ReportUnit> topFilmsOfContrarianUsers = contrarianUsers
                .findTopFilmsOfContrarianUsers(allMovies, cUsers, titles)
                .collectAsMap();

        testTopFilmsOfContrariantUsers(topFilmsOfContrarianUsers);
    }

    private void testTopFilmsOfContrariantUsers(Map<Integer, ReportUnit> topFilmsOfContrarianUsers) {
        ReportUnit user14 = topFilmsOfContrarianUsers.get(14);
        ReportUnit user4 = topFilmsOfContrarianUsers.get(4);
        ReportUnit user6 = topFilmsOfContrarianUsers.get(6);
        String msg = "There was an mistake while finding top films of contrarian users";

        assertTrue(msg, user14.getTitle().equals("CIsle of Man TT 2004 Review"));
        assertTrue(msg, user14.getYear().equals(2003));
        assertTrue(msg, user14.getDateOfRating().equals("2004-12-12"));

        assertTrue(msg, user4.getTitle().equals("CIsle of Man TT 2004 Review"));
        assertTrue(msg, user4.getYear().equals(2003));
        assertTrue(msg, user4.getDateOfRating().equals("2005-11-21"));

        assertTrue(msg, user6.getTitle().equals("8 Man"));
        assertTrue(msg, user6.getYear().equals(1992));
        assertTrue(msg, user6.getDateOfRating().equals("2003-06-01"));
    }

    private void testContrarianUsers(Set<Integer> cUsers) {
        HashSet<Integer> expected = new HashSet<>(Arrays.asList(14,4,6));
        assertEquals("There was an mistake while finding u contrarian users", expected, cUsers);
    }

    private void testTopMovies(Set<Integer> topMovies) {
        HashSet<Integer> expected = new HashSet<>(Arrays.asList(5,1,2));
        assertEquals("There was an mistake while finding top m movies", expected, topMovies);
    }

    private void testReadTitles(JavaPairRDD<Integer, MovieTitlesRecord> titles) {
        Map<Integer, MovieTitlesRecord> map = titles.collectAsMap();
        String msg = "There are some mistakes while readind movie titles";

        assertTrue(msg, map.get(1).getYear().equals(1996) && map.get(1).getTitle().equals("Dinosaur Planet"));

        assertTrue(msg, map.get(5).getYear().equals(2004) && map.get(5).getTitle().equals("The Rise and Fall of ECW"));

        assertTrue(msg, map.get(10).getYear().equals(2001) && map.get(10).getTitle().equals("Fighter"));
    }


    private void testReadAllMovies(JavaPairRDD<Integer, Set<MovieRatingRecord>> allMovies){
        Map<Integer, Set<MovieRatingRecord>> map = allMovies.collectAsMap();
        String msg = "There are some mistakes while reading files";

        assertTrue(msg, map.get(1).contains(new MovieRatingRecord(1,3,"2005-09-06")));
        assertTrue(msg, map.get(1).contains(new MovieRatingRecord(21,3,"2005-03-28")));
        assertTrue(msg, map.get(1).contains(new MovieRatingRecord(30,5,"2005-08-24")));

        assertTrue(msg, map.get(5).contains(new MovieRatingRecord(1,5,"2005-02-08")));
        assertTrue(msg, map.get(5).contains(new MovieRatingRecord(20,5,"2005-04-03")));
        assertTrue(msg, map.get(5).contains(new MovieRatingRecord(1247469,5,"2005-03-22")));

        assertTrue(msg, map.get(10).contains(new MovieRatingRecord(1,2,"2004-02-09")));
        assertTrue(msg, map.get(10).contains(new MovieRatingRecord(13,3,"2005-11-29")));
        assertTrue(msg, map.get(10).contains(new MovieRatingRecord(1388266,4,"2004-04-12")));
    }
}
