package obalitskyi.contrarianusers;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;


import java.util.*;
import java.util.stream.Collectors;


public class ContrarianUsers implements Serializable {

    //Integer partitioner !!!!!!!!!!!!!!
    // partition for join

    public static void main(String[] args) {
        ContrarianUsers contrarianUsers = new ContrarianUsers();
        contrarianUsers.run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("obalitskyi.contrarianusers.ContrarianUsers");

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        if (System.getProperty("os.name").toLowerCase().contains("windows"))
            System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        conf.registerKryoClasses(new Class[]{ContrarianUsers.class, ContrarianUsersComparator.class,
                MovieRatingRecord.class, MovieTitlesRecord.class, ReportUnit.class, TopMoviesComparator.class});

        Properties properties = readAppProperties(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Set<MovieRatingRecord>> allMovies = readAllMovies(
                sc, properties.getProperty("movieRatingInputPath")).persist(StorageLevel.MEMORY_AND_DISK_SER());
        JavaPairRDD<Integer, MovieTitlesRecord> titles = readTitles(
                sc, properties.getProperty("movieTilesInputPath")).persist(StorageLevel.MEMORY_AND_DISK_SER());


        Set<Integer> topMovies = findTopMovies(allMovies, titles,
                Integer.parseInt(properties.getProperty("rNumOfUsers")),
                Integer.parseInt(properties.getProperty("mNumOfMovies")));

        JavaPairRDD<Integer, Integer> topMoviesUserRating = mapToUserRating(allMovies.filter(x -> topMovies.contains(x._1())));
        Set<Integer> contrarianUsers = findContrarianUsers(topMoviesUserRating,
                Integer.parseInt(properties.getProperty("mNumOfMovies")),
                Integer.parseInt(properties.getProperty("uNumOfUsers")));

        JavaPairRDD<Integer, ReportUnit> topFilmsOfContrarianUsers = findTopFilmsOfContrarianUsers(allMovies, contrarianUsers, titles);

        writeReport(topFilmsOfContrarianUsers, properties.getProperty("reportOutputPath"));
    }

    private Properties readAppProperties(SparkConf conf) {
        Properties properties = new Properties();
        properties.setProperty("movieTilesInputPath", conf.get("spark.contrarianusers.input.movietitles"));
        properties.setProperty("movieRatingInputPath", conf.get("spark.contrarianusers.input.movieratings"));
        properties.setProperty("reportOutputPath", conf.get("spark.contrarianusers.output.report"));
        properties.setProperty("mNumOfMovies", conf.get("spark.contrarianusers.mnumofmovies"));
        properties.setProperty("uNumOfUsers", conf.get("spark.contrarianusers.unumofusers"));
        properties.setProperty("rNumOfUsers", conf.get("spark.contrarianusers.rnumofmovies"));

        return properties;
    }

    private void writeReport(JavaPairRDD<Integer, ReportUnit> report, String reportOutput) {
        JavaRDD<String> rdd = report
                .map(t -> "" + t._1() + "," + t._2().getTitle() + "," + t._2().getYear() + "," + t._2().getDateOfRating());

        rdd.coalesce(1).saveAsTextFile(reportOutput);
    }

    JavaPairRDD<Integer, ReportUnit> findTopFilmsOfContrarianUsers(JavaPairRDD<Integer, Set<MovieRatingRecord>> allMovies,
                                                                           Set<Integer> contrarianUsers,
                                                                           JavaPairRDD<Integer, MovieTitlesRecord> titles) {
        return allMovies
                .mapToPair(t -> new Tuple2<>(
                        t._1(),
                        t._2().stream().filter(e -> contrarianUsers.contains(e.getCustomerId())).collect(Collectors.toList())
                ))
                .filter(t -> t._2().size() > 0)
                .join(titles)
                .flatMapToPair(t -> t._2()._1().stream()
                        .map(e -> new Tuple2<>(e.getCustomerId(), new Tuple2<>(new ReportUnit(
                                t._2()._2().getTitle(),
                                t._2()._2().getYear(),
                                e.getDateOfRating()
                            ),
                            e.getRating())
                        )).iterator()
                )
                .reduceByKey((x, y) -> {
                    int res = x._2().compareTo(y._2());
                    if (res == 0) {
                        res = -x._1().getYear().compareTo(y._1().getYear());
                        if (res == 0) {
                            res = x._1().getTitle().compareTo(y._1().getTitle());
                        }
                    }
                    if (res > 0) return x;
                    if (res < 0) return y;
                    return x;
                })
                .mapValues(Tuple2::_1);
    }


    Set<Integer> findContrarianUsers(JavaPairRDD<Integer, Integer> topMoviesUserRating,
                                             int mNumOfMovies, int uNumOfUsers) {
        return topMoviesUserRating
                .mapValues(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()))
                .filter(t -> t._2()._2().equals(mNumOfMovies))
                .mapValues(x -> 1.0 * x._1() / x._2())
                .takeOrdered(uNumOfUsers, new ContrarianUsersComparator()).stream().map(Tuple2::_1).collect(Collectors.toSet());

    }

    JavaPairRDD<Integer, Integer> mapToUserRating(JavaPairRDD<Integer, Set<MovieRatingRecord>> topMovies) {
        return topMovies.flatMapToPair(m -> m._2().stream().map(e -> new Tuple2<>(e.getCustomerId(), e.getRating())).iterator());
    }

    Set<Integer> findTopMovies(JavaPairRDD<Integer, Set<MovieRatingRecord>> allMovies,
                               JavaPairRDD<Integer, MovieTitlesRecord> titles, int rNumOfUsers, int mNumOfMovies) {
        return allMovies
                .filter(m -> m._2().size() >= rNumOfUsers)
                .mapToPair(m -> new Tuple2<>(
                        m._1(),
                    1.0 * m._2().stream().mapToInt(MovieRatingRecord::getRating).sum() / m._2().size()
                ))
               .join(titles)
               .top(mNumOfMovies, new TopMoviesComparator()).stream().map(Tuple2::_1).collect(Collectors.toSet());
    }

    JavaPairRDD<Integer, MovieTitlesRecord> readTitles(JavaSparkContext sc, String movieTilesInput) {
        return sc.textFile(movieTilesInput)
                .mapToPair(s -> {
                    int firstSeparator = s.indexOf(",");
                    int secondSeparator = StringUtils.ordinalIndexOf(s, ",", 2); // The title can contain ","
                    Integer movieId = parseInt(s.substring(0, firstSeparator).trim());
                    Integer year = parseInt(s.substring(firstSeparator + 1, secondSeparator).trim());
                    String title = s.substring(secondSeparator + 1).trim(); /// 1, 2010

                    return new Tuple2<>(movieId, new MovieTitlesRecord(year, title));
                })
                .filter(mTRecord -> mTRecord._1() != -1 && !mTRecord._2().getTitle().isEmpty());
    }

    JavaPairRDD<Integer, Set<MovieRatingRecord>> readAllMovies(JavaSparkContext sc, String movieRatingInput) {
        return sc.wholeTextFiles(movieRatingInput, 8) //!!!!!!!!!!!!!!!!!!!
                //.partitionBy(new HashPartitioner(10))
                .mapToPair(tuple -> {
                    Integer movieId = parseInt(tuple._2().substring(0, tuple._2().indexOf(":")));
                    if (movieId == -1) return new Tuple2<>(-1, new HashSet<>(Collections.singletonList(MovieRatingRecord.createDummy())));
                    Set<MovieRatingRecord> movieRatings = Arrays.stream(tuple._2()
                            .substring(tuple._2().indexOf("\n") + 1)
                            .split("\n")
                    )
                    .map(s -> {
                        String[] fields = s.split(",");
                        if (fields.length != 3) return MovieRatingRecord.createDummy();

                        Integer i = parseInt(fields[0].trim());
                        if (i == -1) System.out.println(movieId + ", " + i);

                        return new MovieRatingRecord(
                                parseInt(fields[0].trim()), // customer ID
                                parseInt(fields[1].trim()), // rating
                                fields[2].trim()            // date of rating
                        );
                    })
                    .filter(mRRecord ->
                            mRRecord.getCustomerId() != -1 &&
                            mRRecord.getRating() != -1
                    )
                    .collect(Collectors.toSet());

                    return new Tuple2<>(movieId, movieRatings);
                })
                .filter(movie -> movie._1() != -1 && movie._2().size() > 0);
    }

    private Integer parseInt(String sInteger) {
        if (sInteger == null) return -1;
        Integer res = -1;
        try {
            res = Integer.parseInt(sInteger);
        } catch (NumberFormatException nfe) {
            res = -1;
        }
        return res;
    }
}