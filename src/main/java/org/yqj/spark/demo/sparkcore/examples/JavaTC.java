package org.yqj.spark.demo.sparkcore.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by yaoqijun.
 * Date:2016-06-27
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class JavaTC {

    private static final int numEdges = 200;
    private static final int numVertices = 100;
    private static final Random rand = new Random(42);

    static List<Tuple2<Integer, Integer>> generateGraph() {
        Set<Tuple2<Integer, Integer>> edges = new HashSet<Tuple2<Integer, Integer>>(numEdges);
        while (edges.size() < numEdges) {
            int from = rand.nextInt(numVertices);
            int to = rand.nextInt(numVertices);
            Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(from, to);
            if (from != to) {
                edges.add(e);
            }
        }
        return new ArrayList<Tuple2<Integer, Integer>>(edges);
    }

    static class ProjectFn implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>,
                Integer, Integer> {
        static final ProjectFn INSTANCE = new ProjectFn();

        @Override
        public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
            return new Tuple2<Integer, Integer>(triple._2()._2(), triple._2()._1());
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaHdfsLR");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Integer slices = (args.length > 0) ? Integer.parseInt(args[0]): 2;
        JavaPairRDD<Integer, Integer> tc = sc.parallelizePairs(generateGraph(), slices).cache();

        JavaPairRDD<Integer, Integer> edges = tc.mapToPair(
                new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> e) {
                        return new Tuple2<Integer, Integer>(e._2(), e._1());
                    }
                });

        long oldCount;
        long nextCount = tc.count();
        do {
            oldCount = nextCount;
            // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
            // then project the result to obtain the new (x, z) paths.
            tc = tc.union(tc.join(edges).mapToPair(ProjectFn.INSTANCE)).distinct().cache();
            nextCount = tc.count();
        } while (nextCount != oldCount);

        System.out.println("TC has " + tc.count() + " edges.");
        sc.stop();
    }
}
