package cn._51doit.spark.day06;

import java.util.Random;

public class RandomTest {

    public static void main(String[] args) {

        Random random = new Random(9);

        int i = random.nextInt(5);

        System.out.println(i);


    }
}
