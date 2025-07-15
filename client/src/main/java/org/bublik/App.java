package org.bublik;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {
    public static void main(String[] args) {

        ExecutorService service = Executors.newFixedThreadPool(10);
        Set<Integer> idOfUpdatedTimestamp = new TreeSet<>();
        List<Integer> countOfUpdatedTimestamptz = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            service
                    .submit(() -> {
                        try (Connection connection = DriverManager.getConnection(args[0])) {
                            for (int j = 0; j < 2; j++) {
//                                int id = updateTimestamp(connection);
                                int count = updateTimestamptzBetween(connection);
                                synchronized (App.class) {
//                                    idOfUpdatedTimestamp.add(id);
                                    countOfUpdatedTimestamptz.add(count);
                                }
                                Thread.sleep(100);
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });

        }

        service.shutdown();
        service.close();
//        idOfUpdatedTimestamp.forEach(System.out::println);
//        System.out.println("Total updated timestamp: " + idOfUpdatedTimestamp.size());
        int sum = 0;
        for (Integer count : countOfUpdatedTimestamptz) {
            sum += count;
        }
        System.out.println("Total updated timestamptz: " + sum);
    }

    public static int getRandomInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt(max - min + 1) + min;
    }

    public static int updateTimestamp(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("update s50k set timestamp = now() where id = ?");
        int id = getRandomInt(1, 999);
        statement.setInt(1, id);
        statement.execute();
        statement.close();
        return id;
    }

    public static int updateTimestamptzBetween(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("update s50k set timestamptz = now() where id between ? and ?");
        int start = getRandomInt(1, 40000);
        int end = getRandomInt(start, start + 20);
        statement.setInt(1, start);
        statement.setInt(2, end);
        statement.execute();
        statement.close();
//        System.out.println("Updated rows from " + start + " to " + end + " | " + (end - start + 1));
        return end - start + 1;
    }
}
