package org.bublik;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.bublik.SQLConstants.*;

public class App {
    public static void main(String[] args) {
        int threadCount = 20;
        while (true) {
            try {
                doIt(args, threadCount);
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void doIt(String[] arr, int threadCount) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount + 1);
        AtomicInteger counterUpdatedById = new AtomicInteger(0);
        AtomicInteger counterUpdatedByRange = new AtomicInteger(0);
        AtomicInteger counterOfInserted = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            int tmp = i;
            service
                    .submit(() -> {
                        try (Connection connection = DriverManager.getConnection(arr[0])) {
                            int c = Math.toIntExact(Thread.currentThread().threadId() % 3);
                            switch (c) {
                                case 0: {
                                    if (tmp == 0) {
                                        int d = insert(connection, true);
                                        counterOfInserted.addAndGet(d);
                                        for (int j = 0; j < 500; j++) {
                                            int k = insert(connection, false);
                                            counterOfInserted.addAndGet(k);
                                        }
                                    }
                                    break;
                                }
                                case 1: {
                                    {
                                        int count = updateByRange(connection, threadCount, "public", "likes");
                                        counterUpdatedByRange.addAndGet(count);
                                    }
                                    {
                                        int count = updateByRange(connection, threadCount, "public", "users");
                                        counterUpdatedByRange.addAndGet(count);
                                    }
                                    break;
                                }
                                case 2: {
                                    for (int j = 0; j < 300; j++) {
                                        int d = updateById(connection, !(j % 3 == 0), "public", "likes");
                                        counterUpdatedById.addAndGet(d);
                                    }
                                    break;
                                }
                                default:
                                    break;
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
        }

        service.shutdown();
        service.close();
        System.out.println("Total inserted: " + counterOfInserted);
        System.out.println("Total unique updated: " + counterUpdatedById);
        System.out.println("Total range updated: " + counterUpdatedByRange);

    }

    public static int updateById(Connection connection, boolean tail, String schemaName, String tableName) throws SQLException {
        String s = tail ? UPDATE_TAIL_BY_ID : UPDATE_BY_ID;
        String sql = s.replace("$schemaName", schemaName).replace("$tableName", tableName);
        PreparedStatement statement = connection.prepareStatement(sql);
        int id = tail ? getRandomInt(1, 1000) : getRandomInt(1, 1000000);
        statement.setInt(1, id);
        int d = statement.executeUpdate();
        statement.close();
        return d;
    }

    public static int updateByRange(Connection connection, int threadCount, String schemaName, String tableName) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(UPDATE_BETWEEN_ID.
                replace("$schemaName", schemaName).replace("$tableName", tableName));
        int t = Math.toIntExact(Thread.currentThread().threadId()) % threadCount * 10;
        int start = getRandomInt(t, t + 100);
        int end = getRandomInt(start, start + 500);
        statement.setInt(1, start);
        statement.setInt(2, end);
        int d = statement.executeUpdate();
        statement.close();
        return d;
    }

    public static int insert(Connection connection, boolean batch) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(BATCH_INSERT_LIKES);
        int count = batch ? getRandomInt(1, 5000) : 0;
        statement.setInt(1, count);
        int d = statement.executeUpdate();
        statement.close();
        return d;
    }

    public static int getRandomInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt(max - min + 1) + min;
    }
}
