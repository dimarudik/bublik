package org.bublik;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

    public static void doIt(String[] args, int threadCount) {
        ExecutorService service = Executors.newFixedThreadPool(threadCount + 1);
        AtomicInteger counterUpdatedTimestamp = new AtomicInteger(0);
        AtomicInteger counterUpdatedTimestampTZ = new AtomicInteger(0);
        AtomicInteger counterOfInserted = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            int tmp = i;
            service
                    .submit(() -> {
                        try (Connection connection = DriverManager.getConnection(args[0])) {
                            int c = Math.toIntExact(Thread.currentThread().threadId() % 3);
                            switch (c) {
                                case 0: {
                                    if (tmp == 0) {
                                        int d = insert(connection);
                                        counterOfInserted.addAndGet(d);
                                    }
                                }
                                case 1: {
                                    int count = updateTimestamptzByRange(connection, threadCount);
                                    counterUpdatedTimestampTZ.addAndGet(count);
                                }
                                case 2: {
                                    for (int j = 0; j < 100; j++) {
                                        int d = updateTimestamp(connection);
                                        counterUpdatedTimestamp.addAndGet(d);
                                    }
                                }
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    });
        }

        service.shutdown();
        service.close();
        System.out.println("Total inserted: " + counterOfInserted);
        System.out.println("Total unique updated timestamp: " + counterUpdatedTimestamp);
        System.out.println("Total range updated timestamptz: " + counterUpdatedTimestampTZ);

    }

    public static int updateTimestamp(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("update s50k set timestamp = now() where id = ?");
        int id = getRandomInt(1, 60000);
        statement.setInt(1, id);
        int d = statement.executeUpdate();
        statement.close();
        return d;
    }

    public static int updateTimestamptzByRange(Connection connection, int threadCount) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("update s50k set timestamptz = now() where id between ? and ?");
        int t = Math.toIntExact(Thread.currentThread().threadId()) % threadCount * 10;
        int start = getRandomInt(t, t + 100);
        int end = getRandomInt(start, start + 500);
        statement.setInt(1, start);
        statement.setInt(2, end);
        int d = statement.executeUpdate();
        statement.close();
//        System.out.println(start + " " + end + " " + (end - start + 1) + " " + t);
        return d;
    }

    public static int insert(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "insert into s50k (id, uuid, \"Primary\", boolean, " +
                "        int2, int4, int8, smallint, bigint, numeric, float8, " +
                "        date, timestamp, timestamptz, description, current_mood, time) " +
                "    select n as id, gen_random_uuid() as uuid, 'PostgreSQL ' || n as name, " +
                "        case when mod(n, 2) = 0 then false else true end as boolean, " +
                "        0 as int2, n as int4, n as int8, 10 as smallint, n as bigint, n / pi() as numeric, n / pi() as float8, " +
                "        current_date, current_timestamp, current_timestamp, " +
                "        rpad('PostgreSQL', 100, '*') as description, " +
                "        case " +
                "            when floor(random() * (3 + 1) + 0)::int = 1 then 'sad'::mood " +
                "            when floor(random() * (3 + 1) + 0)::int = 2 then 'ok'::mood " +
                "            when floor(random() * (3 + 1) + 0)::int = 2 then 'happy'::mood " +
                "            else null end as current_mood, " +
                "        now() as time " +
                "    from generate_series( (select max(id) + 1 from s50k) , (select max(id) + 1 from s50k) + ? ) as n");
        int count = getRandomInt(1, 2500);
        statement.setInt(1, count);
        int d = statement.executeUpdate();
//        System.out.println("inserted: " + d);
        statement.close();
        return d;
    }

    public static int getRandomInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt(max - min + 1) + min;
    }
}
