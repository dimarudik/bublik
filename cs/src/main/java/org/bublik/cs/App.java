package org.bublik.cs;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;

import java.util.Set;


public class App {

    public static void main(String[] args) {
        String serverIP = "localhost";
        String keyspace = "store";
        Cluster cluster = Cluster
                .builder()
                .addContactPoint(serverIP)
                .withoutJMXReporting()
                .build();
        Metadata metadata = cluster.getMetadata();
        Set<TokenRange> tokenRangeSet = metadata.getTokenRanges();
        tokenRangeSet.forEach(tokenRange -> System.out.println(tokenRange.getStart() + " " + tokenRange.getEnd()));
        String key = "1234";

        class LongTokenRange {
            private final long start;
            private final long end;

            LongTokenRange(Object start, Object end) {
                this.start = (long) start;
                this.end = (long) end;
            }

            public Long getStart() {
                return start;
            }

            public Long getEnd() {
                return end;
            }

            boolean contains(long token) {
                return token >= start && token <= end;
            }
        }

        MM3 mm3 = new MM3(key);
        System.out.println(mm3.getTokenLong());

        LongTokenRange l = tokenRangeSet
                .stream()
                .map(tokenRange -> new LongTokenRange(tokenRange.getStart().getValue(), tokenRange.getEnd().getValue()))
                .filter(longTokenRange -> longTokenRange.contains(mm3.getTokenLong()))
                .findAny()
                .orElse(null);

        assert l != null;
        System.out.println(l.start + " " + l.end);

        Session session = cluster.connect(keyspace);
        session.close();
        cluster.close();
    }
}