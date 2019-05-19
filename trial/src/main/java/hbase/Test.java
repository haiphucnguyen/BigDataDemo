package hbase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class Test {
    public static void main(String[] args) {
        Instant now = Instant.now();
        Instant dateNow = now.truncatedTo(ChronoUnit.DAYS);
        System.out.println(now.getEpochSecond() + " " + dateNow.getEpochSecond());
    }
}
