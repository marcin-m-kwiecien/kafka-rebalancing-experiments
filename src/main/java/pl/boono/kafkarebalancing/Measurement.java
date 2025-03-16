package pl.boono.kafkarebalancing;

import java.time.Instant;

public record Measurement(Instant cloudTimestamp) {
}
