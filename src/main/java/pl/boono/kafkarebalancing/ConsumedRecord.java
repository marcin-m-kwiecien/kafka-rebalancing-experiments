package pl.boono.kafkarebalancing;

public record ConsumedRecord<T>(int partition, T value) {
}
