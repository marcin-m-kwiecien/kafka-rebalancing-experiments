package pl.boono.kafkarebalancing;

import java.util.function.Consumer;

public interface MeasurementConsumer {
    void setRecordConsumer(MessageConsumer consumer);

    void start();

    void stop();

    @FunctionalInterface
    interface MessageConsumer extends Consumer<ConsumedRecord<Measurement>> {

    }
}
