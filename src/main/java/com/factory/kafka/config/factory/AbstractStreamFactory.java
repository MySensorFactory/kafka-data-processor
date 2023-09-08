package com.factory.kafka.config.factory;

import com.factory.kafka.config.model.KafkaNativeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

@Getter
public abstract class AbstractStreamFactory<T extends SpecificRecordBase> {
    private final String schemaRegistryUrl;
    private final Serde<T> serde;

    protected AbstractStreamFactory(final KafkaNativeConfig kafkaNativeConfig) {
        this.schemaRegistryUrl = kafkaNativeConfig.getSchemaRegistryUrl();
        this.serde = prepareSerde();
    }

    protected  <U extends SpecificRecordBase> Serde<U> prepareSerde() {
        final Serde<U> result = new SpecificAvroSerde<>();
        result.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return result;
    }
}
