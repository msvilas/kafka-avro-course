package com.github.simplesteph.kafka.apps.v1;

import com.myvaluesolutions.abanca.kafka.avro.TransaccionCategorizable;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.*;

import java.math.BigDecimal;
import java.util.Properties;

import static java.lang.System.*;


public class KafkaAvroJavaProducerV1Demo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        Producer<String, TransaccionCategorizable> producer = new KafkaProducer<String, TransaccionCategorizable>(properties);

        String topic = "QMYV.MYVALUE.CGE.MovimientosCuentaCategorizables";

        // copied from avro examples
        TransaccionCategorizable transaccionCategorizable = TransaccionCategorizable.newBuilder()
                .setCodigoOperacion("980")
                .setConcepto("MOVISTAR")
                .setCodigoTransaccion("")
                .setConceptoAmpliado("FUSION MES NOVIEMBRE ADSL+MOVIL")
                .setDescripcionOperacion("ADEUDO SEPA")
                .setFechaOperacion(Instant.now())
                .setFechaValor(Instant.now())
                .setImporte(BigDecimal.valueOf(23.00))
                .setMovimientoId("A30099999999902012-12-282012-12-28-00.04.00.123456")
                .setProductoId("ARA300999999999")
                .setSaldoFinal(BigDecimal.valueOf(12324.45)).build();

        ProducerRecord<String, TransaccionCategorizable> producerRecord = new ProducerRecord<String, TransaccionCategorizable>(
                topic, transaccionCategorizable
        );

        out.println(transaccionCategorizable);
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    out.println(metadata);
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();

    }
}
