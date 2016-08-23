package com.cansever.consumer;

import com.cansever.consumer.message.MessageObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

/**
 * User: ttacansever
 */
public class AvroDeserializationSchema implements DeserializationSchema<MessageObject> {

    private transient DatumReader<GenericRecord> reader;
    private transient BinaryDecoder decoder;
    private static final String avsc = "{\"namespace\": \"com.cansever\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Message\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"msgId\", \"type\": \"string\"},\n" +
            "     {\"name\": \"username\",  \"type\": \"string\"},\n" +
            "     {\"name\": \"jid\", \"type\": \"string\"},\n" +
            "     {\"name\": \"sentTime\", \"type\": \"long\"},\n" +
            "     {\"name\": \"stanza\",  \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

    @Override
    public MessageObject deserialize(byte[] messageBytes) {
        ensureInitialized();
        try {
            decoder = DecoderFactory.get().binaryDecoder(messageBytes, decoder);
            GenericRecord message = reader.read(null, decoder);
            String messageId = new String(((Utf8) message.get("msgId")).getBytes());
            String username = new String(((Utf8) message.get("username")).getBytes());
            String jid = new String(((Utf8) message.get("jid")).getBytes());
            long sentTime = (Long)message.get("sentTime");
            String stanza = new String(((Utf8) message.get("stanza")).getBytes());
            return new MessageObject(messageId, username, jid, stanza, sentTime);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEndOfStream(MessageObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeExtractor.getForClass(MessageObject.class);
    }

    private void ensureInitialized() {
        if (reader == null) {
            try {
                reader = new GenericDatumReader<>(new Schema.Parser().parse(avsc));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
