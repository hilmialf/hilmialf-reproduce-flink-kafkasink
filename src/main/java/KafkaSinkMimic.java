import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSinkMimic {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSinkMimic.class);
    public static String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC = "test-kafkasink-topic";
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2){
            System.out.println("Usage: java -cp <jarname> KafkaSinkMimic <num_subtasks> <checkpoint_interval_ms> <bootstrap_server>");
        }
        int numSubtasks = Integer.parseInt(args[0]);
        int checkpointIntervalMs = Integer.parseInt(args[1]);
        if (args.length == 3){
            KafkaSinkMimic.BOOTSTRAP_SERVERS = args[2];
        }
        System.out.println("Running with args " + Arrays.asList(args).stream().collect(Collectors.joining(" ")));
        KafkaSinkMimic mimic = new KafkaSinkMimic(checkpointIntervalMs, numSubtasks);
        mimic.run();
    }

    private int checkpointInterval = 5000;
    private int numSubtasks = 1;
    private long currentTs;
    private Executor executor;
    public KafkaSinkMimic(int checkpointInterval, int numSubtasks){
        this.checkpointInterval = checkpointInterval;
        this.numSubtasks = numSubtasks;
        this.currentTs = Instant.now().getEpochSecond();
        this.executor = Executors.newFixedThreadPool(numSubtasks);
    }

    public void run() throws InterruptedException {
        int checkpointId = 0;
        String transactionalIdPrefix = System.getenv("HOSTNAME") + "-" + currentTs + "-";
        while(true){
            for(int i = 0; i<numSubtasks; i++){
                int subtaskId = i;
                int finalCheckpointId = checkpointId;
                executor.execute(() -> this.runSubtask(transactionalIdPrefix, subtaskId, finalCheckpointId));
            }
            Thread.sleep(checkpointInterval);
            checkpointId++;
        }
    }

    public void runSubtask(String transactionalIdPrefix, int subtaskId, int checkpointId) {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("transaction.timeout.ms", "900000");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        String transactionalId = transactionalIdPrefix + subtaskId + "-" + checkpointId;
        props.put("transactional.id", transactionalId);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        try {
            logger.info("Starting transaction with id {}", transactionalId);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, Integer.toString(subtaskId), Instant.now().toString()));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            producer.close();
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
        producer.close();
    }
}
