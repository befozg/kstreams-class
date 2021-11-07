package ru.curs.homework;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.homework.configuration.KafkaConfiguration;
import ru.curs.homework.configuration.TopologyConfiguration;
import ru.curs.counting.model.*;
import ru.curs.homework.util.MatchesHistory;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static ru.curs.counting.model.TopicNames.BET_TOPIC;
import static ru.curs.counting.model.TopicNames.EVENT_SCORE_TOPIC;

public class TestTopology {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> inputBetTopic;
    private TestInputTopic<String, EventScore> inputEventScoreTopic;
    private TestOutputTopic<String, Fraud> outputFraudTopic;
    private TestOutputTopic<String, Long> outputBettorAmountsTopic;
    private TestOutputTopic<String, Long> outputTeamAmountsTopic;
    private TestOutputTopic<String, Long> outputMatchesHistoryTopic;

    @BeforeEach
    public void setUp() throws IOException {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(
                topology, config.asProperties());
        inputBetTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());
        inputEventScoreTopic = topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(EventScore.class).serializer());
        outputBettorAmountsTopic = topologyTestDriver.createOutputTopic(
                TopologyConfiguration.BETTOR_AMOUNTS,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
        outputTeamAmountsTopic = topologyTestDriver.createOutputTopic(
                TopologyConfiguration.TEAM_AMOUNTS,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
        outputTeamAmountsTopic = topologyTestDriver.createOutputTopic(
                TopologyConfiguration.TEAM_AMOUNTS,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
        outputFraudTopic = topologyTestDriver.createOutputTopic(
                TopologyConfiguration.POSSIBLE_FRAUDS,
                Serdes.String().deserializer(), new JsonSerde<>(Fraud.class).deserializer());
        outputMatchesHistoryTopic = topologyTestDriver.createOutputTopic(
                TopologyConfiguration.TEAM_AMOUNTS,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    void placeBet(Bet value) {
        inputBetTopic.pipeInput(value.key(), value);
    }

    void placeEvent(EventScore event) {
        inputEventScoreTopic.pipeInput(event.getEvent(), event);
    }

    @Test
//    @Disabled
    void testBettorAmounts() {
        placeBet(Bet.builder().bettor("John").match("A-B").outcome(Outcome.A).amount(100).build());
        placeBet(Bet.builder().bettor("Mary").match("C-D").outcome(Outcome.D).amount(50).build());
        placeBet(Bet.builder().bettor("John").match("E-F").outcome(Outcome.H).amount(10).build());

        Map<String, Long> expected = new HashMap<>();
        expected.put("John", 110L);
        expected.put("Mary", 50L);

        Map<String, Long> actual = outputBettorAmountsTopic.readKeyValuesToMap();

        assertEquals(expected, actual);
    }

    @Test
//    @Disabled
    void testTeamAmounts() {
        placeBet(Bet.builder().bettor("John").match("A-B").outcome(Outcome.A).amount(100).build());
        placeBet(Bet.builder().bettor("Mary").match("B-A").outcome(Outcome.H).amount(50).build());
        placeBet(Bet.builder().bettor("John").match("A-C").outcome(Outcome.D).amount(10).build());
        placeBet(Bet.builder().bettor("John").match("C-A").outcome(Outcome.A).amount(30).build());

        Map<String, Long> expected = new HashMap<>();
        expected.put("B", 150L);
        expected.put("A", 30L);

        Map<String, Long> actual = outputTeamAmountsTopic.readKeyValuesToMap();

        assertEquals(expected, actual);
    }

    @Test
//    @Disabled
    void testFraud() {
        long currentTimestamp = System.currentTimeMillis();
        Score score = new Score().goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp));
        score = score.goalHome();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 100 * 1000));
        score = score.goalAway();
        placeEvent(new EventScore("A-B", score, currentTimestamp + 200 * 1000));
        //ok
        placeBet(new Bet("John", "A-B", Outcome.H, 1, 1, currentTimestamp - 2000));
        //ok
        placeBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 2000));
        //fraud?
        placeBet(new Bet("Sara", "A-B", Outcome.H, 1, 1, currentTimestamp + 100 * 1000 - 10));
        //fraud?
        placeBet(new Bet("Mary", "A-B", Outcome.A, 1, 1, currentTimestamp + 200 * 1000 - 20));
        Fraud expected1 = Fraud.builder()
                .bettor("Sara").match("A-B").outcome(Outcome.H).amount(1).odds(1)
                .lag(10)
                .build();
        Fraud expected2 = Fraud.builder()
                .bettor("Mary").match("A-B").outcome(Outcome.A).amount(1).odds(1)
                .lag(20)
                .build();
        List<KeyValue<String, Fraud>> expected = new ArrayList<>();
        expected.add(KeyValue.pair("Sara", expected1));
        expected.add(KeyValue.pair("Mary", expected2));
        List<KeyValue<String, Fraud>> actual = outputFraudTopic.readKeyValuesToList();
        assertEquals(expected, actual);
    }

    @Test
    void testAwayBet() {
        Bet bet = Bet.builder()
                .match("Cyprus-Belgium")
                .bettor("Karen Efremyan")
                .outcome(Outcome.A)
                .amount(1000)
                .odds(1.1)
                .build();

        inputBetTopic.pipeInput(bet.key(), bet);
        TestRecord<String, Long> record = outputMatchesHistoryTopic.readRecord();
        assert (bet.getBettor().equals("Karen Efremyan"));
        assertEquals(1000L, record.value().longValue());
    }

    @Test
    void testHomeBet() {
        Bet bet = Bet.builder()
                .match("Russia-Moldova")
                .bettor("Karen Efremyan")
                .outcome(Outcome.H)
                .amount(200)
                .build();

        inputBetTopic.pipeInput(bet.key(), bet);
        TestRecord<String, Long> record = outputMatchesHistoryTopic.readRecord();
        assert (bet.getBettor().equals("Karen Efremyan"));
        assertEquals(200L, record.value().longValue());
    }
}
