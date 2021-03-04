package br.pucrio.lac.processor;

import br.pucrio.lac.model.COVID19_Symptoms;
import br.pucrio.lac.model.Person;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataProcessor
{
    private String _topicPeopleData;
    private String _topicPeopleRecords;
    private String _topicOut;

    private String _topicPeopleEnconters;
    private String _topicPeopleContaminated;
    private String _topicOut2;

    private Properties _kStreamsConfig;

    private String _KAFKA_BROKER = "139.82.100.102:9092";
    //private String _KAFKA_BROKER = "localhost:9092";

    public DataProcessor(String topicPeopleData, String topicPeopleRecords, String topicOut, String topicPeopleEnconters, String topicPeopleContaminated, String topicOut2)
    {
        _topicPeopleData = topicPeopleData;
        _topicPeopleRecords = topicPeopleRecords;
        _topicOut = topicOut;

        _topicPeopleEnconters = topicPeopleEnconters;
        _topicPeopleContaminated = topicPeopleContaminated;
        _topicOut2 = topicOut2;
    }

    /**
     * Método que faz o agregador ficar em execução aguardando a chegada de dados nos topicos de entrada.
     * @return
     */
    public Boolean runClassificator()
    {
        createKafkaProcessor("classification");

        KafkaStreams streams = new KafkaStreams(createClassificationTopology(), _kStreamsConfig);
        //streams.cleanUp(); // Test
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return true;
    }

    public Boolean runEncontersTracker()
    {
        createKafkaProcessor("tracker");

        KafkaStreams streams = new KafkaStreams(createEncontersJoinTopology(), _kStreamsConfig);
        //streams.cleanUp(); // Test
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return true;
    }

    /**
     * Verifica se um topico existe, caso ele não exista, ele é criado.
     */
    private void checkTopic(String topic)
    {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _KAFKA_BROKER);
        try (AdminClient client = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // includes internal topics such as __consumer_offsets
            ListTopicsResult topics = client.listTopics(options);
            Set<String> currentTopicList = topics.names().get();

            boolean topicExists = client.listTopics().names().get().stream().anyMatch(topicName -> topicName.equalsIgnoreCase(topic));

            if(!topicExists)
            {
                CreateTopicsResult result = client.createTopics(
                        Stream.of(topic).map(
                                name -> new NewTopic(name, 1, (short) 1)
                        ).collect(Collectors.toList())
                );
                result.all().get();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cria uma topologia Kafka para ler streams de topicos kafka, processa-las e publicar o resultado no topico _topicOut.
     * A operação que esta topologia faz é união de streams mantendo ID unico.
     *
     * @return
     */
    private Topology createClassificationTopology()
    {
        StreamsBuilder builder = new StreamsBuilder();

        Gson gson = new Gson();
        Type datasetPersonType = new TypeToken<Person>() {}.getType();

        checkTopic(_topicPeopleRecords);
        checkTopic(_topicPeopleData);
        checkTopic(_topicOut);

        KTable<String, String> peopleRecordsTable = builder.table(_topicPeopleRecords);
        KStream<String, String> peopleDataStream = builder.stream(_topicPeopleData);

        KStream<String, String> peopleEnrichedDataJoin =
                peopleDataStream.leftJoin(peopleRecordsTable, /* Left join */
                        //(key, value) -> key,
                        (personData, personRecords) -> {
                            Person personDataObj = gson.fromJson(personData, datasetPersonType);
                            String personRecordStr;// = personRecords;//gson.fromJson(personRecords, datasetPersonType);

                            try
                            {
                                personRecordStr = gson.fromJson(personRecords, datasetPersonType);
                            }catch (Exception e)
                            {
                                personRecordStr = personRecords;
                            }

                            if(personRecordStr != null)
                            {
                                personDataObj.add_symptom(COVID19_Symptoms.SYMPTOM10, "Yes");
                            }

                            personDataObj.calculateClassification(); // Classifica o novo estado da pessoa

                            return gson.toJson(personDataObj); // retorna a pessoa com seus novos dados
                        }
                        );

        peopleEnrichedDataJoin.to(_topicOut);

        return builder.build();
    }

    private Topology createEncontersJoinTopology()
    {
        StreamsBuilder builder = new StreamsBuilder();

        Gson gson = new Gson();
        Type personType = new TypeToken<Person>() {}.getType();

        checkTopic(_topicPeopleContaminated);
        checkTopic(_topicPeopleEnconters);
        checkTopic(_topicOut2);
        checkTopic(_topicPeopleRecords);

        //KStream<String, String> peopleContaminatedStream = builder.stream(_topicPeopleContaminated);
        KTable<String, String> peopleContaminatedTable = builder.table(_topicPeopleContaminated);
        KStream<String, String> peopleEncontersStream = builder.stream(_topicPeopleEnconters);

        KTable<String, String> peopleEncontersTable = peopleEncontersStream
                .groupByKey()
                .aggregate(
                        () -> new String(),
                        (key, value, aggregate) -> {
                            Person person;
                            person = gson.fromJson(value, personType);

                            Person updatedPerson = new Person();
                            if(aggregate != null)
                                if(!aggregate.equalsIgnoreCase(""))
                                {
                                    updatedPerson = gson.fromJson(aggregate, personType);

                                    if(person!=null)
                                    {
                                        for (String personID : person.get_encounters().keySet())
                                        {
                                            updatedPerson.set_name(person.get_name());
                                            updatedPerson.add_encounter(person.get_encounters().get(personID),
                                                                        person.get_encountersDuration().get(personID));
                                        }

                                        updatedPerson.checkEncountersConsistency(); // deleta os encontros maiores ou iguais a 7 dias.
                                    }
                                    else
                                        updatedPerson = null;
                                }
                                else
                                {
                                    updatedPerson = person;
                                }
                            String ret;
                            if(person == null)
                                ret = null;
                            else
                                ret = gson.toJson(updatedPerson);

                            return ret; // value é oq chega pelas streams. E o aggregate é a kTable que estamos montando.
                        });



        KStream<String, String> peopleWhoHaveContact =
                peopleContaminatedTable.join(peopleEncontersTable, /* Left join */
                        //(key, value) -> key,
                        (personContaminated, personEnconters) -> {
                            //Person personContaminatedObj = gson.fromJson(personContaminated, personType);
                            Person personEncountersObj = gson.fromJson(personEnconters, personType);

                            String ret;
                            List<Person> people;
                            if(personEncountersObj != null)
                            { // a pessoa contaminada teve contato com outras!

                                if(personEncountersObj.get_encounters().size() == 0)
                                    return "";
                                ret = "";
                                for (String key : personEncountersObj.get_encounters().keySet())
                                {
                                    //ret = gson.toJson(personEncountersObj.get_enconters().get(key), personType);
                                    ret = ret + personEncountersObj.get_encounters().get(key).get_id();
                                    ret = ret + " ";
                                }

                                ret = ret.substring(0,ret.length()-1);
                            }
                            else
                                ret = null;

                            return ret;
                        }
                ).toStream().flatMapValues(value -> Arrays.asList(value.split("\\s+")))
                .selectKey((key, value) -> {return value;});

        // Atualiza tabela de records, dizendo que a pessoa teve contato
        peopleEncontersTable.toStream().to(_topicOut2); // só pra ver o contato que cada pessoa teve
        peopleWhoHaveContact.to(_topicPeopleRecords); // A lista de IDs das pessoas que tiveram contato com alguem de COVID19


        return builder.build();
    }

    private Topology createEncontersMergeTopology()
    {
        StreamsBuilder builder = new StreamsBuilder();

        Gson gson = new Gson();
        Type datasetListType = new TypeToken<Person>() {}.getType();

        // 1 - streams from Kafka
        KStream<Long, String> streamInput1 = builder.stream(_topicPeopleContaminated); // Seg chave primaria
        KStream<Long, String> streamInput2 = builder.stream(_topicPeopleEnconters); // Comp chave secundaria

        // 2 - apply merge
        KStream<Long, String> mergeStram = streamInput1
                .merge(streamInput2
                );

        // 3 - define the aggregation within the merge
        // Aggregations are key-based operations, which means that they always operate over records (notably record values) of the same key.
        mergeStram
                .groupByKey()
                .aggregate(
                        () -> new String(),
                        (key, value, aggregate) -> {
                            Person dl1;
                            dl1 = gson.fromJson(value, datasetListType);

                            Person newDl = new Person();
                            if(aggregate != null)
                                if(!aggregate.equalsIgnoreCase(""))
                                {
                                    newDl = gson.fromJson(aggregate, datasetListType);
                                    if(dl1!=null)
                                        newDl.set_symptoms(dl1.get_symptoms());
                                    else
                                        newDl = null;
                                }
                                else
                                {
                                    newDl = dl1;
                                }
                            String ret;
                            if(dl1 == null)
                                ret = null;
                            else
                                ret = gson.toJson(newDl);

                            // Se jogar null no aggregate de algum ID ele é deletado.
                            return ret; // value é oq chega pelas streams. E o aggregate é a kTable que estamos montando.
                        })
                // 5 - converter KTable para KStream
                .toStream()
                // 6 - publicar resultado no topico de saida
                .to(_topicOut);

        return builder.build();
    }

    /**
     * Define as configuracoes do kafka processor.
     * @return
     */
    private void createKafkaProcessor(String appName)
    {
        _kStreamsConfig = new Properties();
        _kStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "covid19Finder-application"+appName);
        _kStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _KAFKA_BROKER);
        //_kStreamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        _kStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        _kStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
}
