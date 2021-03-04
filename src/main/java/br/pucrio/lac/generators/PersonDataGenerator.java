package br.pucrio.lac.generators;

import br.pucrio.lac.model.COVID19_Symptoms;
import br.pucrio.lac.model.DataUnit;
import br.pucrio.lac.model.Person;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PersonDataGenerator
{
    public static String KAFKA_BROKER = "139.82.100.102:9092";
    //public static String KAFKA_BROKER = "localhost:9092";
    public static String CLIENT_ID="clientJsonGen";

    // KStream
    public static String[] _attributes = new String[9];

    // KTable
    public static String att10 = "Contato com algum caso de COVID19";


    public static void main(String[] args)
    {
        runProducer();
    }

    private static Person generatePersonData(String personName, String personID)
    {
        Person person = new Person();
        person.set_id(personID);
        person.set_name(personName);

        for(COVID19_Symptoms symptoms: COVID19_Symptoms.values())
        {
            if(symptoms.getName().equalsIgnoreCase(COVID19_Symptoms.SYMPTOM10.getName())) {
                person.add_symptom(symptoms, "No"); // No inicio nao tem info de encontro
                continue;
            }

            if(personID.equalsIgnoreCase("2") || personID.equalsIgnoreCase("9011"))
                person.add_symptom(symptoms, "No");
            else
                person.add_symptom(symptoms, "Yes");
        }

        return person;
    }

    private static Person generateEncounterData(String personName, String personID, String num)
    {
        Person person = new Person();
        person.set_id(personID);
        person.set_name(personName);

        Person personMeet = new Person();
        personMeet.set_id("90"+num+personID);

        if(personMeet.get_id().equals("9011"))
            personMeet.set_name("InfelizPerson2");
        else
            personMeet.set_name("Fulano_"+num+personName);

        person.add_encounter(personMeet, "432000000");

        return person;
    }

    private static Person generateContaminatedPerson(String personName, String personID)
    {
        Person person = new Person();
        person.set_id(personID);
        person.set_name(personName);
        person.set_isContaminated(true);

        return person;
    }

    private static boolean runProducer()
    {
        Person person1 = generatePersonData("Vitor2", "1");
        Person person2 = generatePersonData("Roberta", "2");

        Person person9011 = generatePersonData("InfelizPerson2", "9011");

        Person person1Encounter1 = generateEncounterData("Vitor2", "1", "1");
        Person person1Encounter2 = generateEncounterData("Vitor2", "1", "2");

        Person person1Contaminated = generateContaminatedPerson("Vitor2", "1");

        // create the gson object
        Gson gson = new Gson();
        // use the gson objected created to convert albums to json

        String jsonString1 = gson.toJson(person1);
        String jsonString2 = gson.toJson(person2);
        String jsonString9011 = gson.toJson(person9011);

        String jsonString1_1 = gson.toJson(person1Encounter1);
        String jsonString1_2 = gson.toJson(person1Encounter2);

        String jsonString1_contaminated = gson.toJson(person1Contaminated);

        System.out.println("person1 ="+ jsonString1);
        System.out.println("person2 ="+ jsonString2);

        System.out.println("person1_1 ="+ jsonString1_1);
        System.out.println("person1_2 ="+ jsonString1_2);

        Type datasetListType = new TypeToken<Person>() {}.getType();

        // -----------  jsonString to Objects
        Producer<String, String> producer = createProducer();

        try {
            producer.send(peopleDataStream(person1.get_id(), jsonString1)).get(); // esse .get garante a ordem de envio, mas não precisa disso em um app na produção.
            producer.send(peopleDataStream(person2.get_id(), jsonString2)).get();
            producer.send(peopleDataStream(person9011.get_id(),jsonString9011)).get(); // infeliz que teve contato

            producer.send(peopleEncontersStream(person1Encounter1.get_id(), jsonString1_1)).get();
            producer.send(peopleEncontersStream(person1Encounter2.get_id(), jsonString1_2)).get();

            producer.send(peopleContaminatedStream(person1Contaminated.get_id(), jsonString1_contaminated)).get();
            //producer.send(peopleDataStream(newPerson2.get_id(), newJsonString2)).get();

        } catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }

        return true;
    }

    private static ProducerRecord<String, String> peopleDataStream(String key, String value){
        return new ProducerRecord<>("peopleNewData", key, value);
    }
    private static ProducerRecord<String, String> peopleGlobalTable(String key, String value){
        return new ProducerRecord<>("peopleStaticData", key, value);
    }
    private static ProducerRecord<String, String> peopleEncontersStream(String key, String value){
        return new ProducerRecord<>("peopleEncounters", key, value);
    }
    private static ProducerRecord<String, String> peopleContaminatedStream(String key, String value){
        return new ProducerRecord<>("peopleContaminated", key, value);
    }


    private static Producer<String, String> createProducer()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
}
