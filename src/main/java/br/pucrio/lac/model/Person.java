package br.pucrio.lac.model;

import sun.util.cldr.CLDRLocaleDataMetaInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Creator: Vitor
 *
 * Representa uma pessoa monitorada pelo COVID-19Finder. Ela é uma lista de DataUnits.
 */
public class Person
{
    private String _id;

    private String _name;

    private Classification _classification;

    private Map<COVID19_Symptoms, String> _symptoms;

    private Map<String,Person> _encounters;
    private Map<String,String> _encountersDuration;

    private boolean _isContaminated;


    public Person()
    {
        _classification = Classification.NO_RISK;
        _encounters = new HashMap<String,Person>();
        _encountersDuration = new HashMap<String,String>();
        _symptoms = new TreeMap<COVID19_Symptoms, String>();
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String id) {
        _id = id;
    }

    public boolean get_isContaminated()
    {
        return _isContaminated;
    }

    public void set_isContaminated(boolean isContaminated)
    {
        _isContaminated = isContaminated;
    }

    public void add_encounter(Person person, String timeDuration)
    {
        _encounters.put(person.get_id(), person);
        _encountersDuration.put(person.get_id(), timeDuration);
    }

    public void add_symptom(COVID19_Symptoms symptom, String value)
    {
        _symptoms.put(symptom, value);
    }

    public void set_symptoms(Map<COVID19_Symptoms, String> newSymptoms)
    {
        _symptoms = newSymptoms;
    }

    public Map<COVID19_Symptoms,String> get_symptoms()
    {
        return _symptoms;
    }

    public Map<String,Person> get_encounters()
    {
        return  _encounters;
    }

    public Map<String,String> get_encountersDuration()
    {
        return _encountersDuration;
    }

    public boolean search_enconter(Person person)
    {
        Person ret = null;
        ret = _encounters.get(person.get_id());

        if(ret != null)
            return true;
        else
            return false;
    }

    public String get_name()
    {
        return _name;
    }

    public void set_name(String name)
    {
        _name = name;
    }

    public Classification get_classification() {
        return _classification;
    }

    /**
     * Metodo que recalcula a classificacao da pessoa baseado nos dados dela.
     */
    public void calculateClassification()
    {
        int totalValue = 0;

        for(COVID19_Symptoms symptom: COVID19_Symptoms.values())
        {
            if(_symptoms.get(symptom) != null)
            if(_symptoms.get(symptom).equalsIgnoreCase("Yes"))
                totalValue = totalValue + symptom.getValor();
        }

        if(totalValue == 0)
            _classification = Classification.NO_RISK;
        else if(totalValue <= 9)
            _classification = Classification.LOW_RISK;
        else if(totalValue <= 19)
            _classification = Classification.MED_RISK;
        else
            _classification = Classification.HIGH_RISK;
    }

    public void set_classification(Classification classification)
    {
        _classification = classification;
    }

    public void checkEncountersConsistency()
    {
        for (String key : _encountersDuration.keySet())
        {
            Long value = Long.parseLong(_encountersDuration.get(key));
            if (value >= 604800000)
            { // maior ou igual a 7 dias, pode deletar o encontro.
                //_encountersDuration.remove(key);
                //_encounters.remove(key);
            }
        }
    }
}
