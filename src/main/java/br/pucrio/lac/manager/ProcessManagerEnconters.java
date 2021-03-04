package br.pucrio.lac.manager;

import br.pucrio.lac.processor.DataProcessor;

public class ProcessManagerEnconters
{
    private static DataProcessor _manager;

    public static void main(String[] args)
    {
        String topicPeopleData = "peopleNewData2";
        String topicGlobalTable = "peopleStaticData2";
        String topicOut = "peopleUnitedOutput2";

        String topicPeopleEncounters = "peopleEncounters2";
        String topicPeopleContaminated = "peopleContaminated2";
        String topicOut2 = "peopleHaveContact2";

        _manager = new DataProcessor(topicPeopleData, topicGlobalTable, topicOut, topicPeopleEncounters, topicPeopleContaminated, topicOut2);
        _manager.runEncontersTracker();
    }
}
