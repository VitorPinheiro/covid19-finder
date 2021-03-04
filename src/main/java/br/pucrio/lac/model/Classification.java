package br.pucrio.lac.model;

public enum Classification
{
    NO_RISK("Nenhum Risco", 0),
    LOW_RISK("Risco Baixo", 1),
    MED_RISK("Risco Medio", 2),
    HIGH_RISK("Risco Alto", 3),
    HAS_COVID("Esta com COVID", 4);

    private String _name;
    private Integer _id;

    Classification(String name, Integer id) {
        _name = name;
        _id = id;
    }

    public Integer getID() {
        return _id;
    }

    public String getName() {
        return _name;
    }
}
