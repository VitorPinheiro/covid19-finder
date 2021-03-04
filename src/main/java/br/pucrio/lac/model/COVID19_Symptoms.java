package br.pucrio.lac.model;

public enum COVID19_Symptoms
{
    SYMPTOM1("Febre", 5),
    SYMPTOM2("Dor de cabeca", 1),
    SYMPTOM3("Secrecao nasal ou espirros", 1),
    SYMPTOM4("Dor de garganta", 1),
    SYMPTOM5("Tosse seca", 3),
    SYMPTOM6("Dificuldade respiratoria", 10),
    SYMPTOM7("Dor no corpo", 1),
    SYMPTOM8("Diarreia", 1),
    SYMPTOM9("Viajou nos ultimos 14 dias", 3),
    SYMPTOM10("Contato com COVID19", 10);

    private String _name;
    private Integer _valor;

    COVID19_Symptoms(String name, Integer valor) {
        _name = name;
        _valor = valor;
    }

    public Integer getValor() {
        return _valor;
    }

    public String getName() {
        return _name;
    }
}
