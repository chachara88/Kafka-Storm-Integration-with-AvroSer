# ApacheStormMachine
This is the main data processing unit of Master Thesis project.
It contains:

*Apache kafka - Apache Storm Integration*
Data are injected to Storm engine, through custom Kafka Spouts.
Schema Registry and Avro deserializers are configured in Kafka Spouts.

*Apache Storm - Espetech Integration*
Real time queries are executed in streamming data.
Pattern recognition -patterns and regularities in data- is executed.

*Queries*
1. Caclulate teh average variable value inside a predefined time window
2. Check 2 concecutive events of different variables over a predefined theshold. If matched, an alert is popped up.
3. Check for a sudden rise acros 4 events where the last event is x times greater taht the first event. If matched, an alert is popped up!

