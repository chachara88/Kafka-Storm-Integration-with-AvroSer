# Description 
This project refers to a system that executes real Time queries in streams of data.
The main tools used in this project are:
Apache Kafka: A distributed streaming platform, used for data injection in the process engine.
Apache Storm: A distributed realtime computation system, used for processing unbounded streams of data.
Espertech: A complex event processing and streaming analytics platform, used for Pattern recognition. 

# ApacheStormMachine Components
This is the main data processing unit of Master Thesis project.
It contains:

*Apache kafka - Apache Storm Integration*
Data are injected to Storm engine, through custom Kafka Spouts.
Schema Registry and Avro deserializers are configured in Kafka Spouts.

*Apache Storm - Espetech Integration*
Real time queries are executed in streamming data.
Pattern recognition -patterns and regularities in data- is executed.

# Queries
1. Caclulate the average variable value inside a predefined time window
2. Check 2 concecutive events of different variables over a predefined theshold. If matched, an alert is reported.
3. Check for a sudden rise across 4 events where the last event is x times greater or +X greater in value than the first event. If matched, an alert is reported.

# Note
This repository can not been build since IP_ADRESS sont exost for security purposes. 
For a working copy of this repo, please contact me directly.

# Depedencies 
Java v12 or latest

# Keywords # 
Real Time Analytics, 
Big Data Analytics, 
Complex Event Processing (CEP), 
Stream Processing, 
Apache Storm,
Apache Kafka,
EsperTech, 
Avro Serialization, 
Schema registry 
