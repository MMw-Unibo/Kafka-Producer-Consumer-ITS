# Kafka Messaging Example in Java

Questo progetto è un semplice esempio di utilizzo di **Apache Kafka** in Java per inviare e ricevere messaggi tramite topic. È pensato per scopi educativi e dimostrativi, utile per comprendere il funzionamento base di producer e consumer Kafka.

## 📁 Struttura del Progetto

- `ProducerMain.java`  
  Punto di ingresso per il produttore Kafka. Invia un messaggio serializzato a un topic specificato.

- `ConsumerMain.java`  
  Punto di ingresso per il consumatore Kafka. Legge e deserializza messaggi dal topic.

- `MsgKafka.java`  
  Classe di modello che rappresenta un messaggio Kafka. Implementa `Serializable` e contiene i campi `id`, `timestamp`, `msg`.

## 🚀 Avvio del Progetto

### 🧑‍💻 Prerequisiti

- Java 8 o superiore
- Apache Kafka in esecuzione (puoi usare una distribuzione locale o Docker)
- Maven (facoltativo se compili a mano)

### 🏗️ Compilazione

Assumendo che i file `.java` siano nel package default o nello stesso package:

```bash
javac MsgKafka.java ProducerMain.java ConsumerMain.java
```

### ▶️ Avvio del Producer

```bash
java ProducerMain
```

Questo invierà un messaggio Kafka con un ID univoco, timestamp corrente e un messaggio testuale verso il topic `prova`.

### ▶️ Avvio del Consumer

In un altro terminale:

```bash
java ConsumerMain
```

Il consumer si sottoscrive al topic `prova` e stampa i messaggi ricevuti nella console.

## 🛠️ Configurazioni Kafka

Assicurati di avere Kafka avviato con Zookeeper. Il progetto assume che il broker Kafka sia accessibile all'indirizzo:

```
localhost:9092
```

Puoi modificare questa informazione nei sorgenti se necessario.

## 🧪 Esempio di Output

Producer:

```
[Producer] Sent message: ID=abc123, TS=2025-06-13T10:00:00Z, MSG=Ciao dal Producer
```

Consumer:

```
[Consumer] Received message: ID=abc123, TS=2025-06-13T10:00:00Z, MSG=Ciao dal Producer
```

## 📦 Dipendenze

Il progetto utilizza le seguenti librerie Kafka (da includere nel classpath o via Maven):

- `kafka-clients`

### Esempio Maven (pom.xml)

```xml
<dependencies>
  <dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.6.0</version>
  </dependency>
</dependencies>
```

## 🔐 Serializzazione

Il progetto utilizza **serializzazione Java nativa** (`ObjectOutputStream`) per inviare oggetti `MsgKafka` come `byte[]`. Questo approccio è semplice, ma in produzione si consiglia l’uso di formati più interoperabili come **Avro**, **JSON** o **Protobuf**.

## 📌 Note

- Il topic `prova` deve esistere nel broker Kafka, oppure può essere creato automaticamente se la configurazione del broker lo consente.
- I messaggi sono persistiti e supportano lo scambio tra applicazioni distribuite.


---

Per ulteriori domande o suggerimenti, sentiti libero di aprire una issue o un pull request.
