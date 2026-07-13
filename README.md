# 📊 Kafka Streams vs. Lib Padrão — Demo para o Time

Projeto de demonstração para apresentar **Kafka Streams**, comparando-o com a
**biblioteca padrão** (`kafka-clients`) num cenário de **mercado financeiro**.

Um *producer* gera negociações (trades) de ações da B3 e **dois consumidores
fazem a mesma tarefa** — calcular preço médio, VWAP, mínimo, máximo e volume por
papel em janelas de 10 segundos — um usando a lib padrão e outro usando Kafka
Streams. O objetivo é **ver o mesmo problema resolvido das duas formas** e
comparar o esforço.

---

## 🎯 O que é Kafka Streams?

- É uma **biblioteca Java** (não um cluster, não um servidor) para processar
  streams de dados **em cima do Kafka**.
- Você adiciona uma dependência, escreve uma **topologia** (o "passo a passo" do
  processamento) e roda como uma aplicação Java comum.
- Foi feita para **transformar, agregar, juntar e enriquecer** eventos em tempo
  real — não só ler/escrever mensagens.
- Conceitos centrais:
  - **KStream** → um fluxo infinito de eventos (cada trade é um evento).
  - **KTable** → a "foto" atual de um estado (o último valor por chave).
  - **State Store** → estado local (ex.: RocksDB) com backup automático em
    tópicos internos do Kafka → **tolerância a falhas de graça**.
  - **Windowing** → agrupar eventos por janelas de tempo (tumbling, hopping,
    session…).
  - **Event-time** → processa pelo horário do evento, não pelo relógio do servidor.

---

## 🆚 Kafka Streams vs. Lib Padrão (`kafka-clients`)

| Aspecto | Lib padrão (Producer/Consumer) | Kafka Streams |
|---|---|---|
| **Nível de abstração** | Baixo: você controla poll, offsets, threads | Alto: descreve a transformação, ele executa |
| **Estado (agregações)** | Você mantém na mão (Map, banco, cache…) | State Store nativo, com backup automático |
| **Janela de tempo** | Feita manualmente (scheduler, controle de tempo) | `windowedBy(...)` pronto, com event-time |
| **Tolerância a falhas** | Você resolve (perde o estado em memória num crash) | Estado recuperado dos tópicos internos |
| **Exactly-once** | Configuração manual e trabalhosa | `processing.guarantee=exactly_once_v2` |
| **Joins entre tópicos** | Você implementa toda a lógica | `join`, `leftJoin`, `outerJoin` prontos |
| **Escala/paralelismo** | Você gerencia partições e threads | Escala por partição automaticamente |
| **Linhas de código** | Muitas para lógica de estado | Poucas, declarativas |
| **Curva de aprendizado** | Baixa para casos simples | Média (conceitos de stream/table/tempo) |

> 👉 **Neste projeto** os dois consumidores fazem *a mesma agregação em janela*.
> Compare [`StandardConsumerApp`](consumer-standard/src/main/java/com/example/standard/StandardConsumerApp.java)
> com [`StreamsConsumerApp`](consumer-streams/src/main/java/com/example/streams/StreamsConsumerApp.java):
> a versão Streams cabe numa única topologia, enquanto a padrão precisa de
> `Map` + `scheduler` + sincronização de threads — e **ainda perde o estado num
> restart**.

---

## ✅ Vantagens do Kafka Streams

- **Menos código para lógica de estado** — agregações, contagens e janelas são
  operações de primeira classe.
- **Tolerância a falhas embutida** — o estado é replicado em tópicos internos;
  se o app cai e sobe de novo, ele **recupera de onde parou**.
- **Event-time e janelas prontas** — lida com eventos fora de ordem e dados
  atrasados (grace period) sem você reinventar isso.
- **Exactly-once semantics** — com uma linha de configuração.
- **Joins e enriquecimento** — juntar dois streams ou stream + tabela é trivial.
- **Escala horizontal** — suba mais instâncias com o mesmo `application.id` e o
  Kafka rebalanceia as partições automaticamente.
- **Sem cluster extra** — é só uma lib; roda em qualquer lugar que rode um JAR.

## ❌ Desvantagens / Quando NÃO usar

- **Curva de aprendizado** — KStream, KTable, event-time, janelas e serdes exigem
  estudo; para "só ler e gravar", é overkill.
- **Só Kafka** — a fonte e o destino precisam ser Kafka (não é um ETL genérico).
- **Só JVM** — a lib é Java/Scala. Se o time é Python/Go, não se aplica direto.
- **State Store consome disco/memória** — janelas grandes e muito estado pesam
  (RocksDB, rebalanceamentos podem ficar lentos).
- **Debug mais difícil** — a execução é assíncrona e distribuída; entender o que
  aconteceu exige conhecer a topologia e os tópicos internos.
- **Tópicos internos "escondidos"** — o Streams cria tópicos de repartition e
  changelog; é preciso saber que eles existem para operar bem.

### Regra de bolso

- **Lib padrão** → integração simples, ler mensagens e chamar um serviço/gravar
  num banco, produtores, sem estado ou com estado externo simples.
- **Kafka Streams** → agregações, janelas de tempo, joins, contadores, detecção
  de padrões, pipelines de transformação **stateful** dentro do Kafka.

---

## 💰 O cenário da demo (mercado financeiro)

- **Tópico:** `stock-trades`
- **Mensagem (JSON):**
  ```json
  { "ticker": "PETR4", "price": 38.42, "quantity": 1200, "timestamp": 1720000000000 }
  ```
- **Chave:** o `ticker` (garante ordem por papel e distribui entre partições).
- **Papéis simulados:** PETR4, VALE3, ITUB4, BBDC4, MGLU3, WEGE3, ABEV3
  (preços evoluem em *random walk*).
- **Cálculo feito pelos dois consumidores (janela de 10s por papel):**
  - Nº de trades
  - Preço médio simples
  - **VWAP** (preço médio ponderado pelo volume)
  - Mínimo e máximo
  - Volume total

> Os dois consumidores estão em **grupos diferentes**, então **ambos recebem
> todas as mensagens** e você vê os dois resultados lado a lado.

---

## 🚀 Como rodar

Pré-requisitos: **Docker** e **Docker Compose**. (Não precisa de Java/Maven na
máquina — o build acontece dentro do Docker.)

```bash
# na raiz do projeto
docker compose up --build
```

Isso sobe: **Kafka** → **producer** → **consumer-standard** → **consumer-streams**.

### Vendo a saída de cada serviço (recomendado para a apresentação)

```bash
# em terminais separados:
docker compose logs -f producer
docker compose logs -f consumer-standard
docker compose logs -f consumer-streams
```

- No **producer** você vê os trades sendo gerados.
- No **consumer-standard** e no **consumer-streams**, a cada ~10s aparece a
  tabela/linhas com a agregação por papel. **Compare os números** — devem bater.

### Para encerrar

```bash
docker compose down
```

### Dica: inspecionar o tópico direto do host

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic stock-trades --from-beginning
```

---

## 🗂️ Estrutura dos projetos

Projeto **Maven multi-módulo**, Java 21, cada app vira um *fat-jar* e roda no seu
próprio container.

```
kafka-streams/
├── pom.xml                     # POM pai: versões, módulos, dependencyManagement
├── docker-compose.yml          # sobe Kafka + os 3 apps
├── .dockerignore
│
├── common/                     # código compartilhado
│   ├── pom.xml
│   └── src/main/java/com/example/common/
│       ├── Trade.java          # record da negociação (o "evento")
│       └── Json.java           # helper de (de)serialização JSON (Jackson)
│
├── producer/                   # PRODUCER (lib padrão / KafkaProducer)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/example/producer/
│       └── ProducerApp.java    # gera trades e publica em 'stock-trades'
│
├── consumer-standard/          # CONSUMER com a LIB PADRÃO
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/example/standard/
│       └── StandardConsumerApp.java   # poll + Map + scheduler = agregação na mão
│
└── consumer-streams/           # CONSUMER com KAFKA STREAMS
    ├── pom.xml
    ├── Dockerfile
    └── src/main/java/com/example/streams/
        ├── StreamsConsumerApp.java    # a topologia (o coração da demo)
        ├── WindowStats.java           # estado agregado por janela (imutável)
        └── JsonSerde.java             # Serde JSON p/ Trade e WindowStats
```

### Papel de cada módulo

- **`common`** — o *record* `Trade` (o evento) e o utilitário `Json`. Reaproveitado
  por todos, evita duplicação do modelo.
- **`producer`** — usa a **lib padrão** (`KafkaProducer`) para publicar trades
  continuamente. Mostra o lado "produtor" da comparação.
- **`consumer-standard`** — usa a **lib padrão** (`KafkaConsumer`). A agregação em
  janela é **feita à mão**: loop de `poll`, `Map` de acumuladores, um
  `ScheduledExecutorService` para "fechar" a janela e `synchronized` para
  coordenar as duas threads. Funciona, mas **é processing-time e não sobrevive a
  um restart**.
- **`consumer-streams`** — usa **Kafka Streams**. A mesma agregação vira uma
  topologia declarativa:
  `stream → groupByKey → windowedBy(10s) → aggregate → suppress → foreach`.
  Ganha **event-time**, **estado tolerante a falhas** e **resultado final por
  janela** sem código extra.

### Trecho-chave para mostrar na apresentação

**Lib padrão** — só o "esqueleto" da janela manual:

```java
// thread de poll
while (running) {
    var records = consumer.poll(Duration.ofMillis(500));
    for (var rec : records) {
        Trade t = Json.fromJson(rec.value(), Trade.class);
        synchronized (lock) {
            window.computeIfAbsent(t.ticker(), k -> new Acc()).add(t);
        }
    }
}
// ...e uma thread SEPARADA só para fechar a janela a cada 10s
scheduler.scheduleAtFixedRate(this::flushWindow, 10, 10, SECONDS);
```

**Kafka Streams** — a mesma agregação, declarativa:

```java
builder.stream(TOPIC, Consumed.with(Serdes.String(), tradeSerde))
    .groupByKey(Grouped.with(Serdes.String(), tradeSerde))
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
    .aggregate(WindowStats::empty, (k, trade, stats) -> stats.add(trade), /* store */ )
    .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
    .toStream()
    .foreach((window, stats) -> print(window, stats));
```

> **Mensagem final para o time:** a lib padrão te dá controle total e é perfeita
> para casos simples e produção de mensagens. O Kafka Streams brilha quando o
> problema é **stateful** (agregar, janelar, juntar) — ele elimina justamente o
> código chato e propenso a bugs de gerenciar estado, tempo e falhas.
