# 📊 Kafka Streams — Entendendo a ferramenta e quando usá-la

Material de apoio da apresentação para o time.

- **Parte 1 — Teoria:** o que é Kafka Streams, os conceitos principais,
  vantagens, desvantagens e a comparação com a lib padrão.
- **Parte 2 — Os projetos:** o que foi construído nesta POC para demonstrar
  tudo isso na prática.

---
---

# 📚 PARTE 1 — TEORIA

## 🧩 O ponto de partida: o que a lib padrão faz (e o que não faz)

Quando falamos "lib padrão", falamos do **`kafka-clients`** — a biblioteca que
todo mundo já usa no dia a dia. Ela entrega duas peças:

- **`KafkaProducer`** → envia mensagens para um tópico.
- **`KafkaConsumer`** → busca mensagens de um tópico, num loop de `poll()`.

Ela é ótima e resolve a maior parte dos casos. Mas repare no que ela **não**
faz por você:

- ❌ **Não guarda estado.** Precisa contar, somar ou lembrar do que já passou?
  O acumulador é problema seu (um `Map`, um cache, um banco...).
- ❌ **Não entende tempo.** "Nos últimos 5 minutos" é uma conta que você faz na mão.
- ❌ **Não recupera nada.** Se a aplicação cair e voltar, o que estava na memória morreu.

> 🔑 Para "ler mensagem → chamar uma API → gravar no banco", nada disso importa —
> a lib padrão é perfeita. **O problema começa quando o processamento precisa
> lembrar do passado.**

---

## 🎯 O que é Kafka Streams?

- É uma **biblioteca Java**. Você adiciona **uma dependência** no `pom.xml` e pronto.
- **Não** é um servidor. **Não** é um cluster. **Não** tem nada novo para instalar
  ou operar.
- Sua aplicação continua sendo um Java comum (`public static void main`), que roda
  onde você já roda: container, VM, Kubernetes.

A diferença está em **como você escreve o processamento**. Em vez de um loop de
`poll()` com toda a lógica manual dentro, você **descreve** o que quer — e a
biblioteca executa:

```
pegue o fluxo de eventos → agrupe por conta → janele em 5 minutos → conte
```

> Você diz **O QUE** quer. A biblioteca resolve **COMO**: estado, tempo, threads,
> falhas e paralelismo.

---

## 🧠 Os conceitos principais

### 1. Topologia

É a "receita" do seu processamento: a sequência de passos que cada evento
percorre. Você monta a topologia uma vez, na inicialização, e o Kafka Streams a
executa para sempre.

```java
builder.stream("transactions")   // fonte
       .filter(...)              // passo
       .groupByKey()             // passo
       .count()                  // passo
       .toStream()
       .foreach(...);            // destino
```

### 2. KStream — o fluxo de eventos

Um fluxo **infinito de fatos que aconteceram**. Cada mensagem é um evento novo,
independente.

> 💡 Pense num **extrato bancário**: cada linha é um fato que ocorreu.
> "Saque de R$ 100", "depósito de R$ 500". Nenhuma linha substitui a anterior.

### 3. KTable — o estado atual

A **"foto" mais recente** de cada chave. Se chegar um novo valor para a mesma
chave, ele **substitui** o anterior.

> 💡 Pense no **saldo da conta**: só o valor atual importa. Um novo saldo
> sobrescreve o antigo.

**KStream e KTable são as duas visões do mesmo dado** — o extrato (eventos) e o
saldo (estado consolidado). O Kafka Streams converte de um para o outro
naturalmente (`toStream()`, `toTable()`, agregações).

### 4. State Store — o superpoder

Quando você usa `count()`, `aggregate()`, `reduce()` ou um `join`, o Streams
precisa **lembrar** de algo. Ele guarda isso num **State Store**:

- fica **local** na instância (rápido — geralmente RocksDB ou memória);
- mas tem **backup automático em um tópico interno do Kafka** (o *changelog*);
- se a aplicação **cair e subir de novo, mesmo em outra máquina**, ela
  **reconstrói o estado** a partir desse tópico e continua de onde parou.

> 🔥 **Este é o principal argumento a favor do Kafka Streams.** É exatamente o
> que um `Map` em memória, na lib padrão, jamais conseguirá fazer.

### 5. Janelas de tempo (Windowing)

Agrupar eventos por faixas de tempo é uma operação nativa:

- **Tumbling** — blocos fixos que não se sobrepõem (ex.: de 5 em 5 minutos).
- **Hopping** — blocos que se sobrepõem (ex.: 5 min, avançando de 1 em 1 min).
- **Session** — agrupa por períodos de atividade, separados por inatividade.

```java
.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
```

### 6. Event-time vs. Processing-time

- **Processing-time** — o horário em que o **seu código** processou a mensagem
  (o relógio do servidor). É o que você usaria naturalmente na lib padrão.
- **Event-time** — o horário em que o **evento realmente aconteceu** (o timestamp
  da mensagem). É o padrão no Kafka Streams.

> 💡 **Por que importa:** se uma mensagem atrasar (rede lenta, consumidor
> reiniciando, reprocessamento), no processing-time ela cai na **janela errada**.
> No event-time, ela cai na **janela certa** — mesmo chegando depois.
> O Streams ainda permite configurar um *grace period*: quanto tempo esperar por
> eventos atrasados antes de fechar a janela em definitivo.

### 7. Serdes

**Ser**ializer + **De**serializer. O Kafka trafega bytes; o Serde é o "tradutor"
que ensina o Streams a converter esses bytes nos seus objetos de domínio (e
vice-versa) — inclusive para salvar o estado nos tópicos internos.

---

## ✅ Vantagens

- **Estado sem esforço** — contar, somar, agregar, janelar e juntar tópicos são
  operações prontas. Você não reescreve (nem retesta) infraestrutura de estado.
- **Tolerância a falhas embutida** — o State Store tem backup contínuo no Kafka.
  Caiu? Subiu? **Continua de onde parou.**
- **Entende tempo de verdade** — event-time, janelas e tratamento de eventos
  atrasados, sem gambiarra.
- **Exactly-once com uma linha** — `processing.guarantee=exactly_once_v2`.
- **Escala horizontal automática** — suba mais instâncias com o mesmo
  `application.id` e o Kafka redistribui as partições entre elas sozinho.
- **Zero infraestrutura nova** — é um JAR. Sem cluster dedicado, sem servidor
  para operar.

## ❌ Desvantagens

- **Curva de aprendizado** — KStream, KTable, serdes, event-time, janelas...
  exige estudo. Para "ler e gravar", é complexidade desnecessária.
- **Só faz Kafka → Kafka** — a entrada e a saída precisam ser tópicos. Para
  integrar com banco ou API externa, o par dele é o **Kafka Connect** (ou o seu
  próprio código).
- **Só JVM** — Java, Kotlin, Scala. Time Python ou Go precisa de outra solução.
- **Estado ocupa disco e memória** — muitas chaves e janelas longas pesam no
  RocksDB; restauração e rebalanceamento podem ficar lentos.
- **Debug menos óbvio** — a biblioteca cria tópicos internos (*changelog*,
  *repartition*) que você precisa conhecer para operar e investigar problemas.

---

## 🆚 Comparação com a lib padrão

| Aspecto | Lib padrão (`kafka-clients`) | Kafka Streams |
|---|---|---|
| **Estilo de código** | Imperativo: você escreve o loop e a lógica | Declarativo: você descreve a transformação |
| **Processar sem estado** (filtrar, validar, encaminhar) | ✅ Simples e direto | ✅ Simples e direto — **empate** |
| **Processar com estado** (contar, somar, agregar) | Você cria e mantém o acumulador na mão | Operação nativa (`count`, `aggregate`) |
| **Janela de tempo** | Cálculo manual | `windowedBy(...)` — uma linha |
| **Noção de tempo** | Relógio do servidor (processing-time) | Horário do evento (event-time) |
| **App reiniciou, e o estado?** | 💀 Perdeu tudo que estava em memória | ✅ Reconstrói do Kafka e continua |
| **Join entre tópicos** | Você implementa toda a lógica | `join` / `leftJoin` / `outerJoin` prontos |
| **Exactly-once** | Transações manuais, trabalhoso | 1 linha de configuração |
| **Escalar horizontalmente** | Você coordena instâncias e partições | Rebalanceamento automático |
| **Curva de aprendizado** | Baixa | Média |
| **Infraestrutura** | Nenhuma extra | Nenhuma extra (é uma lib) |

---

## 🧭 Regra de bolso: quando usar cada um

- ✅ **Lib padrão** → processamento **sem estado**:
  ler mensagem → validar → chamar serviço → gravar no banco.
  E **todos os producers**.

- ✅ **Kafka Streams** → processamento **com estado**:
  contar, somar, agregar, janelar por tempo, juntar dois tópicos,
  detectar padrões, montar visões materializadas.

> A pergunta que decide não é "qual é mais moderno?", e sim:
> **"para processar a mensagem atual, eu preciso lembrar das anteriores?"**
> Se **não**, use a lib padrão. Se **sim**, o Kafka Streams provavelmente vai
> economizar muito código seu — e muito bug.

---
---

# 🛠️ PARTE 2 — OS PROJETOS

## 💰 O cenário da POC

Um **sistema de pagamentos** simulado: um producer gera **1 transação por
segundo**, distribuídas entre 5 contas (`ACC-001` a `ACC-005`). Cerca de **1 em
cada 5 transações é de valor alto** (acima de R$ 10.000).

Cada transação é uma mensagem JSON no tópico `transactions`:

```json
{ "id": "TX-0042", "accountId": "ACC-003", "amount": 12500.00, "timestamp": 1720000000000 }
```

A **chave** da mensagem é o `accountId` — assim todas as transações de uma conta
caem na mesma partição, mantêm a ordem e podem ser agrupadas.

### As duas regras — o eixo da comparação

**Dois consumidores** aplicam **exatamente as mesmas duas regras**. Um usa a lib
padrão, o outro usa Kafka Streams:

| | Regra | O que ela exige |
|---|---|---|
| **1️⃣** | Transação **acima de R$ 10.000** → dispara um **ALERTA** | Basta olhar a mensagem atual. **SEM estado.** |
| **2️⃣** | **Contar** as transações de cada conta em **janelas de 5 minutos** | Precisa **lembrar** das mensagens anteriores. **COM estado.** |

> 🔑 **Essa divisão é proposital.** A Regra 1 é fácil nas duas abordagens — vai
> dar empate. É a **Regra 2** que separa as duas ferramentas. Foi para isolar
> exatamente essa diferença que a POC foi montada assim.

Os dois consumidores rodam em **grupos de consumo diferentes**, então **ambos
recebem todas as transações** — dá para comparar a saída lado a lado.

---

## ⚔️ O contraste no código

### Regra 1 (sem estado) — praticamente igual nos dois

```java
// LIB PADRÃO — dentro do loop de poll
if (tx.amount() > LIMITE_ALERTA) {
    System.out.println("*** ALERTA *** " + tx.id());
}
```

```java
// KAFKA STREAMS
transacoes
    .filter((conta, tx) -> tx.amount() > LIMITE_ALERTA)
    .foreach((conta, tx) -> System.out.println("*** ALERTA *** " + tx.id()));
```

> **Empate.** Um `if` contra um `filter`. Para regra sem estado, a lib padrão
> resolve numa linha. **Não troque de ferramenta só por isso.**

### Regra 2 (com estado) — aqui a diferença aparece

**Lib padrão** — eu preciso criar o estado, descobrir a janela **na mão** e zerar
o contador **na mão** quando ela virar:

```java
// um Map na memória guardando o contador de cada conta
Map<String, Contador> contadores = new HashMap<>();

int contarNaJanela(Transaction tx) {
    // em qual bloco de 5 minutos esta transação caiu?
    long janelaAtual = tx.timestamp() / JANELA_MS;

    Contador contador = contadores.computeIfAbsent(tx.accountId(), k -> new Contador());

    // a janela virou? então zera o contador na mão
    if (contador.janela != janelaAtual) {
        contador.janela = janelaAtual;
        contador.quantidade = 0;
    }
    return ++contador.quantidade;
}
// ⚠️ e se o processo reiniciar? Esse Map some. A contagem volta do zero.
```

**Kafka Streams** — a mesma regra, declarativa:

```java
transacoes
    .groupByKey()                                              // agrupe por conta
    .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMinutes(5)))   // janele em 5 min
    .count()                                                   // conte
    .toStream()
    .foreach((conta, qtd) -> System.out.println(conta + " -> " + qtd));
// ✅ o estado é salvo no Kafka. Reiniciou? Reconstrói e continua.
```

> Mesma regra de negócio. A diferença é **quem carrega o piano**: no primeiro
> caso, você; no segundo, a biblioteca.

---

## 🗂️ Estrutura dos projetos

Projeto **Maven multi-módulo**, **Java 21**. Cada aplicação vira um *fat-jar* e
roda no seu próprio container. Um `docker-compose` sobe o Kafka e os três apps.
**Tudo em memória — sem banco de dados.**

```
kafka-streams/
├── pom.xml                     # POM pai: versões e módulos
├── docker-compose.yml          # Kafka (KRaft, sem Zookeeper) + os 3 apps
│
├── common/                     # 📦 compartilhado pelos 3 apps
│   └── .../common/
│       ├── Transaction.java    # o evento: id, conta, valor, horário
│       └── Json.java           # helper de (de)serialização JSON
│
├── producer/                   # 🟢 PRODUCER (lib padrão)
│   └── .../producer/
│       └── ProducerApp.java
│
├── consumer-standard/          # 🔵 CONSUMER com a LIB PADRÃO
│   └── .../standard/
│       └── StandardConsumerApp.java
│
└── consumer-streams/           # 🟣 CONSUMER com KAFKA STREAMS
    └── .../streams/
        ├── StreamsConsumerApp.java
        └── JsonSerde.java
```

### O papel de cada módulo

#### 📦 `common`

O `record Transaction` (o evento que circula no tópico) e o helper `Json`.
Compartilhado para os três apps falarem a mesma língua.

#### 🟢 `producer` — usa a lib padrão

Simula o sistema de pagamentos, publicando transações continuamente com
`KafkaProducer`.

> **Por que o producer usa a lib padrão?** Porque **produzir mensagem é igual nas
> duas abordagens.** O Kafka Streams não muda nada aqui. A diferença entre as
> duas ferramentas aparece **no consumo** — e é lá que a POC concentra a
> comparação.

#### 🔵 `consumer-standard` — usa `KafkaConsumer`

- **Regra 1:** um `if` dentro do loop de `poll()`. Trivial.
- **Regra 2:** exige um `Map` na memória, calcular **na mão** em qual janela de 5
  minutos a transação caiu e **zerar o contador na mão** quando a janela vira.

**Limitação central:** esse `Map` vive **apenas na memória do processo**. Se ele
reiniciar, a contagem **volta do zero**.

#### 🟣 `consumer-streams` — usa Kafka Streams

- **Regra 1:** um `filter` + `foreach`. Equivalente ao `if`.
- **Regra 2:** `groupByKey().windowedBy(5 min).count()`. **Três linhas.**

**Ganho central:** sem `Map`, sem cálculo de janela, sem limpeza manual — e o
estado fica num **State Store com backup no Kafka**. Se o processo reiniciar,
ele **reconstrói a contagem e continua**.

---

## 🎬 A prova final: tolerância a falhas

Na apresentação, os dois consumidores são **destruídos e recriados do zero**
(containers com disco limpo). Depois, comparando a **mesma conta na mesma janela**:

| | `ACC-002` na janela atual |
|---|---|
| 🔵 **consumer-standard** | **17** ← 💀 perdeu o `Map` da memória, recomeçou do zero |
| 🟣 **consumer-streams** | **53** ← ✅ reconstruiu o estado a partir do Kafka e continuou |

As **~36 transações de diferença** são exatamente o que o consumidor padrão
**esqueceu** ao reiniciar. O Streams subiu com o disco vazio e ainda assim voltou
com o número certo — restaurando o State Store a partir do **tópico interno de
changelog**, sem uma única linha de código escrita para isso.

---

> 💡 **Mensagem final:** a lib padrão dá controle total e é perfeita para
> processamento **sem estado**. O Kafka Streams brilha quando o problema é
> **stateful** — ele elimina exatamente o código chato e propenso a bugs de
> gerenciar **estado**, **tempo** e **falha**.
> **Escolha pela natureza do problema, não pela moda.**
