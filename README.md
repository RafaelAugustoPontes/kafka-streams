# 📊 Kafka Streams vs. Lib Padrão — Demo para o Time

> **Em uma frase:** um producer gera **transações financeiras** e dois consumidores
> aplicam **as mesmas duas regras** — um usando a **lib padrão** do Kafka e outro
> usando **Kafka Streams**. O objetivo é ver o mesmo problema resolvido das duas
> formas e entender **quando vale a pena cada uma**.

📖 Para apresentar, siga o passo a passo do [ROTEIRO.md](ROTEIRO.md).

---

## 💰 O cenário

Um sistema de pagamentos gerando **1 transação por segundo**, de 5 contas
(`ACC-001` a `ACC-005`). Cada transação é uma mensagem JSON no tópico
`transactions`:

```json
{ "id": "TX-0042", "accountId": "ACC-003", "amount": 12500.00, "timestamp": 1720000000000 }
```

A **chave** da mensagem é o `accountId` — assim todas as transações de uma conta
caem na mesma partição e podem ser agrupadas.

### As duas regras que os consumidores aplicam

| | Regra | O que ela exige |
|---|---|---|
| **1️⃣** | Transação **acima de R$ 10.000** → dispara um **ALERTA** | Só olhar a mensagem atual. **Sem estado.** |
| **2️⃣** | **Contar** as transações de cada conta em **janelas de 5 minutos** | Precisa **lembrar** das mensagens anteriores. **Com estado.** |

> 🔑 **Essa é a chave da apresentação inteira.** A Regra 1 é fácil nas duas
> abordagens. É a **Regra 2** que separa as duas ferramentas.

---

## 🧩 Primeiro: o que a lib padrão faz (e o que não faz)

Quando falamos "lib padrão", falamos do **`kafka-clients`** — o que todo mundo já usa:

- **`KafkaProducer`** → envia mensagens para um tópico.
- **`KafkaConsumer`** → busca mensagens num loop de `poll()`.

Ela é ótima. Mas repare no que ela **não** faz por você:

- ❌ Não guarda **estado**. Precisa contar, somar ou lembrar do passado?
  O acumulador é problema seu.
- ❌ Não entende **janela de tempo**. "Nos últimos 5 minutos" você calcula na mão.
- ❌ Não **recupera** nada se a aplicação cair e voltar. O que estava na memória, morreu.

Para "ler mensagem → chamar API → gravar no banco", nada disso importa.
**O problema começa quando o consumo precisa lembrar do passado.**

---

## 🎯 O que é Kafka Streams?

- É uma **biblioteca Java** — você adiciona **uma dependência** no `pom.xml`.
  **Não** é servidor, **não** é cluster, **não** tem nada para instalar.
- Sua aplicação continua um Java comum (`public static void main`), rodando
  em container, VM ou Kubernetes.
- A diferença é **como você escreve o processamento**: em vez de um loop de
  `poll()` com lógica manual, você **descreve** o que quer (a *topologia*):

  ```
  pegue o fluxo → agrupe por conta → janele em 5 min → conte
  ```

- Você diz **O QUE** quer. A biblioteca resolve **COMO**: estado, tempo,
  falhas e paralelismo.

### O superpoder: o State Store

Quando você usa `count()`, `aggregate()` ou um `join`, o Streams guarda o
resultado parcial num **State Store**:

- fica **local** (rápido) — mas com **backup automático em um tópico interno do Kafka**;
- se a aplicação **cair e subir de novo**, ela **recupera o estado** e continua
  de onde parou.

É exatamente isso que o `Map` em memória do consumer padrão **não** consegue fazer.

---

## 🆚 O contraste em código

### Regra 1 (sem estado) — praticamente igual nas duas

```java
// LIB PADRÃO — dentro do loop de poll
if (tx.amount() > 10_000) {
    System.out.println("*** ALERTA *** " + tx.id());
}
```

```java
// KAFKA STREAMS
transacoes
    .filter((conta, tx) -> tx.amount() > 10_000)
    .foreach((conta, tx) -> System.out.println("*** ALERTA *** " + tx.id()));
```

> Empate. Para regra simples, a lib padrão resolve numerinho. **Não use Streams
> só para isso.**

### Regra 2 (com estado) — aqui a diferença aparece

**Lib padrão** — eu preciso criar o estado, descobrir a janela **na mão** e zerar o
contador **na mão** quando ela virar:

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
// ✅ o estado é salvo no Kafka. Reiniciou? Recupera e continua.
```

> Mesma regra de negócio. A diferença é **quem carrega o piano**: no primeiro
> caso, você; no segundo, a biblioteca.

---

## 📋 A comparação, ponto a ponto

| Aspecto | Lib padrão (`kafka-clients`) | Kafka Streams |
|---|---|---|
| **Estilo** | Imperativo: você escreve o loop e a lógica | Declarativo: você descreve a transformação |
| **Regra sem estado** (alerta) | ✅ Simples e direto | ✅ Simples e direto (empate) |
| **Regra com estado** (contagem) | Você cria e mantém o `Map` na mão | `count()` — a lib cuida do estado |
| **Janela de tempo** | Cálculo manual (limpar o que expirou) | `windowedBy(5 min)` — uma linha |
| **App reiniciou, e o estado?** | 💀 Perdeu tudo. Contagem do zero. | ✅ Recupera do Kafka e continua |
| **Mensagem atrasada** | Entra na conta errada (usa o relógio local) | Entra na janela certa (usa o horário do evento) |
| **Exactly-once** | Transações manuais, trabalhoso | 1 config: `processing.guarantee=exactly_once_v2` |
| **Join entre tópicos** | Você implementa tudo | `join` / `leftJoin` prontos |
| **Escalar** | Você coordena instâncias e partições | Sobe outra instância — rebalance automático |
| **Curva de aprendizado** | Baixa | Média |

---

## ✅ Vantagens do Kafka Streams

- **Estado sem esforço** — contar, somar, agrupar e janelar são operações prontas.
- **Estado à prova de falhas** — backup automático em tópico interno.
  Dá para provar ao vivo (veja "A demonstração mais forte", abaixo).
- **Entende tempo de verdade** — usa o horário do **evento**, não o do servidor;
  mensagem atrasada cai na janela correta.
- **Exactly-once com 1 linha** de configuração.
- **Escala sozinho** — instâncias com o mesmo `application.id` dividem as partições.
- **Zero infraestrutura nova** — é só um JAR.

## ❌ Desvantagens / quando NÃO usar

- **Curva de aprendizado** — KStream, KTable, serdes, janelas... exige estudo.
  Para "ler e gravar", é complexidade desnecessária.
- **Só Kafka → Kafka** — entrada e saída são tópicos. Integrar com banco/API
  externa é papel do seu código ou do Kafka Connect.
- **Só JVM** — time Python ou Go precisa de outra solução.
- **Estado ocupa disco e memória** — muitas chaves e janelas longas pesam.
- **Debug menos óbvio** — a lib cria tópicos internos (*changelog*, *repartition*)
  que você precisa conhecer para operar bem.

## 🧭 Regra de bolso

- ✅ **Lib padrão** → consumo **sem estado**: ler mensagem → validar → chamar
  serviço → gravar no banco. E **todos os producers**.
- ✅ **Kafka Streams** → consumo **com estado**: contar, somar, agregar, janelar
  por tempo, juntar dois tópicos, detectar padrões.

> Nesta POC: se a gente só precisasse do **alerta** (Regra 1), Kafka Streams
> seria exagero. É a **contagem por janela** (Regra 2) que justifica a troca.

---

## 🚀 Como rodar

Pré-requisito: só **Docker** (não precisa de Java nem Maven — o build acontece
dentro do container).

```bash
docker compose up --build
```

### Acompanhando (recomendado: 3 terminais)

```bash
docker compose logs -f producer            # as transações sendo geradas
docker compose logs -f consumer-standard   # as 2 regras, feitas na mão
docker compose logs -f consumer-streams    # as 2 regras, com Kafka Streams
```

Os dois consumidores estão em **grupos diferentes**, então **ambos recebem todas
as transações** — dá para comparar a saída lado a lado.

### 💥 A demonstração mais forte: tolerância a falhas

Anote a contagem de uma conta nos dois consumidores e **destrua os dois
containers** (disco zerado, não é um simples `restart`):

```bash
docker compose up -d --force-recreate consumer-standard consumer-streams
```

Depois de ~20s, compare a **mesma conta na mesma janela**:

| | `ACC-002` na janela atual |
|---|---|
| 🔵 **consumer-standard** | **17** ← 💀 perdeu o `Map` da memória, recomeçou do zero |
| 🟣 **consumer-streams** | **53** ← ✅ reconstruiu o estado a partir do Kafka e continuou |

As ~36 transações de diferença são **exatamente o que o consumer padrão esqueceu**.
O Streams subiu com o disco vazio e ainda assim voltou com o número certo — ele
restaura o State Store a partir do **tópico interno de changelog**. Sem uma linha
de código para isso.

### Encerrando

```bash
docker compose down
```

---

## 🗂️ Estrutura dos projetos

Projeto **Maven multi-módulo** (Java 21). Cada aplicação vira um *fat-jar* e roda
no seu próprio container.

```
kafka-streams/
├── pom.xml                     # POM pai: versões e módulos
├── docker-compose.yml          # sobe Kafka (KRaft) + os 3 apps
├── README.md                   # este arquivo
├── ROTEIRO.md                  # passo a passo da apresentação
│
├── common/                     # 📦 compartilhado pelos 3 apps
│   └── .../common/
│       ├── Transaction.java    # o evento: id, conta, valor, horário
│       └── Json.java           # helper de (de)serialização JSON
│
├── producer/                   # 🟢 PRODUCER (lib padrão)
│   └── .../producer/
│       └── ProducerApp.java    # gera 1 transação/s; ~1 em 5 é acima de 10 mil
│
├── consumer-standard/          # 🔵 CONSUMER com a LIB PADRÃO
│   └── .../standard/
│       └── StandardConsumerApp.java  # loop de poll + Map manual + limpeza na mão
│
└── consumer-streams/           # 🟣 CONSUMER com KAFKA STREAMS
    └── .../streams/
        ├── StreamsConsumerApp.java   # a topologia: filter + groupByKey/count
        └── JsonSerde.java            # ensina o Streams a ler/gravar JSON
```

### O papel de cada módulo

- **`common`** — o `record Transaction` (o evento) e o helper `Json`.
  Compartilhado para os três apps falarem a mesma língua.
- **`producer`** — usa a lib padrão (`KafkaProducer`) para publicar transações.
  Mostra que **produzir é igual nas duas abordagens**.
- **`consumer-standard`** — usa `KafkaConsumer`. A Regra 1 é um `if`; a Regra 2
  exige um `Map` na memória, calcular **na mão** em qual janela a transação caiu
  e **zerar o contador na mão** quando ela vira. Funciona — mas **perde tudo num
  restart**.
- **`consumer-streams`** — usa Kafka Streams. A Regra 1 é um `filter`; a Regra 2
  é `groupByKey().windowedBy(5min).count()`. **Sem estado manual, com recuperação
  automática.**

---

> 💡 **Mensagem final para o time:** a lib padrão dá controle total e é perfeita
> para consumo **sem estado**. O Kafka Streams brilha quando o problema é
> **stateful** — ele elimina exatamente o código chato e propenso a bugs de
> gerenciar **estado**, **tempo** e **falha**. Escolham pela natureza do problema.
