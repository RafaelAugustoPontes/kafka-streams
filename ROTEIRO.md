# 🎤 ROTEIRO DA DEMO — uso pessoal (não apresentar)

> Guia prático da **parte hands-on**: subir o projeto e mostrar rodando.
> A parte teórica está no `README.md` (esse sim vai para o time).
>
> ⏱️ **Duração da demo: ~6 minutos.**

---

## 🔧 PREPARAÇÃO (fazer ANTES da apresentação começar)

### 1. Subir tudo

```bash
docker compose up --build -d
```

> ⚠️ **Faça isso com pelo menos 10 minutos de antecedência.**
> O primeiro build baixa as imagens e as dependências Maven — pode levar
> vários minutos. Não deixe para a hora.

### 2. Conferir que está tudo no ar

```bash
docker compose ps
```

Esperado: **4 containers `Up`**, com o `kafka` em `(healthy)`.

### 3. Deixar acumular transações

Aguarde **uns 2 minutos** rodando antes de apresentar. Assim as contagens já
estarão em números interessantes (não em 1, 2, 3) e a demo de falha fica bem
mais impactante.

### 4. Preparar as janelas na tela

- **Terminal 1** → `docker compose logs -f producer`
- **Terminal 2** → `docker compose logs -f consumer-standard`
- **Terminal 3** → `docker compose logs -f consumer-streams`
- **Editor** → `StandardConsumerApp.java` e `StreamsConsumerApp.java` abertos

> 💡 Deixe os Terminais 2 e 3 **lado a lado**. É a imagem central da demo.

### ✅ Checklist final

- [ ] Docker Desktop aberto
- [ ] `docker compose ps` → 4 containers Up, kafka healthy
- [ ] Rodando há pelo menos 2 min
- [ ] 3 terminais abertos com os `logs -f`
- [ ] Os 2 arquivos de consumer abertos no editor
- [ ] Fonte do terminal aumentada (o time precisa enxergar)

---

## 🎬 A DEMO

### Passo 1 — O producer (30s)

**Mostrar:** Terminal 1.

```
[PRODUCER] TX-0043 | ACC-002 | R$   24728.20 <- ALTA
[PRODUCER] TX-0044 | ACC-002 | R$    4197.55
```

**Falar:**

> "Esse é o sistema de pagamentos gerando transações — uma por segundo, entre 5
> contas. Mais ou menos 1 em cada 5 é acima de 10 mil: essas vão disparar o
> alerta. Aqui não tem nada de especial, é a lib padrão. **Produzir mensagem é
> igual nas duas abordagens** — a diferença aparece no consumo."

---

### Passo 2 — Os dois consumidores rodando (1 min)

**Mostrar:** Terminais 2 e 3 lado a lado.

**Falar:**

> "Os dois estão aplicando **as mesmas duas regras**: alerta acima de 10 mil, e
> contagem por conta em janelas de 5 minutos. Reparem: **a saída é equivalente**.
> Os alertas são os mesmos, as contagens batem."

👉 **Aponte uma conta específica** (ex.: `ACC-003`) e mostre o mesmo número nos
dois terminais.

> "Ou seja: **do lado de fora, os dois resolvem o problema.** A diferença está no
> código que a gente teve que escrever para chegar até aqui. Vamos olhar."

---

### Passo 3 — O código (2,5 min) ⭐ o momento "aha"

#### Regra 1 — sem estado

Mostre nos dois arquivos, rapidamente:

```java
// StandardConsumerApp
if (tx.amount() > LIMITE_ALERTA) { ... }
```
```java
// StreamsConsumerApp
transacoes.filter((conta, tx) -> tx.amount() > LIMITE_ALERTA).foreach(...)
```

> "**Empate.** Um `if` contra um `filter`. Para regra sem estado, a lib padrão
> resolve numa linha. **Não troquem de ferramenta só por isso.**"

#### Regra 2 — com estado

**No `StandardConsumerApp.java`** → mostre o método `contarNaJanela`:

> "Para contar, eu preciso **lembrar** do que já passou. A lib padrão não me dá
> estado nenhum. Então eu tive que: criar um **`Map` na memória**, calcular
> **na mão** em qual bloco de 5 minutos cada transação cai, e **zerar o contador
> na mão** toda vez que a janela vira."

👉 **Aponte o `if (contador.janela != janelaAtual)`** e emende:

> "E tem um detalhe cruel: **isso vive só na memória deste processo.**
> Se ele reiniciar, o `Map` some e a contagem volta do zero.
> **Guardem essa frase** — vou provar ela daqui a pouco."

**No `StreamsConsumerApp.java`** → mostre as 3 linhas:

```java
transacoes
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .count()
```

> "Mesma regra. **Três linhas.** Sem `Map`, sem cálculo de janela, sem limpeza
> manual. Eu digo O QUE quero — agrupe por conta, janele em 5 minutos, conte —
> e a biblioteca resolve COMO."

---

### Passo 4 — 💥 O golpe final: tolerância a falhas (2 min)

> "Lembram do detalhe cruel? Vamos provar. Vou **destruir os dois consumidores**
> — containers zerados, disco limpo — e subir de novo."

#### 4.1 — Anote os números ANTES

Escolha uma conta (ex.: `ACC-002`) e **anote a contagem atual** nos dois
terminais. Fale o número em voz alta para o time gravar.

#### 4.2 — Destrua os dois

```bash
docker compose up -d --force-recreate consumer-standard consumer-streams
```

> ⚠️ **Use `--force-recreate`, NÃO `docker compose restart`.**
> O `restart` reinicia o mesmo container e **preserva o disco** — o state store
> local do Streams sobreviveria sem precisar do Kafka, e alguém do time poderia
> (com razão) dizer que a demo não provou nada. O `--force-recreate` recria os
> containers **do zero**, forçando o Streams a reconstruir o estado a partir do
> tópico interno do Kafka.

#### 4.3 — Aguarde ~20s e compare

Compare a **mesma conta**, na **mesma janela**, nos dois terminais:

| | `ACC-002` na janela atual |
|---|---|
| 🔵 **consumer-standard** | **17** ← 💀 perdeu tudo, recomeçou do zero |
| 🟣 **consumer-streams** | **53** ← ✅ restaurou o estado e continuou |

**Falar:**

> "Olhem a diferença. O consumidor padrão está em **17**, o Streams em **53** —
> mesma conta, mesma janela de 5 minutos. Aquelas ~36 transações de diferença são
> **exatamente o que o consumidor padrão esqueceu** quando reiniciou. O `Map`
> dele estava só na memória.
>
> O Streams subiu com **o disco vazio** e mesmo assim voltou com o número certo:
> ele **reconstruiu o estado a partir de um tópico interno do próprio Kafka**.
> E eu **não escrevi uma linha** para isso acontecer."

#### ⚠️ Se der ruim

- **Os dois zeraram juntos?** A janela de 5 min virou bem na hora. É só esperar
  acumular umas 15-20 transações e repetir o `--force-recreate`.
- **Os números estão baixos demais?** Deixe rodar mais um pouco antes de derrubar.

---

## 🎯 Fechamento (30s)

> "Se a gente só precisasse do **alerta**, Kafka Streams seria exagero — a lib
> padrão resolve. Foi a **contagem com estado** que justificou a troca.
>
> A pergunta que decide não é 'qual é mais moderno'. É:
> **'para processar a mensagem atual, eu preciso lembrar das anteriores?'**
> Se não, lib padrão. Se sim, o Streams economiza muito código — e muito bug."

👉 Aponte o time para o `README.md` (teoria, comparação completa e estrutura).

---

## 🧹 DEPOIS DA APRESENTAÇÃO

```bash
docker compose down
```

---

## 🆘 Troubleshooting

| Problema | Solução |
|---|---|
| `docker compose ps` mostra container saindo | `docker compose logs <serviço>` para ver o erro |
| Consumidores sem imprimir nada | O producer está rodando? `docker compose logs producer` |
| Contagens não batem entre os dois | Suba do zero: `docker compose down` e `docker compose up --build -d` |
| Build muito lento | Normal na 1ª vez (baixa imagens + dependências Maven). Faça antes. |
| Quer inspecionar o tópico cru | `docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning` |

---
---

# ❓ PERGUNTAS QUE PODEM TE FAZER

> Respostas diretas ao ponto. Se não souber alguma, a melhor resposta é
> **"não sei, vou levantar e te respondo"** — nunca invente.

## 🔧 Sobre a ferramenta

**"Precisa subir um cluster novo? Um servidor a mais?"**
Não. É uma **biblioteca Java**, uma dependência no `pom.xml`. Roda como qualquer
app nosso, num JAR. Zero infraestrutura nova.

**"Então qual a diferença para o Spark Streaming / Flink?"**
Spark e Flink são **frameworks distribuídos** — exigem um cluster próprio para
operar. Kafka Streams é só uma **lib** dentro do seu app. Menos poder para
cenários gigantes e multi-fonte, muito menos complexidade operacional.

**"E o ksqlDB? Faz a mesma coisa?"**
Por baixo, o ksqlDB **é Kafka Streams**, com uma camada de SQL por cima. Bom para
quem não quer escrever Java. Kafka Streams te dá mais controle e cabe no nosso
build/deploy normal.

**"Precisa de Schema Registry / Avro?"**
Não. Aqui usamos JSON com um Serde próprio. Avro + Schema Registry é uma boa
prática para contratos entre times, mas é ortogonal — não é requisito do Streams.

## 💾 Sobre estado e falhas

**"Onde exatamente fica o estado?"**
Local na instância (RocksDB ou memória), com **backup contínuo num tópico interno
do Kafka** (o *changelog*). Por isso sobrevive a restart — foi o que a gente
acabou de ver na demo.

**"Se a máquina morrer de vez, o estado vai junto?"**
Não. O estado local se perde, mas o **changelog está no Kafka**. Uma nova
instância lê esse tópico e **reconstrói o estado**. Foi exatamente isso que o
`--force-recreate` provou: subiu com disco vazio e voltou com o número certo.

**"Essa reconstrução não é lenta?"**
Pode ser, se o estado for grande — é uma desvantagem real. Mitigações: *standby
replicas* (réplicas quentes do estado em outras instâncias) e volumes
persistentes, que evitam a restauração completa.

**"Quanto de disco/memória isso consome?"**
Depende do número de chaves e do tamanho das janelas. Estado grande **pesa** — e
essa é uma das desvantagens que citei. Precisa ser dimensionado, não é mágica.

## ⚙️ Sobre escala e operação

**"E se eu subir 2 instâncias do consumidor Streams?"**
Com o **mesmo `application.id`**, o Kafka redistribui as partições entre elas
automaticamente — cada instância fica com um pedaço do estado. Escala horizontal
de graça.

**"E se eu tiver mais instâncias do que partições?"**
As sobrando ficam **ociosas**. O paralelismo máximo é o número de partições —
igualzinho ao consumer group da lib padrão.

**"Quem cria esses tópicos internos?"**
O próprio Kafka Streams, na inicialização. Eles aparecem com o prefixo do
`application.id`. **É importante o time saber que eles existem** — contam no
monitoramento, no disco do broker e nas políticas de retenção.

**"Como a gente monitora isso em produção?"**
O Streams expõe métricas via **JMX** (lag, throughput, tempo de restauração,
estado das threads). Integra com Prometheus/Grafana como qualquer app JVM.

## 🤔 Sobre a decisão (as mais importantes)

**"Dá para fazer tudo isso na lib padrão?"**
**Dá — e foi o que eu fiz no `consumer-standard`.** A questão nunca foi
*possível*. É **quanto código seu você quer escrever, testar e manter**.

**"Então devemos migrar tudo para Streams?"**
**Não.** Para consumo sem estado — ler, validar, chamar serviço, gravar no banco —
a lib padrão é melhor: mais simples e todo mundo já domina. Migre só o que é
**stateful**.

**"Por que o producer não usa Streams?"**
Porque **produzir é igual nas duas abordagens**. Streams é sobre **processar**.

**"Vale a pena o time aprender? Qual o custo?"**
A curva existe: KStream, KTable, serdes, event-time. Uma a duas semanas para
ficar produtivo. **Vale se a gente tiver casos com estado.** Se não tiver, não vale.

**"Temos algum caso real nosso que se encaixa?"**
👉 **Prepare essa resposta antes.** Pense em 1 ou 2 casos concretos do nosso
domínio (detecção de fraude por janela, contadores por cliente, enriquecimento
juntando dois tópicos, agregações para dashboard). É o que transforma a
apresentação em decisão.

## 🎯 Sobre a demo

**"Por que os números dos dois batem antes, mas não depois do restart?"**
Porque o padrão perdeu o `Map` da memória e recomeçou do zero; o Streams
reconstruiu o estado a partir do Kafka. **É esse o ponto da demo.**

**"Essa janela de 5 minutos é do relógio ou do evento?"**
No Streams, do **evento** (event-time) — usa o timestamp da mensagem. No consumer
padrão, eu usei o timestamp da transação também, mas **na mão**. Se eu tivesse
usado o relógio do servidor (o natural), uma mensagem atrasada cairia na **janela
errada**.

**"E se chegar uma mensagem muito atrasada?"**
O Streams tem *grace period* — você configura quanto tempo esperar por eventos
atrasados antes de fechar a janela. Aqui usei `ofSizeWithNoGrace` (sem tolerância)
para simplificar.

**"E exactly-once, funciona mesmo?"**
Uma linha: `processing.guarantee=exactly_once_v2`. Na lib padrão, é um trabalho
considerável de transações manuais. **Não usei nesta POC** para não poluir o
código — mas está a uma config de distância.
