# 🎤 Roteiro da Apresentação — Kafka Streams vs. Lib Padrão

> Duração estimada: **10 minutos**.
> Antes de começar: `docker compose up --build -d`, abra 3 terminais e
> os 2 arquivos de consumer no editor.

---

## ✅ Checklist antes de apresentar

- [ ] Docker Desktop aberto
- [ ] `docker compose up --build -d` executado (aguarde ~30s)
- [ ] `docker compose ps` mostra os 4 containers `Up` (kafka `healthy`)
- [ ] Terminal 1: `docker compose logs -f producer`
- [ ] Terminal 2: `docker compose logs -f consumer-standard`
- [ ] Terminal 3: `docker compose logs -f consumer-streams`
- [ ] Editor com `StandardConsumerApp.java` e `StreamsConsumerApp.java`

---

## 1️⃣ Abertura — o problema (1 min)

> "Todo mundo aqui já produziu e consumiu mensagem no Kafka. Isso é o básico.
> A pergunta de hoje é outra: **e quando o consumo precisa de ESTADO?**
> Ou seja, quando processar a mensagem atual exige **lembrar das anteriores**."

**Apresente o cenário:**

> "Montei um sistema de pagamentos: um producer gera **1 transação por segundo**,
> de 5 contas. E dois consumidores aplicam **as mesmas duas regras** —
> um com a lib padrão, outro com Kafka Streams."

**Escreva as duas regras no quadro/slide:**

- **Regra 1** — transação **acima de R$ 10.000** → dispara um **alerta**
- **Regra 2** — **contar** as transações de cada conta em **janelas de 5 minutos**

> "Guardem essa diferença: a **Regra 1 só olha a mensagem atual**. A **Regra 2
> precisa lembrar do passado**. É isso que vai separar as duas ferramentas."

---

## 2️⃣ Producer rodando (1 min)

**Mostrar:** Terminal 1.

> "Esse é o sistema gerando transações. Uma por segundo, e mais ou menos
> **1 em cada 5 é acima de 10 mil** — essas vão disparar o alerta.
> Aqui não tem nada de especial: é a lib padrão, `KafkaProducer`.
> **Produzir mensagem é igual nas duas abordagens.** A diferença é no consumo."

---

## 3️⃣ Os dois consumidores rodando (2 min)

**Mostrar:** Terminais 2 e 3 lado a lado.

> "Os dois estão fazendo a mesma coisa: disparando alerta nas transações altas
> e contando quantas transações cada conta fez. **A saída é equivalente.**"

> "Ou seja: **do lado de fora, os dois resolvem o problema.** A diferença está
> no código que a gente teve que escrever. Vamos olhar."

---

## 4️⃣ O momento "aha" — o código (4 min)

### Regra 1 (sem estado) — mostre nos dois arquivos

**No `StandardConsumerApp.java`:**
```java
if (tx.amount() > LIMITE_ALERTA) { ... }
```

**No `StreamsConsumerApp.java`:**
```java
transacoes.filter((conta, tx) -> tx.amount() > LIMITE_ALERTA).foreach(...)
```

> "**Empate.** Um `if` contra um `filter`. Para regra simples, sem estado, a lib
> padrão resolve numa linha. **Não troquem de ferramenta só por isso.**"

### Regra 2 (com estado) — o contraste

**No `StandardConsumerApp.java`** — mostre o método `contarNaJanela`:

> "Para contar, eu preciso **lembrar** do que já passou. A lib padrão não me dá
> estado nenhum. Então eu tive que: criar um **`Map` na memória**, calcular
> **na mão** em qual bloco de 5 minutos cada transação cai, e **zerar o contador
> na mão** toda vez que a janela vira."

Aponte o `if (contador.janela != janelaAtual)` e emende:

> "E tem um detalhe cruel: **isso vive só na memória deste processo.**
> Se ele reiniciar, o `Map` some e a contagem volta do zero."

**No `StreamsConsumerApp.java`** — mostre as 3 linhas:

```java
transacoes
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
    .count()
```

> "Mesma regra. Três linhas. **Sem `Map`, sem limpeza manual, sem se preocupar
> com tempo.** Eu digo O QUE quero — agrupe por conta, janele em 5 minutos,
> conte — e a biblioteca resolve COMO."

---

## 5️⃣ 💥 O golpe final — tolerância a falhas ao vivo (2 min)

> "E aquele detalhe cruel do estado em memória? Vamos provar. Vou **destruir
> os dois consumidores** — containers zerados, disco limpo — e subir de novo."

**Antes de derrubar**, anote a contagem de uma conta nos dois terminais
(ex.: `ACC-002`).

**Execute na frente de todos:**

```bash
docker compose up -d --force-recreate consumer-standard consumer-streams
```

*(`--force-recreate` recria os containers do zero. Um `restart` simples não
serviria: ele preservaria o disco do container, e alguém poderia argumentar que
o Streams só releu o arquivo local.)*

**Aguarde ~20s e compare a MESMA conta, na MESMA janela, nos dois terminais:**

| | `ACC-002` na janela atual |
|---|---|
| 🔵 **consumer-standard** | **17** ← 💀 perdeu tudo, recomeçou do zero |
| 🟣 **consumer-streams** | **53** ← ✅ restaurou o estado e continuou |

> "Olhem a diferença. O consumidor padrão está contando **17**, o Streams está
> contando **53** — na mesma conta, na mesma janela de 5 minutos.
> Aquelas ~36 transações de diferença são **exatamente o que o consumidor padrão
> esqueceu** quando reiniciou. O `Map` dele estava só na memória.
>
> O Streams subiu com o disco vazio e mesmo assim voltou com o número certo:
> ele **reconstruiu o estado a partir de um tópico interno do próprio Kafka**.
> Isso é de graça — não escrevi uma linha para isso."

⚠️ **Atenção ao apresentar:** a janela é de 5 minutos e é fixa (blocos alinhados
no relógio). Se ela virar bem na hora da demo, os dois zeram juntos e o efeito se
perde. Se isso acontecer, é só esperar acumular algumas transações e repetir.

---

## 6️⃣ Fechamento — quando usar cada um (1 min)

**Mostrar:** a tabela comparativa do `README.md`.

**A regra de bolso para deixar com o time:**

- ✅ **Lib padrão** → consumo **sem estado**: ler → validar → chamar serviço →
  gravar no banco. E **todos os producers**.
- ✅ **Kafka Streams** → consumo **com estado**: contar, somar, agregar, janelar
  por tempo, juntar tópicos.

**Frase final:**

> "Se a gente só precisasse do **alerta**, Kafka Streams seria exagero.
> Foi a **contagem por janela de tempo** que justificou a troca.
> A lib padrão dá controle total e é perfeita para o simples. O Streams elimina
> exatamente o código chato e propenso a bug: gerenciar **estado**, **tempo** e
> **falha**. **Escolham pela natureza do problema, não pela moda.**"

---

## ❓ Perguntas prováveis (respostas curtas)

- **"Precisa subir um cluster novo?"**
  Não. É uma biblioteca Java — roda como qualquer app nosso.

- **"E se eu subir 2 instâncias do consumidor Streams?"**
  Com o mesmo `application.id`, o Kafka rebalanceia as partições entre elas
  automaticamente. Escala horizontal de graça.

- **"Onde exatamente fica o estado?"**
  Local (RocksDB/memória), com backup contínuo num tópico interno de *changelog*
  no próprio Kafka. Por isso sobrevive a restart.

- **"Serve para ler de banco / chamar API externa?"**
  Não diretamente — Streams é Kafka → Kafka. Para integrar com sistemas
  externos, o par dele é o Kafka Connect (ou seu próprio código).

- **"E exactly-once?"**
  Uma linha: `processing.guarantee=exactly_once_v2`. Na lib padrão, é um
  trabalho considerável de transações manuais.

- **"Dá para fazer isso tudo na lib padrão?"**
  Dá — e foi o que fizemos. A questão não é *possível*, é *quanto código
  seu você quer escrever, testar e manter*.

---

## 🧹 Depois da apresentação

```bash
docker compose down
```
