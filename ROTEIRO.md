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

## ❓ Perguntas prováveis (respostas curtas)

- **"Precisa subir um cluster novo?"**
  Não. É uma biblioteca Java — roda como qualquer app nosso, num JAR.

- **"E se eu subir 2 instâncias do consumidor Streams?"**
  Com o mesmo `application.id`, o Kafka redistribui as partições entre elas
  automaticamente. Escala horizontal de graça.

- **"Onde exatamente fica o estado?"**
  Local (RocksDB/memória), com backup contínuo num tópico interno de *changelog*
  no próprio Kafka. Por isso sobrevive a restart — foi o que acabamos de ver.

- **"Serve para ler de banco / chamar API externa?"**
  Não diretamente — Streams é Kafka → Kafka. Para integrar com sistemas externos,
  o par dele é o Kafka Connect (ou o seu próprio código).

- **"E exactly-once?"**
  Uma linha: `processing.guarantee=exactly_once_v2`. Na lib padrão, é um trabalho
  considerável de transações manuais.

- **"Dá para fazer tudo isso na lib padrão?"**
  Dá — e foi o que fizemos no `consumer-standard`. A questão não é *possível*,
  é **quanto código seu você quer escrever, testar e manter**.

- **"Por que o producer não usa Streams?"**
  Porque produzir é igual nas duas abordagens. Streams é sobre **processar**.

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
