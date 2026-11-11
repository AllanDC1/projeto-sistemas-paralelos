# Sistema de mensagens

Sistema simples de troca de mensagens em canais. Os usuários são bots que fazem login, criam e listam canais, publicam mensagens e recebem as publicações dos canais em que estão inscritos. O sistema tem 2 servidores, um broker, um proxy Pub/Sub e um serviço de referência, todos comunicando por ZeroMQ na rede do Docker Compose.

```sh
docker compose up --build --menu=false
```

Sobem 2 servidores, 2 bots, o broker, o proxy e o serviço de referência. Os bots agem sozinhos e todas as mensagens enviadas e recebidas aparecem nos logs, no formato `hora [relogio] direcao conteudo`.

## Escolhas

- **Linguagens:** Java nos bots e Python nos servidores, no broker, no proxy e no serviço de referência.
- **Serialização:** Protocol Buffers (`protocol/chat.proto`). Toda mensagem é um `Envelope` com o timestamp do envio e o valor do relógio lógico de quem enviou.
- **Persistência:** SQLite, com um arquivo por servidor em um volume Docker próprio. Cada servidor guarda os logins, os canais e as mensagens publicadas, todos com timestamp.

## Troca de mensagens

- **Pedidos dos bots (Req/Rep):** login, criação de canal, listagem de canais, publicação e histórico de um canal. Passam pelo broker, que reparte os pedidos entre os servidores.
- **Publicações (Pub/Sub):** ao receber um pedido de publicação, o servidor grava a mensagem e a publica no proxy usando o nome do canal como tópico. Cada bot se inscreve em um canal e exibe as publicações recebidas com os timestamps de envio e de recebimento.
- **Armazenamento das publicações:** cada mensagem é gravada na tabela `messages` com usuário, canal, texto e timestamp.

## Replicação

- **Método:** replicação ativa. O servidor que atende um pedido de escrita (login, criação de canal ou publicação) grava no próprio banco e envia o mesmo pedido aos outros servidores pelo proxy Pub/Sub, no tópico `replication`. Cada servidor que o recebe aplica a mesma operação no seu banco.
- **Como resolve o problema:** todos os servidores acabam com os mesmos dados. Se um deles parar, os outros continuam com o histórico completo e qualquer um responde o histórico de um canal.
- **Adaptação ao projeto:** a propagação reutiliza o Pub/Sub já existente, sem canal novo entre servidores, e a consistência é eventual: o bot recebe a resposta sem esperar as outras réplicas gravarem.
- **Limite:** não há ressincronização. Um servidor que ficar fora do ar perde o que for publicado nesse período.
