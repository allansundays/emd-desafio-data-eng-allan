# Desafio de Data Engineer - EMD

Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados no Escritório de Dados do Rio de Janeiro

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de uma API instantânea. A API consiste nos dados de GPS do BRT que são gerados na hora da consulta com o último sinal transmitido por cada veículo.

Para o desafio, será necessário construir uma pipeline que captura os dados minuto a minuto e gera um arquivo no formato CSV. O arquivo gerado deverá conter no mínimo 10 minutos de dados capturados (estruture os dados da maneira que achar mais conveniente), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá conter o ID do onibus, posição e sua a velocidade.

A pipeline deverá ser construída subindo uma instância local do Prefect (em Python). Utilize a versão *0.15.9* do Prefect.

## O que iremos avaliar

- Completude: A solução proposta atende a todos os requisitos do desafio?
- Simplicidade: A solução proposta é simples e direta? É fácil de entender e trabalhar?
- Organização: A solução proposta é organizada e bem documentada? É fácil de navegar e encontrar o que se procura?
- Criatividade: A solução proposta é criativa? Apresenta uma abordagem inovadora para o problema proposto?
- Boas práticas: A solução proposta segue boas práticas de Python, Git, Docker, etc.?

## Atenção

- A solução desse desafio deve ser publicada em um fork deste repositório no GitHub.
- O link do repositório deve ser enviado até às 23:59, horário de Brasília, do dia 26 de julho de 2023 (quarta-feira) para o e-mail utilizado para contato com o assunto "Desafio Data Engineer - EMD".
- Você deve ser capaz de apresentar sua solução, explicando como a idealizou, caso seja aprovado(a) para a próxima etapa.

## Links de referência / utilidades

- Documentação [Prefect](https://docs-v1.prefect.io/)
- Documentação [DBT](https://docs.getdbt.com/docs/introduction)
- Instalar e configurar o
   [Prefect Server](https://docs.prefect.io/orchestration/getting-started/install.html)
   locamente com um [Docker Agent](https://docs.prefect.io/orchestration/agents/docker.html)
- Construir a pipeline de captura da [API do
   BRT](https://dados.mobilidade.rio/gps/brt)
- Repositório pipelines do [Escritorio de Dados](https://github.com/prefeitura-rio/pipelines)
- Repositório de modelos DBT do [Escritorio de Dados](https://github.com/prefeitura-rio/queries-datario)


## Dúvidas?

Fale conosco pelo e-mail que foi utilizado para o envio desse desafio.

## Configuração do ambiente de execução


## Executando a pipeline manualmente

- Crie um ambiente virtual 
```
python -m venv env
 ```

- ative o ambiente virtual 
```
env\scripts\activate
 ```
- Instale as dependências
```
pip install -r requirements.txt
 ```
- Execute o arquivo `pipeline.py`:
```
python pipeline.py
```

### Rodando via Docker

```
docker-compose up -d
```



