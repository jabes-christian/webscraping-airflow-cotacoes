# 🚀 Web Scraping — Cotações de Moedas

Web Scraping/ETL com Apache Airflow + PostgreSQL + Docker extraindo dados de um .csv de cotações de moedas do Banco Central para um Base de Dados.

---

## 📁 Estrutura do Projeto

```
meu-projeto/
├── dags/                   # DAGs do Airflow (seus arquivos .py)
│    └── cotacoes.py         # arquivo.py do scraping
├── logs/                   # Logs gerados automaticamente
├── plugins/                # Plugins customizados do Airflow
├── secrets/
│   ├── variables.yaml      # Variáveis de ambiente do Airflow
│   └── connections.yaml    # Conexões (banco, APIs, etc.)
├── .env                    # Variáveis do Docker Compose
├── .gitignore
├── docker-compose.yml
├── Dockerfile              # Apenas se usar Opção B
└── requirements.txt        # Apenas se usar Opção B
```

---

## ⚙️ Como usar este template em um novo projeto

### 1. Copie a estrutura
```bash
cp -r airflow-template/ meu-novo-projeto/
cd meu-novo-projeto/
```

### 2. Escolha como instalar dependências Python

**Opção A — Rápida (sem Dockerfile):**
No `.env`, adicione os pacotes na variável:
```env
_PIP_ADDITIONAL_REQUIREMENTS=requests pandas psycopg2-binary
```
> ⚠️ Reinstala a cada restart do container. Boa para poucos pacotes.

**Opção B — Recomendada (com Dockerfile):**
1. Adicione as dependências no `requirements.txt`
2. No `docker-compose.yml`, comente `image:` e descomente `build: .`
3. Execute `docker compose build` antes do primeiro `up`

### 3. Configure as conexões e variáveis
Edite `secrets/connections.yaml` e `secrets/variables.yaml` conforme o projeto.

### 4. Suba o ambiente
```bash
# Primeira vez (inicializa o banco e cria o usuário admin)
docker compose up airflow-init

# Após o init finalizar, suba os serviços
docker compose up -d airflow-webserver airflow-scheduler
```

### 5. Acesse a UI
- **URL:** http://localhost:8080
- **Usuário:** `admin` (definido no `.env`)
- **Senha:** `admin` (definido no `.env`)

---

## 🗄️ Conectar DBeaver no PostgreSQL

| Campo    | Valor       |
|----------|-------------|
| Host     | `localhost` |
| Porta    | `5432`      |
| Banco    | `airflow`   |
| Usuário  | `airflow`   |
| Senha    | `airflow`   |

> 💡 Para separar dados do ETL dos metadados do Airflow, crie um schema dedicado:
> ```sql
> CREATE SCHEMA meu_projeto;
> ```

---

## 🛠️ Comandos úteis

```bash
# Ver logs em tempo real
docker compose logs -f

# Ver status dos containers
docker compose ps

# Parar tudo
docker compose down

# Parar e remover volumes (APAGA O BANCO)
docker compose down -v

# Recriar imagem customizada (Opção B)
docker compose build --no-cache
```

---

## 🔐 Segurança

- **Nunca versione** o arquivo `.env` e a pasta `secrets/` — já estão no `.gitignore`
- Em produção, troque as senhas padrão (`airflow`/`admin`) por valores seguros no `.env`

---

## 👨‍💻 Autor

Desenvolvido por **Jabes Christian**
