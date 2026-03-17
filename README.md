# AWS Data Engineering - GitOps & CI/CD

Este repositório atua como um repositório de testes para a infraestrutura de dados na AWS. Ele utiliza os princípios de GitOps para gerenciar deploys contínuos via GitHub Actions, utilizando a AWS CLI nativa e scripts de automação.

## 🏗️ Decisões Arquiteturais e Design Patterns

1. **Separação de Preocupações (Clean Architecture):** Código produtivo (Lambdas, Glue Scripts) e metadados de infraestrutura (Memória, Timeout, Regras de EventBridge) coexistem lado a lado, mas são aplicados na AWS em etapas distintas do pipeline para garantir atomicidade.
2. **FinOps & Otimização de Esteira:** Os gatilhos do GitHub Actions (`paths`) são isolados. Uma alteração em um script do Glue jamais acionará o pipeline de deploys de Lambdas, economizando minutos de *runner*.
3. **Segurança (SecOps):** Nenhuma credencial de longo prazo (IAM Access Keys) é armazenada no GitHub. A autenticação utiliza **OIDC (OpenID Connect)**. Segredos de Lambdas exportados são mascarados automaticamente.
4. **Resiliência (Drift Detection):** Para respeitar a cultura de operações manuais no S3, os artefatos do Glue operam em modo *Append-Only* (sem deleção forçada). No entanto, o pipeline executa uma auditoria de *Drift Detection* (desvio de estado) para alertar sobre arquivos zumbis no S3.

---

## 📂 Estrutura do Repositório

O repositório está organizado por domínios de serviço da AWS. Modificar arquivos dentro de um diretório específico acionará apenas o workflow correspondente.

```text
.
├── .github/workflows/       # Pipelines de CI/CD (Actions)
├── admin_tools/             # Scripts utilitários para exportar o estado atual da AWS
├── eventbridge/             # Regras (Rules) e Cronogramas (Schedules)
├── glue/
│   ├── jobs/                # Metadados em JSON (Configurações, DPUs, Workers)
│   ├── libs/                # Dependências (.whl, .zip, .jar)
│   └── scripts/             # Código fonte dos jobs PySpark/Python
├── lambda/                  # Funções serverless (Código + config.json de hardware)
└── stepfunctions/           # Máquinas de estado (ASL em JSON)

```

---

## 🚀 Pipelines de CI/CD (Workflows)

A esteira está configurada para operar majoritariamente no padrão **Update-Only** e **Upsert** dependendo da resiliência da API da AWS. Todos os *payloads* JSON são higienizados dinamicamente via `jq` em tempo de voo para remover valores nulos (`null`) antes da injeção na AWS CLI.

### 1. AWS Lambda (`cd-lambda.yml`)

* **Gatilho:** Modificações em `lambda/**`.
* **Comportamento:** O pipeline identifica exatamente qual função foi alterada via `git diff`. Ele empacota e atualiza o código (`.zip`) e, em seguida, atualiza a configuração de hardware/ambiente lendo o arquivo `config.json`.
* **Segurança:** A atualização de infraestrutura ignora propositalmente injeções de Variáveis de Ambiente para proteger segredos de produção já alocados na AWS.

### 2. AWS Glue (`cd-glue-*.yml`)

Dividido em três fluxos independentes:

* **Jobs:** Atualiza a infraestrutura de execução (Workers, Retries, Execution Class) e aplica Governança (Tags) em chamadas separadas.
* **Scripts & Libs:** Utiliza o `aws s3 sync`. Executa um `dryrun` primeiro para relatar arquivos órfãos (Drift Detection) e, em seguida, sincroniza o código real para o bucket de artefatos sem deletar o histórico na nuvem.

### 3. AWS Step Functions (`cd-stepfunctions.yml`)

* **Gatilho:** Modificações em `stepfunctions/**`.
* **Comportamento:** Processa os JSONs e atualiza o *Definition* (código ASL), o *Role* e as configurações de *Logging/Tracing*. Usa padrão de falha caso a SFN não exista (evitando a criação de recursos não rastreados).

### 4. Amazon EventBridge (`cd-eventbridge.yml`)

* **Gatilho:** Modificações em `eventbridge/**`.
* **Comportamento:** Atualiza Regras (e seus respectivos Targets) e Cronogramas do EventBridge Scheduler.

---

## 🛠️ Ferramentas de Administração (`admin_tools/`)

Para manter o repositório sincronizado com alterações feitas emergencialmente pelo Console da AWS, utilize os scripts na pasta `admin_tools/`. Eles farão a engenharia reversa da infraestrutura atual para JSON.

**Pré-requisitos locais:**

* Python 3.9+
* Boto3 (`pip install boto3`)
* AWS CLI configurada (`aws configure`)

**Uso:**

1. Navegue até a raiz do repositório.
2. Execute o script desejado:
```bash
python admin_tools/lambda_import.py
python admin_tools/stepfunctions_import.py
python admin_tools/eventbridge_import.py
python admin_tools/glue_export.py

```


3. O script irá baixar os códigos, formatar os JSONs, aplicar heurísticas de segurança (mascarar senhas) e remover localmente pastas/arquivos que já foram deletados na AWS (`Purge`). Revise as mudanças com `git diff` antes de commitar.

---

## 🔐 Configuração do GitHub Actions (Setup)

Para este pipeline funcionar, as seguintes configurações devem estar estabelecidas no repositório:

1. **GitHub Secrets:**
* `AWS_ROLE_ARN`: O ARN da Role do IAM que o GitHub Actions assumirá via OIDC.


2. **Permissões Mínimas (IAM Role):**
* A Role precisa ter uma relação de confiança (Trust Relationship) exclusiva com este repositório.
* As políticas anexadas devem seguir o *Least Privilege*, liberando apenas `s3:PutObject`, `lambda:UpdateFunctionCode`, `glue:UpdateJob`, etc., limitados aos recursos (ARNs) específicos deste projeto.



```