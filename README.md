# RedScore Web Scrapper

Este projeto automatiza a coleta de dados de futebol do site RedScore, processa as informações e as sincroniza com o banco de dados local (SQLite), nuvem (Supabase) e repositório GitHub.

## 🚀 Funcionalidades

- **Coleta de Agenda**: Identifica os jogos do dia seguinte para as ligas configuradas.
- **Extração de Histórico**: Coleta estatísticas detalhadas (chutes, ataques, escanteios, odds) dos confrontos passados das equipes.
- **Processamento Híbrido**: Utiliza `requests` com cookies do Selenium para maior performance na Fase 2.
- **Persistência Robusta**: Banco de dados SQLite local com deduplicação e limpeza automática (VACUUM).
- **Sincronização**: Migração automática para Supabase e versionamento de dados no GitHub.
- **Monitoramento**: Logs estruturados com rotação e notificações desktop (Linux).

## 🛠️ Instalação

1. Clone o repositório.
2. Crie um ambiente virtual:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Baixe o `geckodriver` (Firefox) e coloque na raiz do projeto ou garanta que esteja no PATH.

## ⚙️ Configuração

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
REDSCORE_USER="seu-email@exemplo.com"
REDSCORE_PASS="sua-senha"

# Supabase
SUPABASE_URL="https://sua-url.supabase.co"
SUPABASE_KEY="sua-chave-anon-ou-secret"
```

As ligas monitoradas podem ser configuradas em `ligas_config.py`.

## 📂 Estrutura do Projeto

- `coletor.py`: Script principal que gerencia o fluxo de coleta.
- `data.py`: Módulo de raspagem e processamento de dados.
- `login_redscore.py`: Automação de login via Selenium.
- `migrar_banco.py`: Sincronização de dados com o Supabase.
- `executar_scraping.sh`: Script de automação total para agendamento (cron).
- `dados.db`: Banco de dados SQLite (ignorado pelo Git).
- `jogos_do_dia/`: Histórico diário de agendas em CSV.

## 🧪 Testes

Execute os testes unitários com:
```bash
pytest
```

## 📝 Auditoria

O sistema gera arquivos na pasta `auditoria/` e logs em `coletor.log` para monitorar falhas de extração ou inconsistências nos dados do site.
