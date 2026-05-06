import sqlite3
import pandas as pd
import logging

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("exportar")

def exportar_para_csv(nome_db="dados_historicos.db", nome_csv="dados_historicos.csv"):
    try:
        log.info(f"Conectando ao banco {nome_db}...")
        conn = sqlite3.connect(nome_db)
        
        # Ler os dados da tabela 'jogos'
        query = "SELECT * FROM jogos ORDER BY Data ASC"
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            log.warning("O banco de dados está vazio. Nenhum arquivo gerado.")
            return

        # Exportar para CSV
        df.to_csv(nome_csv, index=False, encoding="utf-8-sig")
        log.info(f"Sucesso! {len(df)} registros exportados para '{nome_csv}'.")
        
        conn.close()
    except Exception as e:
        log.error(f"Erro ao exportar dados: {e}")

if __name__ == "__main__":
    exportar_para_csv()
