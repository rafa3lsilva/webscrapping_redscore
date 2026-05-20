# 📊 Relatório de Auditoria e Diagnóstico de Qualidade das Ligas

Este relatório apresenta o diagnóstico de qualidade dos dados coletados por amostragem para novas ligas candidatas a inclusão no Redscore.
* **Data da Execução:** 20/05/2026 19:50:26
* **Tamanho da Amostra por Liga:** 35 jogos (distribuídos nas últimas 3 temporadas)

---

## 📋 Tabela Comparativa de Cobertura e Tiering

| Liga | Cobertura Odds (%) | Cobertura xG (%) | Cobertura Scouts (%) | Cobertura xGOT/Box (%) | Jogos Amostrados | Status | Tier Sugerido |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| Albânia - Superliga | 91.4% | 0.0% | 42.9% | 0.0% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Argentina - Primera B Nacional | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Argélia - Ligue 1 | 97.1% | 0.0% | 42.9% | 0.0% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Armênia - Premier League | 91.4% | 0.0% | 62.9% | 0.0% | 35 | Sucesso | **Tier 3 (Exótica)** |
| Bolívia - Liga De Futbol Prof | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Coréia do Sul - K League 2 | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| EUA - USL Championship | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Egito - Premier League | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Escócia - Championship | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Escócia - League One | 100.0% | 0.0% | 0.0% | 0.0% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Escócia - League Two | 91.4% | 0.0% | 0.0% | 0.0% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Eslováquia - Fortuna Liga | 100.0% | 100.0% | 100.0% | 71.4% | 35 | Sucesso | **Tier 1 (Elite)** |
| Eslovênia - 1. SNL | 97.1% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Estônia - Meistriliiga | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Finlândia - Veikkausliiga | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Hungria - OTP Bank Liga | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Irlanda - Premier Division | 82.9% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Irlanda do Norte - Premiership | 93.3% | 0.0% | 100.0% | 0.0% | 15 | Sucesso | **Tier 3 (Exótica)** |
| Islândia - Pepsideild | 88.6% | 71.4% | 97.1% | 71.4% | 35 | Sucesso | **Descartar (Baixa Qualidade)** |
| Israel - Ligat ha'Al | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Lituânia - A Lyga | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Malásia - Super League | 100.0% | 0.0% | 100.0% | 0.0% | 15 | Sucesso | **Tier 2 (Acesso)** |
| Paraguai - Division 1 | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| País de Gales - Premier League | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Peru - Primera Division | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Sérvia - Super Liga | 91.4% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 3 (Exótica)** |
| Uruguai - Primera Division | 100.0% | 0.0% | 100.0% | 0.0% | 35 | Sucesso | **Tier 2 (Acesso)** |
| Venezuela - Primera Division | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |
| África do Sul - Premier League | 100.0% | 71.4% | 100.0% | 71.4% | 35 | Sucesso | **Tier 2 (Acesso)** |

---

## 🧠 Diretrizes para Decisão de Inclusão

1. **Tier 1 (Elite):**
   * *Critério:* Odds ≥ 95% e xG ≥ 90%.
   * *Ação:* Inclusão recomendada imediatamente com suporte a estatísticas avançadas e xG de alta fidelidade no treinamento de Machine Learning.
   
2. **Tier 2 (Acesso):**
   * *Critério:* Odds ≥ 95%, xG < 90% e Scouts Clássicos ≥ 80%.
   * *Ação:* Inclusão recomendada usando modelagem híbrida (apenas scouts tradicionais + movimentação de odds, sem pesos de xG ou descartando xG nulos no Pandas).
   
3. **Tier 3 (Exótica):**
   * *Critério:* Odds ≥ 90% e Scouts Clássicos ≥ 50%.
   * *Ação:* Excelente para caçar ineficiências em casas de apostas menores. Evitar xG e focar em cantos, gols HT/FT e ELO de força.

4. **Descartar:**
   * *Critério:* Odds < 80% ou Scouts Clássicos < 30%.
   * *Ação:* Não incluir no banco de dados. Os dados são esparsos ou incompletos, o que polui os pipelines de modelagem.
