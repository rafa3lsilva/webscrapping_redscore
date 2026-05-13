import re
import os

def gerar_html():
    md_path = "relatorio_xg_ligas.md"
    if not os.path.exists(md_path):
        print(f"Erro: {md_path} não encontrado.")
        return

    with open(md_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Parse do Markdown
    # Ex: | Liga | Possui xG? | % Odds Open | % Odds Close | URL |
    rows = re.findall(r'\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|', content)
    data = []
    for r in rows:
        liga = r[0].strip()
        if liga == "Liga" or liga.startswith("---"): continue
        
        status = "SIM" if "SIM" in r[1] else "NÃO"
        perc_open = r[2].strip()
        perc_close = r[3].strip()
        url_match = re.search(r'\[Link\]\(([^)]+)\)', r[4])
        url = url_match.group(1) if url_match else "#"
        
        data.append({
            "liga": liga,
            "status": status,
            "perc_open": perc_open,
            "perc_close": perc_close,
            "url": url
        })

    # Template HTML com CSS moderno e JS para filtro
    html_template = f"""
<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Dados RedScore</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg: #0b0f19;
            --card-bg: #161b2a;
            --text: #f1f5f9;
            --text-dim: #94a3b8;
            --primary: #38bdf8;
            --success: #22c55e;
            --error: #ef4444;
            --border: #2d3748;
        }}
        
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        
        body {{
            font-family: 'Inter', sans-serif;
            background-color: var(--bg);
            color: var(--text);
            padding: 2rem;
            line-height: 1.5;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        header {{
            margin-bottom: 2rem;
            text-align: center;
        }}
        
        h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            background: linear-gradient(90deg, #38bdf8, #818cf8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        
        .stats-summary {{
            display: flex;
            gap: 1rem;
            justify-content: center;
            margin-bottom: 2rem;
        }}
        
        .stat-card {{
            background: var(--card-bg);
            padding: 1rem 2rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            text-align: center;
            min-width: 150px;
        }}
        
        .stat-value {{ font-size: 1.5rem; font-weight: 700; color: var(--primary); }}
        .stat-label {{ font-size: 0.875rem; color: var(--text-dim); }}

        .controls {{
            display: flex;
            gap: 1rem;
            margin-bottom: 2rem;
            flex-wrap: wrap;
            background: var(--card-bg);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid var(--border);
        }}
        
        input[type="text"] {{
            flex: 1;
            min-width: 300px;
            padding: 0.75rem 1rem;
            border-radius: 8px;
            border: 1px solid var(--border);
            background: #0f172a;
            color: var(--text);
            font-size: 1rem;
            outline: none;
            transition: border-color 0.2s;
        }}
        
        input[type="text"]:focus {{ border-color: var(--primary); }}
        
        .filter-buttons {{
            display: flex;
            gap: 0.5rem;
        }}
        
        button {{
            padding: 0.75rem 1.25rem;
            border-radius: 8px;
            border: 1px solid var(--border);
            background: #0f172a;
            color: var(--text);
            cursor: pointer;
            font-weight: 600;
            transition: all 0.2s;
        }}
        
        button.active {{
            background: var(--primary);
            color: #000;
            border-color: var(--primary);
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--card-bg);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid var(--border);
        }}
        
        th, td {{
            padding: 1.25rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        th {{
            background: rgba(255,255,255,0.03);
            color: var(--text-dim);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }}
        
        tr:last-child td {{ border-bottom: none; }}
        
        tr:hover {{ background: rgba(255,255,255,0.02); }}
        
        .badge {{
            padding: 0.35rem 0.85rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
        }}
        
        .badge-sim {{ background: rgba(34, 197, 94, 0.15); color: var(--success); }}
        .badge-nao {{ background: rgba(239, 68, 68, 0.15); color: var(--error); }}
        
        .perc-bar-bg {{
            width: 80px;
            height: 6px;
            background: #0f172a;
            border-radius: 10px;
            display: inline-block;
            vertical-align: middle;
            margin-right: 8px;
            overflow: hidden;
        }}
        
        .perc-bar-fill {{
            height: 100%;
            background: var(--primary);
            border-radius: 10px;
        }}

        a {{ color: var(--primary); text-decoration: none; font-weight: 500; font-size: 0.9rem; }}
        a:hover {{ text-decoration: underline; }}

        @media (max-width: 800px) {{
            .controls {{ flex-direction: column; }}
            input[type="text"] {{ min-width: 100%; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>RedScore Data Coverage</h1>
            <p style="color: var(--text-dim)">Monitoramento de cobertura de xG e Odds (Abertura/Fechamento)</p>
        </header>

        <div class="stats-summary">
            <div class="stat-card">
                <div class="stat-value" id="total-count">0</div>
                <div class="stat-label">Total de Ligas</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="sim-count" style="color: var(--success)">0</div>
                <div class="stat-label">Ligas com xG</div>
            </div>
        </div>

        <div class="controls">
            <input type="text" id="search" placeholder="Filtrar por nome da liga..." onkeyup="renderTable()">
            <div class="filter-buttons">
                <button class="active" onclick="setFilter('ALL', this)">Todas</button>
                <button onclick="setFilter('SIM', this)">Com xG</button>
                <button onclick="setFilter('NÃO', this)">Sem xG</button>
            </div>
        </div>

        <table id="ligaTable">
            <thead>
                <tr>
                    <th>Liga</th>
                    <th style="text-align: center">Possui xG?</th>
                    <th style="text-align: center">% Odds Open</th>
                    <th style="text-align: center">% Odds Close</th>
                    <th style="text-align: right">Links</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </div>

    <script>
        const data = {data};
        let currentFilter = 'ALL';

        function renderTable() {{
            const tbody = document.querySelector('#ligaTable tbody');
            const searchText = document.getElementById('search').value.toLowerCase();
            
            tbody.innerHTML = '';
            
            data.forEach(item => {{
                const matchesSearch = item.liga.toLowerCase().includes(searchText);
                const matchesFilter = currentFilter === 'ALL' || item.status === currentFilter;

                if (matchesSearch && matchesFilter) {{
                    const tr = document.createElement('tr');
                    const percOpen = parseInt(item.perc_open);
                    const percClose = parseInt(item.perc_close);
                    
                    tr.innerHTML = `
                        <td style="font-weight: 600">${{item.liga}}</td>
                        <td style="text-align: center">
                            <span class="badge ${{item.status === 'SIM' ? 'badge-sim' : 'badge-nao'}}">${{item.status}}</span>
                        </td>
                        <td style="text-align: center">
                            <div class="perc-bar-bg"><div class="perc-bar-fill" style="width: ${{percOpen}}%"></div></div>
                            <span style="font-size: 0.85rem">${{item.perc_open}}</span>
                        </td>
                        <td style="text-align: center">
                            <div class="perc-bar-bg"><div class="perc-bar-fill" style="width: ${{percClose}}%"></div></div>
                            <span style="font-size: 0.85rem">${{item.perc_close}}</span>
                        </td>
                        <td style="text-align: right">
                            <a href="${{item.url}}" target="_blank">Acessar RedScore →</a>
                        </td>
                    `;
                    tbody.appendChild(tr);
                }}
            }});

            document.getElementById('total-count').innerText = data.length;
            document.getElementById('sim-count').innerText = data.filter(i => i.status === 'SIM').length;
        }}

        function setFilter(filter, btn) {{
            currentFilter = filter;
            document.querySelectorAll('.filter-buttons button').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            renderTable();
        }}

        renderTable();
    </script>
</body>
</html>
    """

    with open("dashboard_xg.html", "w", encoding="utf-8") as f:
        f.write(html_template)
    
    print("Dashboard gerado com sucesso: dashboard_xg.html")

if __name__ == "__main__":
    gerar_html()
