"""
HTML Report Generator for Intervals Generator.
Creates visual summary of merged training data.
"""

from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

from .logging_config import get_logger


# HTML Template
REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="pl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Intervals Generator - Raport {date}</title>
    <style>
        :root {{
            --bg-dark: #1a1a2e;
            --bg-card: #16213e;
            --accent: #e94560;
            --accent-light: #ff6b6b;
            --text: #eaeaea;
            --text-muted: #a0a0a0;
            --success: #4ade80;
            --warning: #fbbf24;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: var(--bg-dark);
            color: var(--text);
            padding: 2rem;
            line-height: 1.6;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        header {{
            text-align: center;
            margin-bottom: 2rem;
            padding: 2rem;
            background: linear-gradient(135deg, var(--bg-card), #0f3460);
            border-radius: 16px;
            border: 1px solid rgba(233, 69, 96, 0.3);
        }}
        h1 {{
            font-size: 2.5rem;
            background: linear-gradient(90deg, var(--accent), var(--accent-light));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }}
        .subtitle {{ color: var(--text-muted); }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }}
        .card {{
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid rgba(255,255,255,0.1);
        }}
        .card h3 {{
            color: var(--accent);
            margin-bottom: 1rem;
            font-size: 1.1rem;
        }}
        .stat {{ font-size: 2.5rem; font-weight: 700; color: var(--accent-light); }}
        .stat-label {{ color: var(--text-muted); font-size: 0.9rem; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }}
        th, td {{
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        th {{ color: var(--accent); font-weight: 600; }}
        tr:hover {{ background: rgba(233, 69, 96, 0.1); }}
        .warning {{ color: var(--warning); }}
        .success {{ color: var(--success); }}
        .footer {{
            text-align: center;
            margin-top: 2rem;
            padding: 1rem;
            color: var(--text-muted);
            font-size: 0.85rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🏋️ Intervals Generator</h1>
            <p class="subtitle">Raport z przetwarzania danych treningowych</p>
            <p class="subtitle">{date}</p>
        </header>
        
        <div class="grid">
            <div class="card">
                <h3>📊 Podsumowanie</h3>
                <div class="stat">{rows}</div>
                <div class="stat-label">wierszy danych</div>
            </div>
            <div class="card">
                <h3>📈 Kolumny</h3>
                <div class="stat">{cols}</div>
                <div class="stat-label">zmiennych</div>
            </div>
            <div class="card">
                <h3>⏱️ Czas trwania</h3>
                <div class="stat">{duration}</div>
                <div class="stat-label">minut treningu</div>
            </div>
            <div class="card">
                <h3>📁 Plik wynikowy</h3>
                <div style="font-size: 1rem; word-break: break-all;">{filename}</div>
            </div>
        </div>
        
        <div class="card">
            <h3>📋 Szczegóły kolumn</h3>
            <table>
                <thead>
                    <tr>
                        <th>Kolumna</th>
                        <th>Typ</th>
                        <th>Wartości</th>
                        <th>Braki (%)</th>
                    </tr>
                </thead>
                <tbody>
                    {columns_table}
                </tbody>
            </table>
        </div>
        
        {alerts_section}
        
        <footer class="footer">
            Wygenerowano przez Intervals Generator | {timestamp}
        </footer>
    </div>
</body>
</html>
"""


class ReportGenerator:
    """Generates HTML reports for merged training data."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory for report files. Defaults to base_dir/reports
        """
        self.output_dir = output_dir
        self.logger = get_logger()
    
    def generate_html_report(
        self,
        df: pd.DataFrame,
        output_path: Path,
        filename: str = "training_data.csv"
    ) -> Path:
        """
        Generate an HTML report for the merged DataFrame.
        
        Args:
            df: Merged training DataFrame
            output_path: Where to save the report
            filename: Name of the source CSV file
            
        Returns:
            Path to generated report
        """
        self.logger.info("📝 Generowanie raportu HTML...")
        
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate stats
        rows = len(df)
        cols = len(df.columns)
        duration = rows // 60 if rows > 0 else 0  # Assuming 1 row = 1 second
        
        # Build columns table
        columns_table = []
        alerts = []
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            missing_pct = (1 - non_null / rows) * 100 if rows > 0 else 0
            
            # Value range
            if pd.api.types.is_numeric_dtype(df[col]):
                val_range = f"{df[col].min():.1f} - {df[col].max():.1f}"
            else:
                val_range = f"{non_null} wartości"
            
            # Styling for missing data
            missing_class = "warning" if missing_pct > 5 else "success"
            
            columns_table.append(f"""
                <tr>
                    <td>{col}</td>
                    <td>{dtype}</td>
                    <td>{val_range}</td>
                    <td class="{missing_class}">{missing_pct:.1f}%</td>
                </tr>
            """)
            
            # Alert for high missing percentage
            if missing_pct > 10:
                alerts.append(f"⚠️ Kolumna '{col}' ma {missing_pct:.1f}% brakujących wartości")
        
        # Build alerts section
        alerts_section = ""
        if alerts:
            alerts_html = "".join(f"<li>{a}</li>" for a in alerts)
            alerts_section = f"""
                <div class="card">
                    <h3>⚠️ Alerty</h3>
                    <ul style="padding-left: 1.5rem; color: var(--warning);">
                        {alerts_html}
                    </ul>
                </div>
            """
        
        # Generate HTML
        html = REPORT_TEMPLATE.format(
            date=datetime.now().strftime("%d.%m.%Y"),
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            rows=rows,
            cols=cols,
            duration=duration,
            filename=filename,
            columns_table="".join(columns_table),
            alerts_section=alerts_section
        )
        
        # Save report
        output_path.write_text(html, encoding='utf-8')
        
        self.logger.info(f"   ✅ Raport zapisany: {output_path}")
        
        return output_path
