import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from pyathena import connect
from app.config import AWS_REGION, BUCKET_NAME


# Configure connection to Athena
staging_dir = f's3://{BUCKET_NAME}/query-results/'
conn = connect(s3_staging_dir=staging_dir, region_name=AWS_REGION)

# Query to get stocks with highest average participation
query = """
SELECT 
  stock_code, 
  stock_name,
  AVG(part_numeric) as avg_part,
  AVG(record_density) as avg_density,
  SUM(record_count) as total_records
FROM 
  bovespa_transformed 
GROUP BY 
  stock_code, stock_name
ORDER BY 
  avg_part DESC
LIMIT 15
"""

# Execute query and load results
df = pd.read_sql(query, conn)

# Create a larger figure to accommodate the chart and annotations
plt.figure(figsize=(14, 10))

# Create bar chart - using smaller width to leave space for interpretation box
ax = sns.barplot(x='stock_code', y='avg_part', data=df, palette='viridis')

# Add titles and labels
plt.title('Average Stock Participation in B3', fontsize=16, fontweight='bold')
plt.xlabel('Stock Code', fontsize=12)
plt.ylabel('Average Participation (%)', fontsize=12)
plt.xticks(rotation=45, ha='right', fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Add values on top of each bar
for i, v in enumerate(df['avg_part']):
    ax.text(i, v + 0.05, f"{v:.2f}%", ha='center', fontsize=9)

# Add company name below each stock code
for i, (code, name) in enumerate(zip(df['stock_code'], df['stock_name'])):
    ax.annotate(f"{name}", xy=(i, 0), xytext=(0, -20), 
                textcoords="offset points", ha='center', va='center',
                fontsize=8, rotation=45)

# Add explanation box in the upper right corner
explanation_text = """
Interpretation:
- Stocks with higher participation percentage have 
  greater weight in the index composition and 
  typically higher market liquidity.
- Higher participation usually indicates companies 
  with larger market capitalization and economic 
  relevance.
- The distribution of participation shows market 
  concentration in certain sectors and companies.
"""

# Place the explanation box in the upper right corner
plt.figtext(0.70, 0.80, explanation_text, wrap=True, fontsize=10, 
           bbox=dict(facecolor='lightyellow', alpha=0.7, boxstyle='round,pad=0.5', 
                    edgecolor='gray', linewidth=1))

# Add information about top 3 stocks
top_stocks = df.head(3)
for i, (code, part) in enumerate(zip(top_stocks['stock_code'], top_stocks['avg_part'])):
    plt.annotate(
        f"{code}: {part:.2f}%\nHighest market relevance",
        xy=(i, part), xytext=(i-0.5, part + 1),
        arrowprops=dict(arrowstyle='->', lw=1.5, color='red'),
        fontsize=9, color='darkred', fontweight='bold'
    )

# Adjust layout 
plt.tight_layout(rect=[0, 0.05, 0.95, 0.95])
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

# Add analysis date
from datetime import datetime
today = datetime.now().strftime('%Y-%m-%d')
plt.figtext(0.98, 0.02, f"Generated on: {today}", ha='right', fontsize=8, fontstyle='italic')

# Add upper title with context
plt.suptitle('B3 Market Composition Analysis', fontsize=18, y=0.98)

# Save the chart
output_path = f"{BUCKET_NAME}_stock_participation.png"
plt.savefig(output_path, dpi=300, bbox_inches='tight')

# Show the chart
plt.show()
