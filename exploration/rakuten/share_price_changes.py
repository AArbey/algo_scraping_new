import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
from matplotlib.lines import Line2D

INPUT_FILE = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_plus5.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/rakuten/figures_final"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Chargement des données
df = pd.read_csv(INPUT_FILE, parse_dates=['Timestamp', 'Day'])
df = df.dropna(subset=['Price', 'Timestamp'])

df['algo_suspect'] = df['algo_suspect'].astype(bool)
df['Price_change'] = df.groupby('CodeID')['Price'].diff().abs()

# On garde uniquement les vraies modifications de prix (≠ 0 ou NaN)
df = df[df['Price_change'] > 0]

# Extraire jour et heure
df['Day_of_week'] = df['Timestamp'].dt.day_name()
df['Hour'] = df['Timestamp'].dt.hour

# Total de changements de prix par groupe algo / non-algo (pour normaliser)
total_changes = df.groupby('algo_suspect')['Price_change'].count().to_dict()

# Part par jour
daily_counts = df.groupby(['Day_of_week', 'algo_suspect']).agg(
    Count=('Price_change', 'count')
).reset_index()

daily_counts['Share_of_changes'] = daily_counts.apply(
    lambda row: row['Count'] / total_changes[row['algo_suspect']], axis=1
)

# Part par heure
hourly_counts = df.groupby(['Hour', 'algo_suspect']).agg(
    Count=('Price_change', 'count')
).reset_index()

hourly_counts['Share_of_changes'] = hourly_counts.apply(
    lambda row: row['Count'] / total_changes[row['algo_suspect']], axis=1
)

# Traduction et tri des jours de la semaine
day_translation = {
    'Monday': 'Lundi',
    'Tuesday': 'Mardi',
    'Wednesday': 'Mercredi',
    'Thursday': 'Jeudi',
    'Friday': 'Vendredi',
    'Saturday': 'Samedi',
    'Sunday': 'Dimanche'
}
weekday_order = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']

daily_counts['Day_of_week'] = daily_counts['Day_of_week'].map(day_translation)
daily_counts['Day_of_week'] = pd.Categorical(daily_counts['Day_of_week'], categories=weekday_order, ordered=True)
daily_counts = daily_counts.sort_values('Day_of_week')

plt.style.use('seaborn-v0_8')

def save_plot(fig, filename):
    fig.savefig(os.path.join(OUTPUT_DIR, filename), 
                dpi=300, 
                bbox_inches='tight',
                format='png')
    plt.close(fig)

# Plot par jour de la semaine
fig1, ax1 = plt.subplots(figsize=(10, 5))
sns.lineplot(
    data=daily_counts,
    x='Day_of_week',
    y='Share_of_changes',
    hue='algo_suspect',
    palette={False: 'blue', True: 'red'},
    style='algo_suspect', 
    markers=True,
    dashes=[(1, 0), (2, 2)], 
    legend=False,  
    ax=ax1
)

legend_elements = [
    Line2D([0], [0], color='blue', lw=2, label='Non-algorithmique'),
    Line2D([0], [0], color='red', lw=2, linestyle='--',label='Algorithmique')
]
ax1.legend(handles=legend_elements, title='Type de changement')

ax1.set_title('Part des changements de prix par jour')
ax1.set_xlabel('Jour de la semaine')
ax1.set_ylabel('Part des changements')
ax1.grid(True)
save_plot(fig1, 'share_par_jour.png')

# Plot par heure de la journée
fig2, ax2 = plt.subplots(figsize=(12, 5))
sns.lineplot(
    data=hourly_counts,
    x='Hour',
    y='Share_of_changes',
    hue='algo_suspect',
    palette={False: 'blue', True: 'red'},
    style='algo_suspect', 
    markers=True,
    dashes=[(1, 0), (2, 2)], 
    legend=False,  
    ax=ax2
)

legend_elements = [
    Line2D([0], [0], color='blue', lw=2, label='Non-algorithmique'),
    Line2D([0], [0], color='red', lw=2, linestyle='--', label='Algorithmique')
]
ax2.legend(handles=legend_elements, title='Type de changement')

ax2.set_title('Part des changements de prix par heure')
ax2.set_xlabel('Heure de la journée')
ax2.set_ylabel('Part des changements')
ax2.set_xticks(range(0, 24))
ax2.grid(True)
save_plot(fig2, 'share_par_heure.png')
