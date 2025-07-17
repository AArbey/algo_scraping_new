import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
from matplotlib.lines import Line2D

INPUT_FILE = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_plus5.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/rakuten/figures_final"
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_csv(INPUT_FILE, parse_dates=['Timestamp', 'Day'])
df = df.dropna(subset=['Price', 'Timestamp'])

df['algo_suspect'] = df['algo_suspect'].astype(bool)
df['Price_change'] = df.groupby('CodeID')['Price'].diff().abs() / 100

df['Day_of_week'] = df['Timestamp'].dt.day_name()
df['Hour'] = df['Timestamp'].dt.hour

daily_data = df.groupby(['Day_of_week', 'algo_suspect']).agg(
    Share_of_changes=('Price_change', 'mean')
).reset_index()

hourly_data = df.groupby(['Hour', 'algo_suspect']).agg(
    Share_of_changes=('Price_change', 'mean')
).reset_index()

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

daily_data['Day_of_week'] = daily_data['Day_of_week'].map(day_translation)
daily_data['Day_of_week'] = pd.Categorical(daily_data['Day_of_week'], categories=weekday_order, ordered=True)
daily_data = daily_data.sort_values('Day_of_week')

plt.style.use('seaborn-v0_8')

def save_plot(fig, filename):
    fig.savefig(os.path.join(OUTPUT_DIR, filename), 
                dpi=300, 
                bbox_inches='tight',
                format='png')
    plt.close(fig)

# Graph j
fig1, ax1 = plt.subplots(figsize=(10, 5))
sns.lineplot(
    data=daily_data,
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

ax1.set_title('Moyenne des variations absolues de prix par jour')
ax1.set_xlabel('Jour de la semaine')
ax1.set_ylabel('Variation moyenne')
ax1.grid(True)
save_plot(fig1, 'variations_par_jour.png')

# Graph h
fig2, ax2 = plt.subplots(figsize=(12, 5))
plot = sns.lineplot(
    data=hourly_data,
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

ax2.set_title('Moyenne des variations absolues de prix par heure')
ax2.set_xlabel('Heure de la journée')
ax2.set_ylabel('Variation moyenne')
ax2.set_xticks(range(0, 24))
ax2.grid(True)
save_plot(fig2, 'variations_par_heure.png')




daily_price = df.groupby(['Day_of_week', 'algo_suspect']).agg(
    Mean_price=('Price', 'mean')
).reset_index()

hourly_price = df.groupby(['Hour', 'algo_suspect']).agg(
    Mean_price=('Price', 'mean')
).reset_index()

fig3, ax3 = plt.subplots(figsize=(10, 5))
sns.lineplot(
    data=daily_price,
    x='Day_of_week',
    y='Mean_price',
    hue='algo_suspect',
    palette={False: 'blue', True: 'red'},
    style='algo_suspect',
    markers=True,
    dashes=[(1, 0), (2, 2)],
    legend=False,
    ax=ax3
)

ax3.legend(handles=legend_elements, title='Type de changement')
ax3.set_title('Prix moyen par jour selon le type de changement')
ax3.set_xlabel('Jour de la semaine')
ax3.set_ylabel('Prix moyen (€)')
ax3.grid(True)
save_plot(fig3, 'prix_moyen_par_jour.png')

fig4, ax4 = plt.subplots(figsize=(12, 5))
sns.lineplot(
    data=hourly_price,
    x='Hour',
    y='Mean_price',
    hue='algo_suspect',
    palette={False: 'blue', True: 'red'},
    style='algo_suspect',
    markers=True,
    dashes=[(1, 0), (2, 2)],
    legend=False,
    ax=ax4
)

ax4.legend(handles=legend_elements, title='Type de changement')
ax4.set_title('Prix moyen par heure selon le type de changement')
ax4.set_xlabel('Heure de la journée')
ax4.set_ylabel('Prix moyen (€)')
ax4.set_xticks(range(0, 24))
ax4.grid(True)
save_plot(fig4, 'prix_moyen_par_heure.png')



modeles_uniques = df['Model'].unique()

MODEL_DIR = os.path.join(OUTPUT_DIR, 'par_modele')
os.makedirs(MODEL_DIR, exist_ok=True)

def save_model_plot(fig, filename):
    fig.savefig(os.path.join(MODEL_DIR, filename), 
                dpi=300, 
                bbox_inches='tight',
                format='png')
    plt.close(fig)

for modele in modeles_uniques:
    df_modele = df[df['Model'] == modele]
    
    daily_price_modele = df_modele.groupby(['Day_of_week', 'algo_suspect']).agg(
        Mean_price=('Price', 'mean')
    ).reset_index()
    
    daily_price_modele['Day_of_week'] = daily_price_modele['Day_of_week'].map(day_translation)
    daily_price_modele['Day_of_week'] = pd.Categorical(daily_price_modele['Day_of_week'], categories=weekday_order, ordered=True)
    daily_price_modele = daily_price_modele.sort_values('Day_of_week')
    
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.lineplot(
        data=daily_price_modele,
        x='Day_of_week',
        y='Mean_price',
        hue='algo_suspect',
        palette={False: 'blue', True: 'red'},
        style='algo_suspect',
        markers=True,
        dashes=[(1, 0), (2, 2)],
        legend=False,
        ax=ax
    )
    ax.legend(handles=legend_elements, title='Type de changement')
    ax.set_title(f'Prix moyen par jour - {modele}')
    ax.set_xlabel('Jour de la semaine')
    ax.set_ylabel('Prix moyen (€)')
    ax.grid(True)
    save_model_plot(fig, f'prix_jour_{modele}.png')
    
    hourly_price_modele = df_modele.groupby(['Hour', 'algo_suspect']).agg(
        Mean_price=('Price', 'mean')
    ).reset_index()
    
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(
        data=hourly_price_modele,
        x='Hour',
        y='Mean_price',
        hue='algo_suspect',
        palette={False: 'blue', True: 'red'},
        style='algo_suspect',
        markers=True,
        dashes=[(1, 0), (2, 2)],
        legend=False,
        ax=ax
    )
    ax.legend(handles=legend_elements, title='Type de changement')
    ax.set_title(f'Prix moyen par heure - {modele}')
    ax.set_xlabel('Heure de la journée')
    ax.set_ylabel('Prix moyen (€)')
    ax.set_xticks(range(0, 24))
    ax.grid(True)
    save_model_plot(fig, f'prix_heure_{modele}.png')