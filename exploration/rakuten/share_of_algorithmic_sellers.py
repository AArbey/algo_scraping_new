import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Charger les données
INPUT_FILE = "/home/scraping/algo_scraping/exploration/rakuten/NEWdata_with_variables_fast_plus5.csv"
OUTPUT_DIR = "/home/scraping/algo_scraping/exploration/rakuten/figures_final"
df = pd.read_csv(INPUT_FILE, parse_dates=['Timestamp', 'Day'])
df = df.dropna(subset=['Price', 'Timestamp', 'Seller', 'Model'])
df['algo_suspect'] = df['algo_suspect'].astype(bool)

# Grouper par (Timestamp, Model) = état de la concurrence à un instant donné
grouped = df.groupby(['Timestamp', 'Model'])

results = []
for (ts, model), group in grouped:
    sellers = group['Seller'].nunique()
    algo_sellers = group[group['algo_suspect']]['Seller'].nunique()
    results.append({'Timestamp': ts, 'Model': model,
                    'n_sellers': sellers,
                    'n_algo': algo_sellers})

summary = pd.DataFrame(results)

# Regrouper le nombre de vendeurs algo
def bin_algo_sellers(n):
    if n == 0:
        return '0 algorithmic sellers'
    elif n == 1:
        return '1 algorithmic seller'
    elif n == 2:
        return '2 algorithmic sellers'
    elif n == 3:
        return '3 algorithmic sellers'
    else:
        return '>3 algorithmic sellers'

summary['algo_seller_bin'] = summary['n_algo'].apply(bin_algo_sellers)

# Filtrer pour les cas entre 1 et 10 vendeurs (comme dans le papier)
summary = summary[summary['n_sellers'].between(1, 20)]
summary = summary[summary['n_sellers'] != 17]


# Part de chaque catégorie d’algo_seller par n_sellers
share_table = summary.groupby(['n_sellers', 'algo_seller_bin']).size().reset_index(name='count')
total_per_n_sellers = summary.groupby('n_sellers').size().reset_index(name='total')

share_table = share_table.merge(total_per_n_sellers, on='n_sellers')
share_table['share'] = share_table['count'] / share_table['total']

# Ordre des catégories
algo_order = ['0 algorithmic sellers', '1 algorithmic seller', '2 algorithmic sellers',
              '3 algorithmic sellers', '>3 algorithmic sellers']
share_table['algo_seller_bin'] = pd.Categorical(share_table['algo_seller_bin'],
                                                categories=algo_order,
                                                ordered=True)
share_table = share_table.sort_values(['n_sellers', 'algo_seller_bin'])

print(summary['n_sellers'].value_counts().sort_index())

# Pivot pour plot en barres empilées
pivot = share_table.pivot(index='n_sellers', columns='algo_seller_bin', values='share').fillna(0)

colors = ['#537cea', '#ff6bd4', 'green', 'gold', 'purple']
pivot.plot(kind='bar', stacked=True, color=colors, figsize=(10, 6))

plt.title("Part des vendeurs algorithmique selon le nombre de vendeurs")
plt.xlabel("Nombre de vendeurs")
plt.ylabel("Part des observations")
plt.legend(title="Nombre de vendeurs algorithmique", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.grid(True, axis='y')
save_path = os.path.join(OUTPUT_DIR, "share_algo_sellers_by_competition_fixed.png")
plt.savefig(save_path, dpi=300)
plt.close()
