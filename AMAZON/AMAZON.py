"""
Script de scraping pour Amazon
------------------------------

Ce script scrape par itération une liste de pages de produits Amazon.
Pour chaque produit (défini par un ASIN), il récupère les informations sur l'offre principale 
ainsi que les offres supplémentaires via des requêtes AJAX. Les informations extraites sont ensuite 
converties et enregistrées dans un fichier Parquet.
Les requêtes sont effectuées de manière répartie sur un intervalle de 1 heure, puis le script recommence la liste à l'infini.

Détails :
- Le script effectue une requête GET sur un URL Amazon pour récupérer les informations de l'offre principale, 
  et utilise des requêtes AJAX pour récupérer les offres supplémentaires.
- À chaque itération, les nouvelles données sont ajoutées dans un fichier Parquet ('amazon_offers.parquet').
- Les requêtes sont effectuées de manière aléatoire pour éviter le blocage, en utilisant un intervalle défini de temps entre chaque produit.
- Une fois que tous les produits de la liste sont scrappés, le script attend quelques minutes et recommence à l'infini.

Variables :
- EXCEL_FILE : Chemin vers le fichier Excel contenant les ASINs, ids, et noms des produits.
- PARQUET_FILE : Nom du fichier Parquet où les offres seront enregistrées.
- SCRAPE_INTERVAL : Interval entre chaque cycle de scraping

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES
Date : 28-10-2024
Version : 2.0

"""

# Nouvelle version du script Amazon avec Selenium pour VM MiNET avec Xvfb
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime
import os
import time
import re
import math
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pwd
import sys

def drop_privileges():
    if os.getuid() != 0:
        return  # Déjà exécuté en tant qu'utilisateur normal
    
    # Identifiants de l'utilisateur scraping
    user_name = "scraping"
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid
    
    # Définir le groupe
    os.setgid(user_gid)
    # Définir l'utilisateur
    os.setuid(user_uid)
    # Définir le HOME
    os.environ['HOME'] = f'/home/{user_name}'

# Changer d'utilisateur
drop_privileges()

EXCEL_FILE = '/home/scraping/algo_scraping/lien.xlsx'
PARQUET_FILE = "/home/scraping/algo_scraping/AMAZON/amazon_offers.parquet"
SCRAPE_INTERVAL = 1200 # 20min
BASE_URL_TEMPLATE = 'https://www.amazon.fr/dp/{asin}'

# Logger racine

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler console → stdout (pour systemd)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def start_xvfb():
    xvfb_process = subprocess.Popen(
        ["Xvfb", ":98", "-screen", "0", "1920x1080x24"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    os.environ["DISPLAY"] = ":98"
    return xvfb_process

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = '/usr/bin/google-chrome'
    #chrome_options.add_argument("--user-data-dir=/tmp/chrome_user_data_vm")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def clean_text(text):
    if text:
        return re.sub(r'\s+', ' ', text.strip())
    return 'N/A'

def scrape_main_offer(driver, asin, idsmartphone, phone_name):
    url = BASE_URL_TEMPLATE.format(asin=asin)
    offers = []
    logging.info(f"Scraping main offer for ASIN {asin}")
    try:
        
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 🟡 Extraction robuste du prix
        price_value = pd.NA
        found_price = soup.find('span', class_='a-offscreen')
        if found_price:
            price_text = found_price.get_text(strip=True)
            price_text = price_text.replace("€", "").replace(",", ".").strip()
            try:
                price_value = float(re.sub(r"[^\d.]", "", price_text))
            except ValueError:
                logging.warning(f"[{asin}] Impossible de convertir le prix '{price_text}' en float.")
                price_value = pd.NA
        else:
            logging.warning(f"[{asin}] Aucun élément a-offscreen trouvé pour le prix.")


        # Frais de livraison

        # Code html correspondant :

        # <span data-csa-c-type="element" data-csa-c-content-id="DEXUnifiedCXPDM" data-csa-c-delivery-price="GRATUITE" 
        # data-csa-c-value-proposition="" data-csa-c-delivery-type="Livraison" data-csa-c-delivery-time="samedi 12 juillet" 
        # data-csa-c-delivery-destination="" data-csa-c-delivery-condition="" data-csa-c-pickup-location="" data-csa-c-distance="" 
        # data-csa-c-delivery-cutoff="Commandez dans les 13 h 14 min" data-csa-c-mir-view="CONSOLIDATED_CX" data-csa-c-mir-type="DELIVERY" 
        # data-csa-c-mir-sub-type="" data-csa-c-mir-variant="DEFAULT" data-csa-c-delivery-benefit-program-id="cfs" data-csa-c-id="4psuiv-h7n0mn-un0iny-xj0pf4"> 
        # <a aria-label="en savoir plus sur Livraison GRATUITE" target="_blank" href="/gp/help/customer/display.html?nodeId=GZXW7X6AKTHNUP6H" 
        # data-a-atomic-interop="" data-a-component="text-link">Livraison GRATUITE</a> <span class="a-text-bold">samedi 12 juillet</span>. 
        # Commandez dans les <span id="ftCountdown" class="ftCountdownClass" style="color: #067D62">13 h 14 min</span>. 
        # <a aria-label="Informations relatives aux frais de livraison et méthodes d’expédition" target="_blank" 
        # href="/gp/help/customer/display.html/?nodeId=201911090" data-a-atomic-interop="" data-a-component="text-link">Détails</a> </span>



        shipcost = pd.NA
        shipcost_element = soup.find('a', href="/gp/help/customer/display.html?nodeId=GZXW7X6AKTHNUP6H")
        logging.debug(f"shipcost_element: {shipcost_element}")
        if shipcost_element:
            shipcost = clean_text(shipcost_element.get_text())
            logging.info(f"[{asin}] Frais de livraison trouvés : {shipcost}")
        else:
            logging.warning(f"[{asin}] Aucun élément href=\"/gp/help/customer/display.html?nodeId=GZXW7X6AKTHNUP6H\" trouvé pour les frais de livraison.")
        
        if shipcost == "Livraison GRATUITE":
            shipcost = 0.0
        else:
            shipcost = pd.NA


        # 🧾 Vendeur
        expediteur_name = 'N/A'
        seller_name = 'N/A'

        expediteur_block = soup.find('div', id='aod-offer-shipsFrom')
        if expediteur_block:
            expediteur_span = expediteur_block.find('span', class_='a-size-small a-color-base')
            expediteur_name = clean_text(expediteur_span.get_text()) if expediteur_span else 'N/A'


        # Première tentative : structure classique
        try:
            seller_block = soup.find('div', id='aod-offer-soldBy')
            if seller_block:
                seller_span = seller_block.find('span', class_='a-size-small a-color-base')
                if seller_span and seller_span.get_text(strip=True):
                    seller_name = clean_text(seller_span.get_text())
                    logging.info(f"Vendeur trouvé (classique) : {seller_name}")
        except Exception as e:
            logging.warning(f"Erreur récupération vendeur (classique) : {e}")

        # Fallback : structure alternative (ex. Amazon, dans la buybox)
        if seller_name == 'N/A':
            try:
                alt_seller_span = soup.find('span', class_='a-size-small offer-display-feature-text-message')
                if alt_seller_span and alt_seller_span.get_text(strip=True):
                    seller_name = clean_text(alt_seller_span.get_text())
                    logging.info(f"Vendeur trouvé (fallback) : {seller_name}")
            except Exception as e:
                logging.warning(f"Erreur fallback vendeur (alt structure) : {e}")

        offer_details = {
                'pfid': "AMAZ",
                'idsmartphone': idsmartphone,
                'url': url,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Price': price_value,
                'shipcost': shipcost,
                'seller': seller_name,
                'rating': pd.NA,
                'ratingnb': pd.NA,
                'offertype': 'Neuf',
                'offerdetails': pd.NA,
                'shipcountry': pd.NA,
                'sellercountry': pd.NA,
                'descriptsmartphone': phone_name
            }
        offers.append(offer_details)
        logging.info("Main offer retrieved")

    except Exception as e:
        logging.error(f"Erreur lors du scraping principal pour ASIN {asin}: {e}")
    return offers

def scrape_additional_offers(driver, asin, idsmartphone, phone_name):
    logging.info(f"Scraping additional offers for ASIN {asin}")
    url = f"https://www.amazon.fr/gp/offer-listing/{asin}"
    offers = []
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        offer_blocks = soup.find_all('div', class_='a-section a-spacing-none a-padding-base aod-information-block aod-clear-float')
        if not offer_blocks:
            logging.warning(f"[{asin}] Aucune offre secondaire trouvée.")

        for offer_block in offer_blocks:
            try:
                # Prix
                price_whole = offer_block.find('span', class_='a-price-whole')
                price_fraction = offer_block.find('span', class_='a-price-fraction')
                price_whole_text = price_whole.get_text(strip=True) if price_whole else '0'
                price_fraction_text = price_fraction.get_text(strip=True) if price_fraction else '00'
                price_str = f"{price_whole_text}.{price_fraction_text}"
                price_str = re.sub(r"[^\d.]", "", price_str)
                price_value = float(price_str)

                # Shipping cost

                shipcost = pd.NA
                shipcost_element = offer_block.find('a', href="/gp/help/customer/display.html?nodeId=GZXW7X6AKTHNUP6H")
                
                if shipcost_element:
                    shipcost = clean_text(shipcost_element.get_text())
                    logging.debug(f"[{asin}] Frais de livraison trouvés : {shipcost}")
                else:
                    logging.debug(f"[{asin}] Aucun élément pour les frais de livraison trouvé dans l'offre secondaire avec la première méthode.")
                    # On cherche un autre élément
                    shipcost_element = offer_block.find('div', id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE")
                    if shipcost_element:
                        shipcost = clean_text(shipcost_element.get_text())
                        # On récupère les 2 premiers mots pour le shipcost
                        shipcost = ' '.join(shipcost.split()[:2])
                        logging.debug(f"[{asin}] Frais de livraison trouvés dans l'élément alternatif : {shipcost}")
                if shipcost == " Livraison GRATUITE ":
                    shipcost = 0.0
                elif shipcost == "Livraison gratuite":
                    shipcost = 0.0
                elif shipcost == "Livraison GRATUITE":
                    shipcost = 0.0
                # Si shipcost est déjà un float
                elif isinstance(shipcost, float):
                    shipcost = shipcost
                else:
                    logging.warning(f"[{asin}] Frais de livraison non trouvés ou format inattendu : {shipcost}")
                

                # Vendeur
                seller_block = offer_block.find('div', id='aod-offer-soldBy')
                if seller_block:
                    seller_name_element = seller_block.find('a', class_='a-size-small a-link-normal', role='link')
                    if seller_name_element:
                        seller_name = clean_text(seller_name_element.get_text())
                    else:
                        seller_name_span = seller_block.find('span', class_='a-size-small a-color-base')
                        seller_name = clean_text(seller_name_span.get_text()) if seller_name_span else 'N/A'
                else:
                    seller_name = 'N/A'

                # Info sur le vendeur (SI pas Amazon)
                seller_rating = pd.NA
                if seller_name.lower() != 'amazon':
                    try:
                        seller_info_element = offer_block.find('div', id="aod-offer-seller-rating")
                        logging.info(f"[{asin}] Seller info element found: {seller_info_element}")
                        if seller_info_element:
                            ##########################################################""
                            # Récupérer l'évaluation du vendeur "rating"
                            rating_element = seller_info_element.find('span', class_='a-icon-alt')
                            # Exemple : L'évaluation du vendeur est de 3.5&nbsp;étoiles sur 5
                            # On doit donc extraire le nombre d'étoiles
                            if rating_element:
                                rating_text = rating_element.get_text(strip=True)
                                rating_match = re.search(r'(\d+(?:\.\d+)?)\s*étoiles', rating_text)
                                if rating_match:
                                    seller_rating = float(rating_match.group(1))
                                    logging.debug(f"[{asin}] Seller rating found: {seller_rating}")
                                else:
                                    logging.warning(f"[{asin}] Aucun match trouvé pour l'évaluation du vendeur dans le texte : {rating_text}")
                                    seller_rating = pd.NA
                            else:
                                logging.warning(f"[{asin}] Aucun élément d'évaluation du vendeur trouvé.")
                                seller_rating = pd.NA
                            ##############################################################
                            # Récupérer le nombre d'évaluations du vendeur "ratingnb"

                            #<span id="seller-rating-count-{iter}" class="a-size-small a-color-base">  
                            # <span>(466&nbsp;évaluations) <br>57&nbsp;% positif(s) au cours des 12&nbsp;derniers mois</span>  </span>

                            # On Souhaite uniquement récupérer le nombre d'évaluations
                            rating_nb_element = seller_info_element.find('span', id=re.compile(r'seller-rating-count-\d+'))
                            if rating_nb_element:
                                rating_nb_text = rating_nb_element.get_text(strip=True)
                                rating_nb_match = re.search(r'(\d+)\s*évaluations', rating_nb_text)
                                if rating_nb_match:
                                    seller_rating_nb = int(rating_nb_match.group(1))
                                    logging.info(f"[{asin}] Seller rating count found: {seller_rating_nb}")
                                else:
                                    logging.warning(f"[{asin}] Aucun match trouvé pour le nombre d'évaluations du vendeur dans le texte : {rating_nb_text}")
                                    seller_rating_nb = pd.NA
                            else:
                                logging.warning(f"[{asin}] Aucun élément de nombre d'évaluations du vendeur trouvé.")
                                seller_rating_nb = pd.NA

                    except Exception as e:
                        logging.warning(f"[{asin}] Erreur lors de la récupération des informations sur le vendeur : {e}")
                        seller_info_text = 'N/A'
                else:
                    seller_info_text = 'N/A'

                # État
                state_element = offer_block.find('div', id='aod-offer-heading')
                logging.info(f"[{asin}] State element found: {state_element}")
                if state_element:
                    span = state_element.find('span')
                    logging.info(f"[{asin}] Span found: {span}")
                    product_state = clean_text(span.get_text()) if span else 'N/A'
                else:
                    product_state = 'N/A'

                offer_details = {
                    'pfid': "AMAZ",
                    'idsmartphone': idsmartphone,
                    'url': url,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Price': price_value,
                    'shipcost': shipcost,
                    'seller': seller_name,
                    'rating': seller_rating,
                    'ratingnb': pd.NA,
                    'offertype': product_state,
                    'offerdetails': pd.NA,
                    'shipcountry': pd.NA,
                    'sellercountry': pd.NA,
                    'descriptsmartphone': phone_name
                }
                offers.append(offer_details)
                logging.info(f"[{asin}] Offre secondaire ajoutée : {seller_name} - {price_value}€")

            except Exception as e:
                logging.warning(f"[{asin}] Erreur lors du parsing d’une offre secondaire : {e}")
    except Exception as e:
        logging.error(f"Erreur dans scrape_additional_offers pour ASIN {asin} : {e}")
    return offers


def save_offers_to_parquet(offers, filename=PARQUET_FILE): #plus utilisé
    if not offers:
        logging.info("Aucune offre à enregistrer.")
        return
    df = pd.DataFrame(offers)
    for col in ['ratingnb', 'Price', 'rating']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    try:
        if os.path.isfile(filename):
            existing_df = pd.read_parquet(filename, engine='fastparquet')
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(filename, engine='fastparquet', index=False)
        else:
            df.to_parquet(filename, engine='fastparquet', index=False)
        logging.info(f"Les données ont été enregistrées dans {filename}")
    except Exception as e:
        logging.error(f"Erreur de sauvegarde Parquet : {e}")

def save_offers_to_parquet_and_csv(offers,
                                   parquet_file=PARQUET_FILE,
                                   csv_file="/home/scraping/algo_scraping/AMAZON/amazon_offers.csv"):
    if not offers:
        logger.info("Aucune offre à enregistrer.")
        return

    df = pd.DataFrame(offers)
    for col in ['ratingnb', 'Price', 'rating']:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 1) Parquet
    try:
        if os.path.isfile(parquet_file):
            existing = pd.read_parquet(parquet_file, engine="fastparquet")
            df = pd.concat([existing, df], ignore_index=True)
        df.to_parquet(parquet_file, index=False, engine="fastparquet")
        logger.info(f"Les données ont été enregistrées dans {parquet_file}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde Parquet : {e}")

    # 2) CSV (open in text‐mode)
    try:
        header = not os.path.isfile(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            df.to_csv(f, index=False, header=header)
        logger.info(f"Les données ont été aussi enregistrées dans {csv_file}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde CSV : {e}")


def scrape_amazon_product(driver, asin, idsmartphone, phone_name, batch_id):
    offers = scrape_main_offer(driver, asin, idsmartphone, phone_name)
    offers += scrape_additional_offers(driver, asin, idsmartphone, phone_name)
    # Ajout du batch_id à chaque offre
    for offer in offers:
        offer['batch_id'] = batch_id
    save_offers_to_parquet_and_csv(offers)

if __name__ == "__main__":
    xvfb = start_xvfb()
    driver = init_driver()

    # Initialisation de batch_id
    csv_file = "amazon_offers.csv"
    if os.path.exists(csv_file):
        try:
            last_val = pd.read_csv(csv_file, encoding='utf-8').tail(1).iloc[0, -1]
            last_num = pd.to_numeric(last_val, errors='coerce')
            batch_id = int(last_num) + 1 if not pd.isna(last_num) else 0
            logging.debug(f"Batch ID initialisé à {batch_id} à partir du fichier CSV.")
        except Exception:
            batch_id = 0
            logging.error("Erreur lors de la lecture du fichier CSV. Réinitialisation de batch_id.")
    else:
        batch_id = 0
        logging.debug("Fichier CSV non trouvé. Initialisation de batch_id à 0.")

    try:
        while True:
            try:
                df = pd.read_excel(EXCEL_FILE, sheet_name='AMAZON', dtype={"idsmartphone": str})
                if 'Link_ID' in df.columns:
                    df.rename(columns={'Link_ID': 'ASIN'}, inplace=True)
                if not {'ASIN', 'idsmartphone', 'Phone'}.issubset(df.columns):
                    raise ValueError("Colonnes manquantes dans le fichier Excel")
                asins = df[['ASIN', 'idsmartphone', 'Phone']].dropna().values.tolist()
                logging.info(f"Entrain de traiter {len(asins)} ASINs")
            except Exception as e:
                logging.error(f"Erreur lecture Excel : {e}")
                time.sleep(600)
                continue

            if asins:
                sleep_time = math.ceil(SCRAPE_INTERVAL / len(asins))
                for idx, (asin, idsmartphone, phone_name) in enumerate(asins):
                    logging.info(f"Traitement de l'ASIN {asin} ({idx+1}/{len(asins)})")
                    scrape_amazon_product(driver, asin, idsmartphone, phone_name, batch_id)
                    if idx < len(asins) - 1:
                        logging.info(f"Pause de {sleep_time} secondes")
                        time.sleep(sleep_time)
                logging.info("Cycle terminé, pause de 5 minutes")
                time.sleep(300)
                # Incrémentation du batch_id après chaque cycle complet
                batch_id += 1
            else:
                logging.info("Aucun ASIN trouvé, pause de 10 minutes")
                time.sleep(600)
    except Exception as e:
        logging.error(f"Erreur dans le main loop : {e}")
    finally:
        driver.quit()
        xvfb.terminate()
        xvfb.wait()
        if os.path.exists("/tmp/.X98-lock"):
            os.remove("/tmp/.X98-lock")
