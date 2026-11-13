#!/usr/bin/env python3
"""Récupère le prix en dollars des actifs versés, jour par jour, via l'API publique DefiLlama.

Prix retenu : celui du jour de la transaction, à midi UTC (l'article ne précise pas sa
méthode). Les résultats sont mis en cache dans donnees/prix/prix.json : relancer le script
ne demande que les prix manquants (nouveaux lots collectés). Un actif sans prix connu
(jeton sans valeur, jeton frauduleux…) est noté une fois pour toutes avec price = null.

API : https://coins.llama.fi (gratuite, sans clé).
"""

import json
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import analyse
import donnees

API = "https://coins.llama.fi/prices/historical/{ts}/{cles}?searchWidth=12h"
PAR_REQUETE = 40  # clés par appel (longueur d'URL raisonnable)
INTERVALLE = 0.3


def demander(ts, cles):
    url = API.format(ts=ts, cles=quote(",".join(cles), safe=",:"))
    for essai in range(4):
        try:
            demande = Request(url, headers={"User-Agent": "daas-reproduction/1.0"})
            with urlopen(demande, timeout=30) as r:
                return json.load(r).get("coins", {})
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as erreur:
            if essai == 3:
                raise
            print(f"  nouvelle tentative ({erreur})", file=sys.stderr)
            time.sleep(2 ** (essai + 1))
    return {}


def main():
    listes = donnees.lire_listes()
    transactions = donnees.lire_transactions()
    if not transactions:
        sys.exit("Aucune transaction collectée.")
    besoins = analyse.besoins_prix(transactions, listes)

    fichier = analyse.FICHIER_PRIX
    fichier.parent.mkdir(parents=True, exist_ok=True)
    cache = json.loads(fichier.read_text(encoding="utf-8")) if fichier.exists() else {}
    manquants = {j: sorted(c - set(cache.get(j, {}))) for j, c in besoins.items()}
    manquants = {j: c for j, c in manquants.items() if c}
    total = sum(len(c) for c in manquants.values())
    print(f"{len(besoins)} jours, {total} prix à récupérer.")

    fait = 0
    try:
        for n, j in enumerate(sorted(manquants), 1):
            midi = int(datetime.strptime(j, "%Y-%m-%d").replace(
                hour=12, tzinfo=timezone.utc).timestamp())
            for i in range(0, len(manquants[j]), PAR_REQUETE):
                paquet = manquants[j][i:i + PAR_REQUETE]
                reponse = demander(midi, paquet)
                jour_cache = cache.setdefault(j, {})
                for cle in paquet:
                    info = reponse.get(cle) or {}
                    jour_cache[cle] = {k: info.get(k) for k in
                                       ("price", "decimals", "symbol", "confidence")}
                fait += len(paquet)
                time.sleep(INTERVALLE)
            if n % 50 == 0:
                print(f"  {n}/{len(manquants)} jours ({fait}/{total} prix)", flush=True)
    finally:
        # Sauvegarde même en cas d'interruption : rien n'est redemandé ensuite.
        fichier.write_text(json.dumps(cache, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
                           encoding="utf-8")

    sans_prix = sum(v.get("price") is None for jour in cache.values() for v in jour.values())
    print(f"Terminé : {fait} prix ajoutés ; {sans_prix} couples (jour, actif) sans prix connu.")
    print(f"Cache : {fichier}")


if __name__ == "__main__":
    main()
