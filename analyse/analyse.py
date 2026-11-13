#!/usr/bin/env python3
"""Recalcule les chiffres de l'article IMC 2025 à partir des transactions déjà collectées.

Utilisable à tout moment de la collecte : les transactions étant collectées dans un ordre
aléatoire, l'analyse porte sur un échantillon aléatoire et les totaux sont extrapolés.

Méthode (article, §4.2, §4.3 et §5.1) :
  - versement : transfert d'ETH (interne) ou d'ERC-20 vers un opérateur ou un affilié
    publié, depuis une adresse qui n'en est pas un ;
  - paire de partage : versements d'un même actif, depuis une même adresse, vers un
    opérateur et un affilié ; part opérateur = montant opérateur / total ;
  - gains : somme des versements (ce que reçoivent opérateurs et affiliés) ;
  - victimes : sources des jetons volés (ERC-20 ou NFT) envoyés aux comptes DaaS ;
    à défaut, l'expéditeur de la transaction (cas ETH, où la victime envoie elle-même).
Les NFT ne sont pas valorisés : l'article indique qu'ils sont revendus et que c'est
l'ETH obtenu qui est partagé, donc déjà compté.
"""

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import donnees

ETH = "eth"
ZERO = "0x" + "0" * 40
RATIOS_CONNUS = (10, 12.5, 15, 17.5, 20, 25, 30, 33, 40)  # part opérateur, en %
ARTICLE = {
    "transactions": 87077,
    "gains_operateurs_usd": 23.1e6,
    "gains_affilies_usd": 111.9e6,
    "victimes": 76582,
    "debut": "2023-03-01",
    "fin": "2025-04-01",
}
CLE_PRIX_ETH = "coingecko:ethereum"
FICHIER_PRIX = donnees.RACINE / "donnees" / "prix" / "prix.json"


# --------------------------------------------------------------------------- par transaction

def flux(e):
    """Mouvements d'actifs fongibles : (actif, de, vers, quantité entière)."""
    for i in e["eth_internes"]:
        if not i["erreur"] and int(i["wei"]) > 0:
            yield ETH, i["de"], i["vers"], int(i["wei"])
    for t in e["transferts"]:
        if t["norme"] == "erc20" and int(t["quantite"]) > 0:
            yield t["jeton"], t["de"], t["vers"], int(t["quantite"])


def analyser_transaction(e, listes):
    op, aff, contrats = listes["operateurs"], listes["affilies"], listes["contrats"]
    daas = op | aff
    attaquants = daas | contrats

    # Versements vers opérateurs / affiliés, regroupés par (actif, source).
    groupes = defaultdict(lambda: {"op": 0, "aff": 0, "op_adr": set(), "aff_adr": set()})
    for actif, de, vers, q in flux(e):
        if vers not in daas or de in daas:
            continue
        g = groupes[(actif, de)]
        if vers in op:  # une adresse dans les deux listes est traitée comme opérateur
            g["op"] += q
            g["op_adr"].add(vers)
        else:
            g["aff"] += q
            g["aff_adr"].add(vers)

    paires = []
    for (actif, source), g in groupes.items():
        total = g["op"] + g["aff"]
        paires.append({
            "actif": actif, "source": source, "op": g["op"], "aff": g["aff"],
            "complete": g["op"] > 0 and g["aff"] > 0,
            "part_op": float(Decimal(g["op"]) * 100 / total) if total else None,
            "op_adr": sorted(g["op_adr"]), "aff_adr": sorted(g["aff_adr"]),
        })

    # Victimes : sources de jetons (ERC-20 ou NFT) remis aux comptes DaaS ou aux contrats.
    victimes = {t["de"] for t in e["transferts"]
                if t["vers"] in attaquants and t["de"] not in attaquants and t["de"] != ZERO}
    origine_victime = "jetons"
    if not groupes:
        victimes = set()  # aucun versement aux comptes DaaS : pas de vol confirmé
    elif not victimes:
        origine_victime = "expediteur"
        if e["expediteur"] and e["expediteur"] not in attaquants:
            victimes = {e["expediteur"]}

    actifs = {p["actif"] == ETH for p in paires}
    cas = {frozenset(): "aucun", frozenset({True}): "eth",
           frozenset({False}): "erc20"}.get(frozenset(actifs), "eth+erc20")
    return {"paires": paires, "victimes": victimes, "origine_victime": origine_victime,
            "cas": cas}


# --------------------------------------------------------------------------- prix

def charger_prix():
    if not FICHIER_PRIX.exists():
        return None
    return json.loads(FICHIER_PRIX.read_text(encoding="utf-8"))


def jour(horodatage):
    return datetime.fromtimestamp(horodatage, timezone.utc).strftime("%Y-%m-%d")


def cle_prix(actif):
    return CLE_PRIX_ETH if actif == ETH else "ethereum:" + actif


def besoins_prix(transactions, listes):
    """Couples (jour, clé d'actif) à valoriser : utilisé par prix.py."""
    horloge = donnees.Horloge(transactions)
    besoins = defaultdict(set)
    for e in transactions.values():
        ts = horloge.date(e["bloc"], e.get("horodatage"))
        if ts is None:
            continue
        for p in analyser_transaction(e, listes)["paires"]:
            besoins[jour(ts)].add(cle_prix(p["actif"]))
    return besoins


def valeur_usd(prix, j, actif, quantite):
    """None si l'actif n'a pas de prix ce jour-là."""
    info = (prix or {}).get(j, {}).get(cle_prix(actif))
    if not info or info.get("price") is None:
        return None
    decimales = 18 if actif == ETH else info.get("decimals")
    if decimales is None:
        return None
    return float(Decimal(quantite) / (Decimal(10) ** int(decimales))) * float(info["price"])


# --------------------------------------------------------------------------- agrégation

def ratio_proche(part):
    if part is None:
        return "?"
    proche = min(RATIOS_CONNUS, key=lambda r: abs(r - part))
    return f"{proche:g} %" if abs(proche - part) <= 0.6 else "autre"


def analyser(transactions, listes):
    horloge = donnees.Horloge(transactions)
    prix = charger_prix()
    lignes, cas, ratios = [], Counter(), Counter()
    roles_par_montant = Counter()
    eth = {"op": 0, "aff": 0}
    usd = {"op": 0.0, "aff": 0.0}
    non_valorise = Counter()
    pertes_victimes = defaultdict(float)
    victimes_toutes, origine = set(), Counter()
    dates = []

    for e in transactions.values():
        r = analyser_transaction(e, listes)
        ts = horloge.date(e["bloc"], e.get("horodatage"))
        j = jour(ts) if ts else None
        if ts:
            dates.append(ts)
        cas[r["cas"]] += 1
        origine[r["origine_victime"] if r["victimes"] else "aucune"] += 1
        victimes_toutes |= r["victimes"]

        usd_tx = {"op": 0.0, "aff": 0.0}
        for p in r["paires"]:
            if p["complete"]:
                ratios[ratio_proche(p["part_op"])] += 1
                roles_par_montant["petit = opérateur" if p["op"] < p["aff"]
                                  else "petit = affilié" if p["op"] > p["aff"]
                                  else "parts égales"] += 1
            if p["actif"] == ETH:
                eth["op"] += p["op"]
                eth["aff"] += p["aff"]
            for role in ("op", "aff"):
                if not p[role]:
                    continue
                if prix is None:
                    continue
                v = valeur_usd(prix, j, p["actif"], p[role]) if j else None
                if v is None:
                    non_valorise[ETH if p["actif"] == ETH else "erc20"] += 1
                else:
                    usd_tx[role] += v
        usd["op"] += usd_tx["op"]
        usd["aff"] += usd_tx["aff"]
        for victime in r["victimes"]:  # perte répartie entre les victimes de la transaction
            pertes_victimes[victime] += (usd_tx["op"] + usd_tx["aff"]) / len(r["victimes"])

        premiere = next((p for p in r["paires"] if p["complete"]), None)
        lignes.append({
            "transaction": e["tx"], "date": j or "", "cas": r["cas"],
            "nb_paires": sum(p["complete"] for p in r["paires"]),
            "part_operateur_pct": f"{premiere['part_op']:.2f}" if premiere else "",
            "ratio": ratio_proche(premiere["part_op"]) if premiere else "",
            "eth_operateur": str(Decimal(sum(p["op"] for p in r["paires"] if p["actif"] == ETH)) / 10**18),
            "eth_affilie": str(Decimal(sum(p["aff"] for p in r["paires"] if p["actif"] == ETH)) / 10**18),
            "usd_operateur": f"{usd_tx['op']:.2f}" if prix else "",
            "usd_affilie": f"{usd_tx['aff']:.2f}" if prix else "",
            "victimes": "|".join(sorted(r["victimes"])),
            "origine_victime": r["origine_victime"] if r["victimes"] else "",
            "lien": f"https://etherscan.io/tx/{e['tx']}",
        })

    n = len(transactions)
    facteur = ARTICLE["transactions"] / n
    victimes_moins_1000 = (sum(v < 1000 for v in pertes_victimes.values()) / len(pertes_victimes)
                           if prix and pertes_victimes else None)
    bilan = {
        "date_analyse_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "transactions_analysees": n,
        "part_du_dataset_pct": round(100 * n / ARTICLE["transactions"], 2),
        "facteur_extrapolation": round(facteur, 2),
        "periode": [jour(min(dates)), jour(max(dates))] if dates else None,
        "cas": dict(cas),
        "ratios_part_operateur": dict(ratios.most_common()),
        "roles_selon_montants": dict(roles_par_montant),
        "gains_eth": {k: float(Decimal(v) / 10**18) for k, v in eth.items()},
        "prix_disponibles": prix is not None,
        "gains_usd_echantillon": usd if prix else None,
        "gains_usd_extrapoles": {k: v * facteur for k, v in usd.items()} if prix else None,
        "versements_non_valorises": dict(non_valorise),
        "victimes_echantillon": len(victimes_toutes),
        "victimes_extrapolees_naif": round(len(victimes_toutes) * facteur),
        "origine_victime": dict(origine),
        "victimes_perte_moins_1000_usd_pct": round(100 * victimes_moins_1000, 1)
        if victimes_moins_1000 is not None else None,
    }
    return bilan, lignes


# --------------------------------------------------------------------------- affichage

def millions(x):
    return f"{x / 1e6:,.1f} M$".replace(",", " ")


def afficher(bilan):
    b, a = bilan, ARTICLE
    print(f"Transactions analysées : {b['transactions_analysees']} "
          f"({b['part_du_dataset_pct']} % du dataset, extrapolation ×{b['facteur_extrapolation']})")
    if b["periode"]:
        print(f"Période observée       : {b['periode'][0]} → {b['periode'][1]}  "
              f"(article : {a['debut']} → {a['fin']})")
    print(f"Cas                    : {b['cas']}")
    print(f"Part de l'opérateur    : {b['ratios_part_operateur']}")
    print(f"Rôles selon montants   : {b['roles_selon_montants']}")
    print(f"Gains en ETH (natif)   : opérateurs {b['gains_eth']['op']:.2f} ETH, "
          f"affiliés {b['gains_eth']['aff']:.2f} ETH")
    print()
    print("Indicateur                | Échantillon | Extrapolé  | Article")
    print("--------------------------|-------------|------------|-----------")
    if b["prix_disponibles"]:
        for cle, nom in (("op", "Gains opérateurs"), ("aff", "Gains affiliés")):
            article = a["gains_operateurs_usd" if cle == "op" else "gains_affilies_usd"]
            print(f"{nom:<26}| {millions(b['gains_usd_echantillon'][cle]):>11} | "
                  f"{millions(b['gains_usd_extrapoles'][cle]):>10} | {millions(article)}")
    else:
        print("Gains en dollars          | (lancer d'abord analyse/prix.py)")
    print(f"{'Victimes (naïf)':<26}| {b['victimes_echantillon']:>11} | "
          f"{b['victimes_extrapolees_naif']:>10} | {a['victimes']}")
    if b["victimes_perte_moins_1000_usd_pct"] is not None:
        print(f"{'Victimes < 1 000 $':<26}| {b['victimes_perte_moins_1000_usd_pct']:>10} % |"
              f"            | 83.5 %")
    if b["versements_non_valorises"]:
        print(f"\nVersements sans prix : {b['versements_non_valorises']}")
    print("\nNB : l'extrapolation des victimes surestime légèrement (une victime volée plusieurs")
    print("fois peut être comptée plusieurs fois) ; elle devient exacte en fin de collecte.")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lots", default=str(donnees.LOTS), help="dossier des lots collectés")
    args = parser.parse_args()
    try:
        listes = donnees.lire_listes()
        transactions = donnees.lire_transactions(args.lots)
    except OSError as erreur:
        sys.exit(f"Lecture impossible : {erreur}")
    if not transactions:
        sys.exit("Aucune transaction collectée : lancer d'abord la collecte (et git pull).")

    bilan, lignes = analyser(transactions, listes)
    donnees.RESULTATS.mkdir(exist_ok=True)
    (donnees.RESULTATS / "analyse_bilan.json").write_text(
        json.dumps(bilan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with open(donnees.RESULTATS / "analyse_transactions.csv", "w", encoding="utf-8-sig",
              newline="") as f:
        tableau = csv.DictWriter(f, fieldnames=list(lignes[0]), delimiter=";")
        tableau.writeheader()
        tableau.writerows(sorted(lignes, key=lambda l: (l["cas"] != "aucun", l["date"])))
    afficher(bilan)
    print(f"\nDétail : {donnees.RESULTATS / 'analyse_transactions.csv'} "
          "(les transactions « aucun » en premier, à examiner)")


if __name__ == "__main__":
    main()
