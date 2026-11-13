#!/usr/bin/env python3
"""Collecte des transferts des transactions de partage DaaS (IMC 2025) via l'API Routescan.

Sous-commandes :
  collecter           transactions publiées pas encore collectées (reprise automatique) ;
  collecter-contrats  historique complet des contrats de partage pas encore collectés ;
  ranger              compresse les lots bruts (--contrats pour ceux des contrats) ;
  bilan               avancement de la collecte des transactions (suivi.json) ;
  bilan-contrats      avancement de la collecte des contrats.

Pour chaque transaction publiée, deux appels :
  - eth_getTransactionReceipt : expéditeur, contrat appelé, succès, bloc, événements
    (transferts ERC-20, ERC-721 et ERC-1155) ;
  - txlistinternal : transferts d'ETH internes et horodatage.

Pour chaque contrat : création, transactions reçues, transferts d'ETH internes,
transferts ERC-20 / ERC-721 / ERC-1155 (6 appels, plus si l'historique dépasse une page).

Bibliothèque standard uniquement. Aucune clé ni aucun wallet requis.
"""

import argparse
import gzip
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RACINE = Path(__file__).resolve().parent.parent
DATASET = RACINE / "donnees_article"
COLLECTE = RACINE / "donnees" / "collecte"
LOTS = COLLECTE / "lots"
CONTRATS = RACINE / "donnees" / "contrats"
LOTS_CONTRATS = CONTRATS / "lots"
TAILLE_PAGE = int(os.environ.get("COLLECTE_TAILLE_PAGE", "10000"))  # maximum de l'API

API = os.environ.get(
    "COLLECTE_API", "https://api.routescan.io/v2/network/mainnet/evm/1/etherscan/api")
GRAINE = 2025  # Ordre de traitement aléatoire mais reproductible.

TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_1155_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_1155_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"


# --------------------------------------------------------------------------- lecture

def lire_liste(nom):
    chemin = DATASET / nom
    return [l for l in chemin.read_text(encoding="utf-8-sig").lower().split() if l]


def lire_enregistrements(dossiers):
    """Lit tous les lots (.jsonl ou .jsonl.gz). Une dernière ligne tronquée est ignorée."""
    for dossier in dossiers:
        if not dossier.exists():
            continue
        for chemin in sorted(dossier.glob("*.jsonl*")):
            ouvrir = gzip.open if chemin.suffix == ".gz" else open
            try:
                with ouvrir(chemin, "rt", encoding="utf-8") as f:
                    for ligne in f:
                        try:
                            yield json.loads(ligne)
                        except json.JSONDecodeError:
                            continue  # ligne coupée par un arrêt brutal
            except (OSError, EOFError) as erreur:
                print(f"Lot illisible ignoré : {chemin} ({erreur})", file=sys.stderr)


def etats_connus(dossiers, cle="tx"):
    """Dernier statut connu de chaque transaction (ou de chaque contrat, cle="contrat")."""
    etats = {}
    for e in lire_enregistrements(dossiers):
        if cle in e:
            # Un succès n'est jamais écrasé par une erreur ultérieure.
            if etats.get(e[cle]) != "ok":
                etats[e[cle]] = e.get("statut")
    return etats


# --------------------------------------------------------------------------- API

class LimiteAtteinte(Exception):
    """L'API signale une limite de débit ou de quota."""


class ClientRoutescan:
    def __init__(self, intervalle):
        self.intervalle = intervalle
        self.dernier = 0.0
        self.appels = 0

    def _get(self, parametres):
        attente = self.intervalle - (time.monotonic() - self.dernier)
        if attente > 0:
            time.sleep(attente)
        url = API + "?" + urlencode(parametres)
        demande = Request(url, headers={"Accept": "application/json",
                                        "User-Agent": "daas-reproduction/1.0"})
        self.dernier = time.monotonic()
        self.appels += 1
        try:
            with urlopen(demande, timeout=20) as reponse:
                return json.load(reponse)
        except HTTPError as erreur:
            if erreur.code == 429:
                raise LimiteAtteinte("HTTP 429") from None
            raise

    def appeler(self, parametres, essais=4):
        """Appel avec nouvelles tentatives espacées (2 s, 4 s, 8 s)."""
        for essai in range(essais):
            try:
                donnees = self._get(parametres)
                texte = json.dumps(donnees)[:300].lower()
                if "rate limit" in texte or "max calls" in texte or "too many" in texte:
                    raise LimiteAtteinte(texte[:120])
                return donnees
            except (LimiteAtteinte, HTTPError, URLError, TimeoutError,
                    ConnectionError, json.JSONDecodeError) as erreur:
                if isinstance(erreur, HTTPError) and erreur.code < 500 and erreur.code != 429:
                    raise
                if essai == essais - 1:
                    raise
                time.sleep(2 ** (essai + 1))
        raise RuntimeError("inaccessible")

    def recu(self, tx):
        d = self.appeler(dict(module="proxy", action="eth_getTransactionReceipt", txhash=tx))
        if "error" in d:
            raise ValueError(f"Erreur RPC : {d['error']}")
        return d.get("result")  # None si la transaction est inconnue

    def internes(self, tx):
        d = self.appeler(dict(module="account", action="txlistinternal", txhash=tx))
        resultat = d.get("result")
        if str(d.get("status")) == "1" and isinstance(resultat, list):
            return resultat
        message = str(d.get("message", "")).lower()
        if str(d.get("status")) == "0" and "no transactions found" in message:
            return []
        raise ValueError(f"txlistinternal : {d.get('message')} ({str(resultat)[:100]})")

    def liste(self, action, **parametres):
        """Réponse de type liste du module account ; [] si rien n'est trouvé."""
        d = self.appeler(dict(module="account", action=action, **parametres))
        resultat = d.get("result")
        if str(d.get("status")) == "1" and isinstance(resultat, list):
            return resultat
        message = str(d.get("message", "")).lower()
        if str(d.get("status")) == "0" and ("no transactions found" in message
                                            or "no records found" in message):
            return []
        raise ValueError(f"{action} : {d.get('message')} ({str(resultat)[:100]})")

    def historique(self, action, adresse):
        """Tout l'historique d'une adresse, au-delà de la limite d'une page.

        L'API ne renvoie qu'une page de TAILLE_PAGE lignes. Le dernier bloc d'une page
        pleine peut être incomplet : on écarte ses lignes et on repart de ce bloc, qui
        est alors relu en entier (pas de dédoublonnage : deux transferts identiques
        dans une même transaction restent deux transferts).
        """
        lignes, debut = [], 0
        while True:
            page = self.liste(action, address=adresse, startblock=debut, endblock=99999999,
                              page=1, offset=TAILLE_PAGE, sort="asc")
            if len(page) < TAILLE_PAGE:
                return lignes + page
            dernier = int(page[-1]["blockNumber"])
            if int(page[0]["blockNumber"]) == dernier:
                raise ValueError(f"{action} : plus de {TAILLE_PAGE} lignes dans le bloc {dernier}")
            lignes += [l for l in page if int(l["blockNumber"]) != dernier]
            debut = dernier

    def creation(self, adresse):
        d = self.appeler(dict(module="contract", action="getcontractcreation",
                              contractaddresses=adresse))
        resultat = d.get("result")
        if str(d.get("status")) == "1" and isinstance(resultat, list) and resultat:
            return resultat[0]
        raise ValueError(f"getcontractcreation : {d.get('message')} ({str(resultat)[:100]})")


# --------------------------------------------------------------------------- décodage

def adresse(mot):
    return "0x" + mot[-40:].lower()


def mots(data):
    h = (data or "0x")[2:]
    return [h[i:i + 64] for i in range(0, len(h), 64)]


def decoder_logs(logs):
    """Extrait les transferts de jetons d'une liste d'événements de reçu."""
    transferts, autres = [], 0
    for log in logs or []:
        sujets = [s.lower() for s in log.get("topics", [])]
        jeton = (log.get("address") or "").lower()
        if not sujets:
            autres += 1
            continue
        try:
            if sujets[0] == TOPIC_TRANSFER and len(sujets) == 3:
                transferts.append(dict(norme="erc20", jeton=jeton, de=adresse(sujets[1]),
                                       vers=adresse(sujets[2]),
                                       quantite=str(int(log.get("data") or "0x0", 16))))
            elif sujets[0] == TOPIC_TRANSFER and len(sujets) == 4:
                transferts.append(dict(norme="erc721", jeton=jeton, de=adresse(sujets[1]),
                                       vers=adresse(sujets[2]), id=str(int(sujets[3], 16)),
                                       quantite="1"))
            elif sujets[0] == TOPIC_1155_SINGLE and len(sujets) == 4:
                m = mots(log.get("data"))
                transferts.append(dict(norme="erc1155", jeton=jeton, de=adresse(sujets[2]),
                                       vers=adresse(sujets[3]), id=str(int(m[0], 16)),
                                       quantite=str(int(m[1], 16))))
            elif sujets[0] == TOPIC_1155_BATCH and len(sujets) == 4:
                m = mots(log.get("data"))
                o_ids, o_val = int(m[0], 16) // 32, int(m[1], 16) // 32
                n = int(m[o_ids], 16)
                ids = m[o_ids + 1:o_ids + 1 + n]
                vals = m[o_val + 1:o_val + 1 + n]
                for i, v in zip(ids, vals):
                    transferts.append(dict(norme="erc1155", jeton=jeton, de=adresse(sujets[2]),
                                           vers=adresse(sujets[3]), id=str(int(i, 16)),
                                           quantite=str(int(v, 16))))
            else:
                autres += 1
        except (ValueError, IndexError):
            autres += 1  # événement mal formé : compté, pas interprété
    return transferts, autres


def enregistrement(tx, recu, internes):
    horodatage = next((int(i["timeStamp"]) for i in internes if i.get("timeStamp")), None)
    transferts, autres = decoder_logs(recu.get("logs"))
    return {
        "tx": tx,
        "statut": "ok",
        "bloc": int(recu["blockNumber"], 16),
        "horodatage": horodatage,  # absent s'il n'y a aucun mouvement interne
        "expediteur": (recu.get("from") or "").lower(),
        "appele": (recu.get("to") or "").lower(),
        "succes": recu.get("status") == "0x1",
        "eth_internes": [
            dict(de=(i.get("from") or "").lower(), vers=(i.get("to") or "").lower(),
                 wei=str(i.get("value") or "0"), type=i.get("type", ""),
                 erreur=i.get("isError") not in ("0", 0, None))
            for i in internes
        ],
        "transferts": transferts,
        "autres_logs": autres,
        "collecte_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# --------------------------------------------------------------------------- contrats

def _bas(v):
    return (v or "").lower()


def compacter_tx(l):
    return dict(tx=_bas(l.get("hash")), bloc=int(l["blockNumber"]), ts=int(l["timeStamp"]),
                de=_bas(l.get("from")), vers=_bas(l.get("to")), wei=str(l.get("value") or "0"),
                erreur=str(l.get("isError", "0")) != "0", methode=l.get("methodId", ""),
                fonction=(l.get("functionName") or "").split("(")[0])


def compacter_interne(l):
    return dict(tx=_bas(l.get("hash")), bloc=int(l["blockNumber"]), ts=int(l["timeStamp"]),
                de=_bas(l.get("from")), vers=_bas(l.get("to")), wei=str(l.get("value") or "0"),
                type=l.get("type", ""), erreur=str(l.get("isError", "0")) != "0")


def compacter_jeton(l, norme):
    e = dict(tx=_bas(l.get("hash")), bloc=int(l["blockNumber"]), ts=int(l["timeStamp"]),
             norme=norme, jeton=_bas(l.get("contractAddress")), de=_bas(l.get("from")),
             vers=_bas(l.get("to")), symbole=l.get("tokenSymbol", ""))
    if norme == "erc20":
        e.update(quantite=str(l.get("value") or "0"), decimales=l.get("tokenDecimal"))
    else:
        e.update(id=str(l.get("tokenID", "")), quantite=str(l.get("tokenValue") or "1"))
    return e


def historique_contrat(client, contrat):
    avertissements = []
    try:
        c = client.creation(contrat)
        creation = dict(createur=_bas(c.get("contractCreator")), tx=_bas(c.get("txHash")))
    except (ValueError, LimiteAtteinte, OSError) as erreur:
        creation = None  # facultatif : certaines API ne le proposent pas
        avertissements.append(f"creation: {erreur}"[:200])

    jetons = [compacter_jeton(l, "erc20") for l in client.historique("tokentx", contrat)]
    jetons += [compacter_jeton(l, "erc721") for l in client.historique("tokennfttx", contrat)]
    try:
        jetons += [compacter_jeton(l, "erc1155") for l in client.historique("token1155tx", contrat)]
    except ValueError as erreur:
        avertissements.append(f"token1155tx: {erreur}"[:200])

    return {
        "contrat": contrat,
        "statut": "ok",
        "creation": creation,
        "transactions": [compacter_tx(l) for l in client.historique("txlist", contrat)],
        "eth_internes": [compacter_interne(l) for l in client.historique("txlistinternal", contrat)],
        "jetons": jetons,
        "avertissements": avertissements,
        "collecte_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# --------------------------------------------------------------------------- commandes

def ordre_de_traitement(transactions):
    ordre = sorted(set(transactions))
    random.Random(GRAINE).shuffle(ordre)
    return ordre


def cmd_collecter(args):
    toutes = ordre_de_traitement(lire_liste("profit_sharing_transactions.log"))
    etats = etats_connus([LOTS, Path(args.sortie).parent])
    deja = {tx for tx, s in etats.items() if s in ("ok", "introuvable")}
    a_faire = [tx for i, tx in enumerate(toutes)
               if i % args.partitions == args.partition and tx not in deja]
    if args.max_transactions:
        a_faire = a_faire[:args.max_transactions]

    print(f"Partition {args.partition}/{args.partitions} : {len(a_faire)} transactions à "
          f"collecter ({len(deja)} déjà faites sur {len(toutes)}).", flush=True)
    if not a_faire:
        return 0

    sortie = Path(args.sortie)
    sortie.parent.mkdir(parents=True, exist_ok=True)
    client = ClientRoutescan(args.intervalle)
    fin = time.monotonic() + args.duree_minutes * 60
    compte, echecs_consecutifs = Counter(), 0

    with sortie.open("a", encoding="utf-8") as f:
        for n, tx in enumerate(a_faire, 1):
            if time.monotonic() > fin:
                print("Budget de temps atteint : arrêt propre.", flush=True)
                break
            try:
                recu = client.recu(tx)
                if recu is None:
                    e = {"tx": tx, "statut": "introuvable"}
                elif (recu.get("transactionHash") or "").lower() != tx:
                    raise ValueError("le reçu ne correspond pas à la transaction demandée")
                else:
                    e = enregistrement(tx, recu, client.internes(tx))
                echecs_consecutifs = 0
            except (LimiteAtteinte, OSError, ValueError, KeyError, TypeError) as erreur:
                e = {"tx": tx, "statut": "erreur", "detail": f"{type(erreur).__name__}: {erreur}"[:300]}
                echecs_consecutifs += 1
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
            f.flush()
            compte[e["statut"]] += 1
            if n % 100 == 0 or n == len(a_faire):
                print(f"  {n}/{len(a_faire)}  {dict(compte)}  ({client.appels} appels)", flush=True)
            if echecs_consecutifs >= args.max_echecs:
                print(f"{echecs_consecutifs} échecs consécutifs (dernier : {e['detail']}). "
                      "Arrêt pour ne pas insister auprès de l'API.", flush=True)
                break

    print(f"Terminé : {dict(compte)}", flush=True)
    return 0


def cmd_collecter_contrats(args):
    tous = ordre_de_traitement(lire_liste("profit_sharing_contracts.log"))
    etats = etats_connus([LOTS_CONTRATS, Path(args.sortie).parent], cle="contrat")
    deja = {c for c, s in etats.items() if s == "ok"}
    a_faire = [c for i, c in enumerate(tous)
               if i % args.partitions == args.partition and c not in deja]
    if args.max_contrats:
        a_faire = a_faire[:args.max_contrats]

    print(f"Partition {args.partition}/{args.partitions} : {len(a_faire)} contrats à "
          f"collecter ({len(deja)} déjà faits sur {len(tous)}).", flush=True)
    if not a_faire:
        return 0

    sortie = Path(args.sortie)
    sortie.parent.mkdir(parents=True, exist_ok=True)
    client = ClientRoutescan(args.intervalle)
    fin = time.monotonic() + args.duree_minutes * 60
    compte, echecs_consecutifs = Counter(), 0

    with sortie.open("a", encoding="utf-8") as f:
        for n, contrat in enumerate(a_faire, 1):
            if time.monotonic() > fin:
                print("Budget de temps atteint : arrêt propre.", flush=True)
                break
            try:
                e = historique_contrat(client, contrat)
                echecs_consecutifs = 0
            except (LimiteAtteinte, OSError, ValueError, KeyError, TypeError) as erreur:
                e = {"contrat": contrat, "statut": "erreur",
                     "detail": f"{type(erreur).__name__}: {erreur}"[:300]}
                echecs_consecutifs += 1
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
            f.flush()
            compte[e["statut"]] += 1
            taille = (f"{len(e['transactions'])} tx, {len(e['eth_internes'])} internes, "
                      f"{len(e['jetons'])} jetons" if e["statut"] == "ok" else e["detail"][:80])
            print(f"  {n}/{len(a_faire)}  {contrat}  {e['statut']}  ({taille})", flush=True)
            if echecs_consecutifs >= args.max_echecs:
                print(f"{echecs_consecutifs} échecs consécutifs. "
                      "Arrêt pour ne pas insister auprès de l'API.", flush=True)
                break

    print(f"Terminé : {dict(compte)} ({client.appels} appels)", flush=True)
    return 0


def cmd_bilan_contrats(args):
    tous = set(lire_liste("profit_sharing_contracts.log"))
    etats = etats_connus([LOTS_CONTRATS], cle="contrat")
    statuts = Counter(etats.get(c, "restant") for c in tous)
    contenu = Counter()
    for e in lire_enregistrements([LOTS_CONTRATS]):
        if e.get("statut") == "ok":
            contenu["transactions"] += len(e["transactions"])
            contenu["eth_internes"] += len(e["eth_internes"])
            contenu["jetons"] += len(e["jetons"])
            contenu["avec_avertissement"] += bool(e["avertissements"])
    bilan = {
        "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "contrats_dataset": len(tous),
        "collectes": statuts["ok"],
        "en_erreur": statuts["erreur"],
        "restants": statuts["restant"],
        "progression_pct": round(100 * statuts["ok"] / len(tous), 2),
        "lignes_collectees": dict(contenu),
    }
    CONTRATS.mkdir(parents=True, exist_ok=True)
    (CONTRATS / "suivi.json").write_text(json.dumps(bilan, ensure_ascii=False, indent=2) + "\n",
                                         encoding="utf-8")
    texte = ["| Indicateur | Valeur |", "|---|---|"]
    texte += [f"| {k} | {v} |" for k, v in bilan.items() if k != "lignes_collectees"]
    texte += [f"| {k} | {v} |" for k, v in contenu.items()]
    print("\n".join(texte))
    resume = os.environ.get("GITHUB_STEP_SUMMARY")
    if resume:
        with open(resume, "a", encoding="utf-8") as f:
            f.write("## Collecte des contrats DaaS\n\n" + "\n".join(texte) + "\n")
    return 0


def cmd_ranger(args):
    """Compresse chaque lot brut dans le dossier de lots choisi."""
    destination = LOTS_CONTRATS if args.contrats else LOTS
    destination.mkdir(parents=True, exist_ok=True)
    source = Path(args.source)
    fichiers = sorted(source.rglob("*.jsonl")) if source.exists() else []
    for chemin in fichiers:
        lignes = []
        with open(chemin, encoding="utf-8") as f:
            for ligne in f:
                try:
                    json.loads(ligne)
                    lignes.append(ligne if ligne.endswith("\n") else ligne + "\n")
                except json.JSONDecodeError:
                    continue
        if not lignes:
            continue
        cible = destination / (chemin.name + ".gz")
        with gzip.open(cible, "wt", encoding="utf-8") as g:
            g.writelines(lignes)
        print(f"{cible.relative_to(RACINE)} : {len(lignes)} enregistrements")
    if not fichiers:
        print("Aucun lot à ranger.")
    return 0


def cmd_bilan(args):
    toutes = set(lire_liste("profit_sharing_transactions.log"))
    etats = etats_connus([LOTS])
    statuts = Counter(etats.get(tx, "restant") for tx in toutes)
    contenu = Counter()
    for e in lire_enregistrements([LOTS]):
        if e.get("statut") == "ok":
            contenu["avec_eth_interne"] += bool(any(int(i["wei"]) > 0 for i in e["eth_internes"]))
            contenu["avec_erc20"] += any(t["norme"] == "erc20" for t in e["transferts"])
            contenu["avec_nft"] += any(t["norme"] != "erc20" for t in e["transferts"])
            contenu["echouees_onchain"] += not e["succes"]
    bilan = {
        "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "transactions_dataset": len(toutes),
        "collectees": statuts["ok"],
        "introuvables": statuts["introuvable"],
        "en_erreur": statuts["erreur"],
        "restantes": statuts["restant"],
        "progression_pct": round(100 * (statuts["ok"] + statuts["introuvable"]) / len(toutes), 2),
        "contenu_des_collectees": dict(contenu),
    }
    COLLECTE.mkdir(parents=True, exist_ok=True)
    (COLLECTE / "suivi.json").write_text(json.dumps(bilan, ensure_ascii=False, indent=2) + "\n",
                                         encoding="utf-8")
    texte = ["| Indicateur | Valeur |", "|---|---|"]
    texte += [f"| {k} | {v} |" for k, v in bilan.items() if k != "contenu_des_collectees"]
    texte += [f"| {k} | {v} |" for k, v in contenu.items()]
    print("\n".join(texte))
    resume = os.environ.get("GITHUB_STEP_SUMMARY")
    if resume:
        with open(resume, "a", encoding="utf-8") as f:
            f.write("## Collecte DaaS\n\n" + "\n".join(texte) + "\n")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="commande", required=True)

    c = sp.add_parser("collecter", help="interroger l'API")
    c.add_argument("--partition", type=int, default=0)
    c.add_argument("--partitions", type=int, default=1)
    c.add_argument("--max-transactions", type=int, default=0, help="0 = sans limite")
    c.add_argument("--duree-minutes", type=float, default=330)
    c.add_argument("--intervalle", type=float, default=0.55, help="secondes entre deux appels")
    c.add_argument("--max-echecs", type=int, default=25, help="échecs consécutifs avant arrêt")
    c.add_argument("--sortie", default=str(RACINE / "lots_bruts" / "lot.jsonl"))
    c.set_defaults(fonction=cmd_collecter)

    cc = sp.add_parser("collecter-contrats", help="historique complet des contrats de partage")
    cc.add_argument("--partition", type=int, default=0)
    cc.add_argument("--partitions", type=int, default=1)
    cc.add_argument("--max-contrats", type=int, default=0, help="0 = sans limite")
    cc.add_argument("--duree-minutes", type=float, default=330)
    cc.add_argument("--intervalle", type=float, default=0.55, help="secondes entre deux appels")
    cc.add_argument("--max-echecs", type=int, default=5, help="échecs consécutifs avant arrêt")
    cc.add_argument("--sortie", default=str(RACINE / "lots_bruts_contrats" / "lot.jsonl"))
    cc.set_defaults(fonction=cmd_collecter_contrats)

    r = sp.add_parser("ranger", help="compresser les lots bruts")
    r.add_argument("--source", default=None)
    r.add_argument("--contrats", action="store_true", help="lots de la collecte des contrats")
    r.set_defaults(fonction=cmd_ranger)

    b = sp.add_parser("bilan", help="état d'avancement (transactions)")
    b.set_defaults(fonction=cmd_bilan)

    bc = sp.add_parser("bilan-contrats", help="état d'avancement (contrats)")
    bc.set_defaults(fonction=cmd_bilan_contrats)

    args = p.parse_args()
    if args.commande == "ranger" and args.source is None:
        args.source = str(RACINE / ("lots_bruts_contrats" if args.contrats else "lots_bruts"))
    if getattr(args, "partitions", 1) < 1 or not 0 <= getattr(args, "partition", 0) < getattr(args, "partitions", 1):
        p.error("partition invalide")
    try:
        return args.fonction(args)
    except OSError as erreur:
        print(f"Erreur de lecture/écriture : {erreur}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
