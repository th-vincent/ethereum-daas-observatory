"""Lecture commune : listes publiées avec l'article et lots collectés."""

import bisect
import gzip
import json
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
DATASET = RACINE / "donnees_article"
LOTS = RACINE / "donnees" / "collecte" / "lots"
RESULTATS = RACINE / "resultats"

SECONDES_PAR_BLOC = 12  # depuis le passage d'Ethereum en preuve d'enjeu (sept. 2022)


def lire_liste(nom):
    texte = (DATASET / nom).read_text(encoding="utf-8-sig")
    return {ligne for ligne in texte.lower().split() if ligne}


def lire_listes():
    return {
        "operateurs": lire_liste("operator_accounts.log"),
        "affilies": lire_liste("affiliate_accounts.log"),
        "contrats": lire_liste("profit_sharing_contracts.log"),
        "transactions": lire_liste("profit_sharing_transactions.log"),
    }


def lire_transactions(dossier=LOTS):
    """Un enregistrement « ok » par transaction, quel que soit le nombre de lots."""
    resultat = {}
    for chemin in sorted(Path(dossier).glob("*.jsonl*")):
        ouvrir = gzip.open if chemin.suffix == ".gz" else open
        with ouvrir(chemin, "rt", encoding="utf-8") as f:
            for ligne in f:
                try:
                    e = json.loads(ligne)
                except json.JSONDecodeError:
                    continue
                if e.get("statut") == "ok":
                    resultat[e["tx"]] = e
    return resultat


class Horloge:
    """Date d'un bloc : exacte quand la collecte l'a fournie, sinon interpolée.

    Environ 40 % des transactions ont un horodatage (celles avec de l'ETH interne).
    Comme elles sont tirées au hasard sur toute la période, elles servent de repères
    pour estimer la date des autres par interpolation linéaire entre deux blocs.
    """

    def __init__(self, transactions):
        reperes = sorted({(e["bloc"], e["horodatage"]) for e in transactions.values()
                          if e.get("horodatage")})
        self.blocs = [b for b, _ in reperes]
        self.dates = [t for _, t in reperes]

    def date(self, bloc, connue=None):
        if connue:
            return connue
        if not self.blocs:
            return None
        i = bisect.bisect_left(self.blocs, bloc)
        if i < len(self.blocs) and self.blocs[i] == bloc:
            return self.dates[i]
        if i == 0:
            return self.dates[0] - (self.blocs[0] - bloc) * SECONDES_PAR_BLOC
        if i == len(self.blocs):
            return self.dates[-1] + (bloc - self.blocs[-1]) * SECONDES_PAR_BLOC
        b0, b1, t0, t1 = self.blocs[i - 1], self.blocs[i], self.dates[i - 1], self.dates[i]
        return round(t0 + (t1 - t0) * (bloc - b0) / (b1 - b0))
