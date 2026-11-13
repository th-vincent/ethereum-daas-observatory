# Collecte des transferts DaaS

Objectif : récupérer, pour les 87 077 transactions de partage publiées avec l'article
IMC 2025 (<https://github.com/blocksecteam/DaaS_dataset>), tous les mouvements
d'actifs, afin de recalculer ensuite les victimes et les montants volés.

Source : API publique Routescan (compatible Etherscan), sans clé. Deux appels par
transaction, espacés d'au moins 0,55 s.

## Lancer la collecte

Onglet **Actions** → *Collecte des transferts DaaS* → **Run workflow**.

| Paramètre | Rôle |
|---|---|
| `partitions` | Nombre de jobs en parallèle (4 par défaut) |
| `max_transactions` | Limite par job. Mettre `50` pour un premier essai, `0` pour tout collecter |
| `duree_minutes` | Durée avant arrêt propre du job (330 min au maximum, GitHub coupe à 6 h) |

La collecte **reprend là où elle s'était arrêtée** : on relance le workflow jusqu'à ce que
`donnees/collecte/suivi.json` indique 0 transaction restante. Les transactions en erreur
sont réessayées au lancement suivant.

Les transactions sont traitées dans un **ordre aléatoire fixé** (graine 2025) : ce qui a
été collecté à n'importe quel moment forme donc un échantillon aléatoire, utilisable pour
un pilote.

## Fichiers produits

- `donnees/collecte/lots/*.jsonl.gz` : une ligne JSON par transaction ;
- `donnees/collecte/suivi.json` : avancement (collectées, en erreur, restantes).

Format d'une ligne :

```json
{"tx": "0x…", "statut": "ok", "bloc": 17000000, "horodatage": 1681000000,
 "expediteur": "0x…", "appele": "0x…", "succes": true,
 "eth_internes": [{"de": "0x…", "vers": "0x…", "wei": "5418472326788925000", "type": "call", "erreur": false}],
 "transferts": [{"norme": "erc20", "jeton": "0x…", "de": "0x…", "vers": "0x…", "quantite": "1000000"}],
 "autres_logs": 3, "collecte_utc": "2026-11-13T10:00:00+00:00"}
```

- `statut` : `ok`, `introuvable` (transaction inconnue de l'API) ou `erreur` (réessayée plus tard) ;
- `expediteur` : adresse qui a signé la transaction (la victime dans le cas ETH) ;
- `eth_internes` : transferts d'ETH effectués par des contrats pendant la transaction ;
- `transferts` : événements ERC-20, ERC-721 et ERC-1155 (`id` pour les NFT) ;
- `horodatage` : présent seulement s'il y a des mouvements internes ; sinon, il faudra le
  déduire du numéro de bloc (étape ultérieure).

Les montants sont des chaînes d'entiers bruts (wei, unités minimales du jeton) : aucune
conversion ni arrondi à ce stade.

## En local

```bash
git clone --depth 1 https://github.com/blocksecteam/DaaS_dataset donnees_article
python collecte/collecte.py collecter --max-transactions 20
python collecte/collecte.py ranger
python collecte/collecte.py bilan
```
