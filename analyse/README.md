# Analyse : recalcul des chiffres de l'article

Fonctionne à n'importe quel stade de la collecte. Les transactions étant collectées dans
un ordre aléatoire, l'échantillon est représentatif et les totaux sont extrapolés
(× 87 077 / nombre de transactions analysées).

```bash
git pull                        # récupérer les derniers lots
python3 analyse/prix.py         # prix en dollars (ne demande que ceux qui manquent)
python3 analyse/analyse.py      # bilan et comparaison avec l'article
```

`prix.py` est facultatif : sans lui, l'analyse donne tout sauf les montants en dollars.

## Méthode (article §4.2, §4.3, §5.1)

| Notion | Définition retenue |
|---|---|
| Versement | Transfert d'ETH (interne) ou d'ERC-20 vers un opérateur ou un affilié publié, depuis une adresse qui n'en est pas un |
| Paire de partage | Versements d'un même actif depuis une même adresse vers un opérateur et un affilié |
| Part opérateur | Montant opérateur / (opérateur + affilié), rapprochée des ratios de l'article (10, 12,5, 15, 17,5, 20, 25, 30, 33, 40 %) |
| Gains | Somme des versements, convertie au prix du jour (midi UTC, DefiLlama) |
| Victimes | Sources des jetons (ERC-20, NFT) remis aux comptes DaaS ; à défaut, l'expéditeur de la transaction (cas ETH) ; aucune s'il n'y a aucun versement |
| NFT | Non valorisés : revendus, c'est l'ETH obtenu qui est partagé (déjà compté) |
| Date | Horodatage collecté, sinon interpolée à partir du numéro de bloc |

## Résultats

- Affichage : tableau échantillon / extrapolé / article ;
- `resultats/analyse_bilan.json` : tous les indicateurs ;
- `resultats/analyse_transactions.csv` : une ligne par transaction, avec lien Etherscan.
  Les transactions classées `aucun` (aucun versement reconnu) sont en tête : ce sont
  celles à examiner à la main.

## Limites connues

- Le nombre de victimes extrapolé surestime un peu tant que la collecte est partielle
  (une victime volée plusieurs fois peut être comptée plusieurs fois).
- La part de victimes ayant perdu moins de 1 000 $ n'est fiable qu'en fin de collecte
  (on ne voit qu'une partie des vols de chaque victime).
- Dans le cas NFT, la victime n'apparaît que si le NFT est transféré dans la transaction
  de partage elle-même ; sinon, c'est l'expéditeur qui est retenu.
- Les jetons sans prix connu (jetons frauduleux, illiquides) ne sont pas valorisés : leur
  nombre est affiché.
