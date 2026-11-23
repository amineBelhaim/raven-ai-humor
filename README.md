# raven-ai-humor

Trouve automatiquement des tweets **récents (≤ 48 h)** et **drôles** sur l'IA, les robots et la tech,
publiés par des comptes X dont **la photo de profil représente Raven (Teen Titans)**. Les avatars sont reconnus
par analyse visuelle, pas par mots-clés.

```
Collecte ──► Dédoublonnage ──► Vision (avatar Raven ?) ──► Notation humour ──► Rapport + Telegram/Discord
 │  recherches thématiques (Top / Latest)          Claude Haiku 5.5            Claude Haiku 5.5
 │  registre des comptes Raven déjà reconnus       cache URL + SHA-256         texte + image du tweet
 └─ découverte quotidienne (fans de Teen Titans)
```

- **Aucun tweet inventé** : tout vient des réponses de l'API. Le modèle ne fait que noter les IDs qu'on lui fournit,
  et un ID inconnu est rejeté. Les liens sont construits à partir de l'ID et du compte renvoyés par l'API.
- **Pas de doublons** : un tweet déjà traité n'est jamais renoté ni renvoyé, et un texte identique publié par un
  autre compte (copier-coller) est écarté. L'état est conservé dans `data/raven.sqlite`.
- **Au-delà des mots-clés** : chaque compte reconnu comme Raven entre dans un *registre*. Tous ses tweets récents
  sont ensuite surveillés, et c'est le modèle qui décide s'ils parlent d'IA ou de tech.

## 1. Installation (local)

```bash
git clone <votre-repo> raven-ai-humor && cd raven-ai-humor
python3 -m venv .venv && source .venv/bin/activate      # Windows : .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                                      # puis remplissez les clés
```

Clés nécessaires :

| Variable | Où l'obtenir |
|---|---|
| `TWITTERAPI_IO_KEY` | twitterapi.io → Dashboard (crédits gratuits de départ, puis paiement à l'usage) |
| `ANTHROPIC_API_KEY` | platform.claude.com → API keys (prévoyez 5 $ de crédit) |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | facultatif : créez un bot avec @BotFather, envoyez-lui un message, puis lisez `chat.id` sur `https://api.telegram.org/bot<TOKEN>/getUpdates` |
| `DISCORD_WEBHOOK_URL` | facultatif : Paramètres du salon → Intégrations → Webhooks |

## 2. Premier lancement

```bash
# a) Calibrer la détection sur quelques avatars (copiez l'URL d'une photo de profil)
python raven_humor.py --check-avatar "https://pbs.twimg.com/profile_images/…_normal.jpg"

# b) Un passage complet
python raven_humor.py
cat reports/latest.md
```

Si vous connaissez déjà des comptes avec un avatar Raven, ajoutez-les dans `registry.seed_usernames`
(`config.yaml`) : c'est le moyen le plus rapide d'obtenir des résultats dès le premier jour.

## 3. Exécution automatique 24 h/24

### Option A — GitHub Actions (gratuit, sans serveur)

1. Poussez le dossier sur un **dépôt privé** (les rapports et la base y sont commités).
   Un passage prend ~2-4 min ; 4 passages/jour ≈ 360-480 min/mois, sous les 2 000 min gratuites d'un compte Free.
2. *Settings → Secrets and variables → Actions* : ajoutez `TWITTERAPI_IO_KEY`, `ANTHROPIC_API_KEY`
   (+ Telegram/Discord si besoin).
3. *Actions → raven-ai-humor → Run workflow* pour tester. Ensuite, ça tourne seul toutes les 6 h.

Limite : GitHub ne garantit pas l'heure exacte des tâches planifiées (retards possibles, parfois de plusieurs heures).
La fenêtre de 48 h et le recouvrement absorbent ces retards.

### Option B — VPS ou Raspberry Pi (le plus fiable)

```bash
sudo useradd -r -m -d /opt/raven-ai-humor raven
sudo -u raven git clone <votre-repo> /opt/raven-ai-humor
cd /opt/raven-ai-humor && sudo -u raven python3 -m venv .venv && sudo -u raven .venv/bin/pip install -r requirements.txt
sudo -u raven cp .env.example .env && sudo -u raven nano .env
sudo cp deploy/raven-humor.service deploy/raven-humor.timer /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now raven-humor.timer
systemctl list-timers raven-humor.timer          # prochain passage
journalctl -u raven-humor.service -f             # logs
```

Sans systemd : `crontab -e` puis
`17 */6 * * * cd /opt/raven-ai-humor && .venv/bin/python raven_humor.py >> logs/run.log 2>&1`
(créez `logs/` avant). Ou simplement `python raven_humor.py --loop --every 360` dans un `tmux`.

## 4. Réglages utiles (`config.yaml`)

| Paramètre | Effet |
|---|---|
| `vision.threshold` | 0.75 par défaut. Montez à 0.85 si vous voyez des faux positifs, descendez à 0.65 si des Raven sont ratées. |
| `humor.min_score` | Note minimale pour apparaître dans le rapport (0-10). |
| `topic_queries[].max_pages` | 20 tweets par page. Plus de pages = plus de couverture et un coût plus élevé. |
| `registry.max_pages_per_chunk` | Profondeur de surveillance des comptes Raven connus. |
| `limits.max_tweets_per_run`, `vision.max_checks_per_run` | Plafonds de coût par passage. |

Chaque passage affiche le nombre d'appels au modèle et le coût estimé.

## 5. Coût estimé (4 passages/jour, réglages par défaut)

| Poste | Volume / jour | Coût / mois |
|---|---|---|
| twitterapi.io (0,15 $ / 1 000 tweets) | ~2 000-3 000 tweets | ~9-14 $ |
| Claude Haiku 5.5 — avatars (cache : seuls les nouveaux avatars sont analysés) | 500-1 500 images | ~2-4 $ |
| Claude Haiku 5.5 — humour (tweets Raven uniquement) | quelques dizaines | < 0,50 $ |
| GitHub Actions (dépôt privé, < 2 000 min) | — | 0 $ |
| **Total** | | **≈ 12-18 $** |

Le premier jour coûte un peu plus, car le cache d'avatars est vide.

## 6. Passer à l'API officielle X

Mettez `TWEET_SOURCE=x_official` et `X_BEARER_TOKEN=…`. Le code est identique, mais l'API officielle facture
environ 0,005 $ par post lu et 0,010 $ par profil lu : pour le même volume, comptez **plusieurs centaines de $ par mois**.
Réduisez alors fortement `max_pages` et `x_official.page_size`. La recherche d'utilisateurs (découverte) n'est pas utilisée avec cette source.

## 7. Limites à connaître

- **Rareté** : peu de comptes ont un avatar Raven. Les recherches par sujet en trouvent quelques-uns, puis le
  registre devient la source principale. Comptez quelques jours de rodage. Les comptes ajoutés dans `seed_usernames` accélèrent le démarrage.
- **Couverture** : sur des mots-clés très actifs (« AI »), une recherche ne voit qu'une partie des tweets ;
  `Top` privilégie les plus populaires de la fenêtre.
- **twitterapi.io est un service tiers** non affilié à X : il peut changer de prix ou cesser de fonctionner, et son usage
  relève des conditions de X. Le basculement vers l'API officielle tient en une variable.
- **Vision** : les avatars très stylisés ou minuscules peuvent tromper le modèle. Consultez la table `avatars` de la base
  SQLite (colonne `reason`) pour ajuster le seuil.
- L'humour est subjectif : la note est un tri, pas une vérité. Les tweets promotionnels et haineux sont exclus.
