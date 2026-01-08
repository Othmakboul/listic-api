# Documentation Technique - Projet LISTIC

Ce document décrit l'architecture technique, les choix technologiques et sert de guide pour les futurs contributeurs souhaitant modifier ou étendre le projet LISTIC.

## 1. Architecture du Système

Le projet repose sur une architecture **micro-services** conteneurisée via Docker.

### Composants
1.  **Frontend (Conteneur `frontend`)** :
    *   **Rôle** : Interface utilisateur, visualisation de données, graphe interactif.
    *   **Port** : `5173` (Dev) / `80` (Prod via Nginx).
    *   **Communication** : Envoie des requêtes HTTP au Backend via `http://localhost:8000`.

2.  **Backend (Conteneur `backend`)** :
    *   **Rôle** : API REST, logique métier, aggrégation de données.
    *   **Port** : `8000`.
    *   **Communication** : Interroge la base de données MongoDB.

3.  **Base de Données (Conteneur `mongodb`)** :
    *   **Rôle** : Stockage persistant des chercheurs, projets, et publications.
    *   **Port** : `27017`.

---

## 2. Stack Technique

### Frontend
*   **Framework** : React 19 (via Vite).
*   **Langage** : JavaScript (ES6+) / JSX.
*   **Styling** : TailwindCSS (Utilitaire CSS).
*   **Visualisation Graphe** : `react-force-graph-2d` (Basé sur le moteur physique D3.js).
*   **Graphiques** : `recharts` (Pour les histogrammes et camemberts).
*   **Icônes** : `lucide-react`.
*   **HTTP Client** : `axios`.

### Backend
*   **Framework** : FastAPI (Python 3.9+).
*   **Serveur** : Uvicorn (ASGI).
*   **Base de données Driver** : `motor` (Driver MongoDB asynchrone).
*   **Validation** : Pydantic.

---

## 3. Structure du Projet

### Frontend (`/frontend`)
*   `src/main.jsx` : Point d'entrée de l'application React.
*   `src/App.jsx` : Gestion du Routing (Navigation).
*   `src/lib/api.js` : Configuration Axios centralisée (URL de base, etc.).
*   `src/pages/` :
    *   `NetworkGraph.jsx` : **Composant complexe**. Contient la logique du graphe (nœuds, liens, physique D3, recherche).
    *   `GlobalDashboard.jsx` : Statistiques globales du laboratoire.
    *   `ResearcherDetails.jsx` : Page de détail d'un chercheur.

### Backend (`/backend`)
*   `main.py` : Point d'entrée unique. Contient toutes les routes API (`@app.get(...)`).
*   `listic_personnes.complete_structure.json` : Fichier de données initiales (parfois utilisé pour le seed).
*   `Dockerfile` : Configuration de l'image Python.

---

## 4. Guide de Modification : Frontend

### Comment modifier le Graphe Réseau ?
Le fichier clé est `src/pages/NetworkGraph.jsx`.

1.  **Modifier la physique (flottement, collision)** :
    *   Cherchez le hook `useEffect` contenant `fgRef.current.d3Force`.
    *   `charge` : Répulsion entre les nœuds (négatif = repousse).
    *   `link` : Longueur des liens.
    *   `velocityDecay` : "Friction" (0 = glace, 1 = miel). Augmenter pour stabiliser.

2.  **Ajouter un type de nœud** :
    *   Dans `handleNodeClick`, ajoutez une condition `if (node.type === 'nouveau_type')`.
    *   Définissez les nouveaux nœuds et liens à ajouter via `addNodesAndLinks`.

3.  **Modifier l'apparence des nœuds** :
    *   Cherchez la prop `nodeCanvasObject` dans le composant `<ForceGraph2D>`. C'est ici que le dessin est fait via l'API Canvas HTML5 (`ctx.fillRect`, `ctx.fillText`).

### Comment ajouter une page ?
1.  Créez le composant dans `src/pages/MaNouvellePage.jsx`.
2.  Ajoutez la route dans `src/App.jsx` : `<Route path="/ma-page" element={<MaNouvellePage />} />`.

---

## 5. Guide de Modification : Backend

### Comment ajouter une route API (Endpoint) ?
Le fichier clé est `main.py`.

1.  Définissez une nouvelle fonction asynchrone décorée :
    ```python
    @app.get("/api/ma-nouvelle-route")
    async def get_data():
        data = await db["ma_collection"].find_one(...)
        return {"result": data}
    ```

2.  Si vous modifiez la base de données, assurez-vous que les clés correspondent aux modèles Pydantic (si utilisés) ou gérez les `ObjectId` de MongoDB (convertir en string).

---

## 6. Commandes Utiles

*   **Lancer le projet (reconstruction)** :
    ```bash
    docker compose down
    docker compose up --build
    ```
*   **Voir les logs** :
    ```bash
    docker compose logs -f
    ```
*   **Lancer seulement le backend** :
    ```bash
    docker compose up backend
    ```
