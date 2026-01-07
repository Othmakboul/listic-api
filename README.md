# LISTIC Intelligence Dashboard

## 🌍 Overview
The **LISTIC Intelligence Dashboard** is a comprehensive web visualization platform designed for the **Laboratory of Informatics, Systems, Information Processing and Knowledge (LISTIC)** at Université Savoie Mont Blanc. It aggregates, analyzes, and visualizes academic data including researcher profiles, publication statistics, project portfolios, and collaboration networks.

This project aims to provide a clear window into the laboratory's scientific output, enabling researchers and administrators to track performance, identify collaboration opportunities, and export data for reporting.

![Dashboard Preview](https://via.placeholder.com/1200x600?text=LISTIC+Dashboard+Preview)

## 🚀 Quick Start (Docker)

**The entire application is containerized and designed to run with a single command.**

### Prerequisites
- Docker & Docker Compose

### Launching the Application
Navigate to the project root (or the backend folder containing `docker-compose.yml`) and run:

```bash
docker compose up --build
```
*Note: The first build might take a few minutes as it compiles the React frontend and installs Python dependencies.*

Once running, access the dashboard at:
👉 **http://localhost:5173**

The Backend API documentation (Swagger UI) is available at:
👉 **http://localhost:8000/docs**

---

## 🛠️ Detailed Technical Architecture

The project follows a decoupled **Client-Server architecture** using Docker for orchestration.

### 1. Frontend Client (`/frontend`)
A modern Single Page Application (SPA) built for performance and interactivity.
- **Core**: React 18 + Vite (for ultra-fast HMR and building).
- **Routing**: `react-router-dom` v6 for seamless client-side navigation.
- **State Management**: React Hooks (`useState`, `useEffect`) for local state.
- **Data Visualization**:
    - `recharts`: For responsive statistical charts (Line, Bar, Pie, Radar).
    - `react-force-graph`: For the interactive researcher collaboration network.
- **Styling**: `Tailwind CSS` for utility-first styling, ensuring consistency and dark mode support.
- **Production Serving**: Served via an **Nginx** container in production mode (multi-stage Docker build).

### 2. Backend API (`/backend`)
A high-performance asynchronous REST API that serves as the intelligence layer.
- **Framework**: **FastAPI** (Python), chosen for its speed (Starlette) and automatic validation (Pydantic).
- **Data Aggregation Services**:
    - `services/hal.py`: Interfaces with the **HAL Open API**. It implements advanced query logic to fetch publications by author ID structure or project acronyms. Includes retry logic and response parsing.
    - `services/dblp.py`: XML parsing service for the **DBLP** database to supplement HAL data.
- **Data Persistence**:
    - Uses **Motor** (AsyncIOMotorClient) for non-blocking MongoDB interactions (if enabled).
    - caching mechanisms to reduce external API load.
- **CORS**: Configured to allow cross-origin requests from the frontend container.

### 3. Data Layer (`/listic-database`)
- **Primary Source**: JSON-based "Gold Standard" data files (Personnes, Projets, Partenaires).
- **Structure**:
    - `listic_personnes.complete_structure.json`: Hierarchical data of researchers, their roles, and IDs.
    - `listic_projets.complete_structure.json`: Metadata for research projects (dates, partners, acronyms).
- **Volume Mapping**: These files are mounted into the Docker containers at runtime, allowing hot-reloading of data without rebuilding containers.

## ✨ Key Features Breakdown

### 📊 Global Dashboard
- **Algorithm**: Aggregates total counts from the storage layer and performs real-time date filtering.
- **Visualization**: Stacked area charts for temporal evolution and bar charts for categorical distribution (Type, Venue).
- **Export**: Generates client-side CSV blobs for instant reporting.

### 🧑‍🔬 Researcher Analysis
- **Dual-Source Fetching**: The system queries both HAL and DBLP in parallel.
- **Fuzzy Matching**: Uses intelligent string matching to deduplicate venues and journals across different data sources.
- **Network Generation**: dynamicaly constructs a graph node-link structure based on co-authorship frequency found in publication metadata.

### 🚀 Project Intelligence
- **Acronym Linking**: Automatically associates publications to projects by scanning full-text metadata for the project's unique acronym.
- **Orphan Detection**: Identifies projects with no detectable scientific output in open repositories.

## � Project Structure

```bash
LISTIC/
├── backend/                # FastAPI Application
│   ├── services/           # External API connectors (HAL, DBLP)
│   ├── main.py             # Entry point & API Routes
│   ├── Dockerfile          # Python environment setup
│   └── docker-compose.yml  # Orchestration config
├── frontend/               # React Application
│   ├── src/
│   │   ├── components/     # Reusable UI widgets (Export, Navbar)
│   │   ├── pages/          # Main views (Dashboard, Network, Projects)
│   │   └── lib/            # Utilities (API client, Formatters)
│   └── Dockerfile          # Node build & Nginx setup
└── listic-database/        # Static Data Store
    ├── listic personnes/   # Researcher metadata
    └── listic_projet/      # Project metadata
```
