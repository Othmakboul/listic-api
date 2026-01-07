# LISTIC Intelligence Dashboard

## 🌍 Overview
The **LISTIC Intelligence Dashboard** is a comprehensive web visualization platform designed for the **Laboratory of Informatics, Systems, Information Processing and Knowledge (LISTIC)**. It aggregates, analyzes, and visualizes academic data including researcher profiles, publication statistics, project portfolios, and collaboration networks.

This project aims to provide a clear window into the laboratory's scientific output, enabling researchers and administrators to track performance, identify collaboration opportunities, and export data for reporting.

![Dashboard Preview](https://via.placeholder.com/1200x600?text=LISTIC+Dashboard+Preview)

## ✨ Key Features

### 📊 Global Dashboard
- **Laboratory Statistics**: Real-time overview of total publications, active researchers, and ongoing projects.
- **Temporal Analysis**: Interactive timelines showing publication trends over years.
- **Top Metrics**: Visual breakdowns of top research keywords, publication venues (journals/conferences), and document types.

### 🧑‍🔬 Researcher Profiles
- **Individual Analytics**: Dedicated pages for each researcher with detailed metrics.
- **Multi-Source Data**: Aggregates data from **HAL** and **DBLP** APIs to ensure comprehensive coverage.
- **Collaboration Network**: view top co-authors and collaborative structures.
- **Recent Activity**: List of latest publications with links to full papers.

### 🚀 Projects Hub
- **Project Portfolio**: Searchable list of all LISTIC projects (ANR, European, Industrial, etc.).
- **Impact Tracking**: Automatic detection of publications associated with specific projects via acronym matching.
- **Partners & Funding**: Detailed views of project partners, funding sources, and durations.

### 🕸️ Network Graph
- **Interactive Graph**: A force-directed graph visualizing connections between researchers within the lab.
- **Cluster Analysis**: Visually identify research groups and regular collaborators.

### 💾 Data Export
- **Export Widget**: Download any dataset from the dashboards.
- **Formats**: Support for full **JSON** datasets and formatted **CSV** tables for easy integration with Excel or other tools.

## 🛠️ Technical Architecture

The project follows a modern microservices architecture:

### Frontend (`/frontend`)
- **Framework**: React 18 with Vite
- **Styling**: Tailwind CSS for a modern, responsive design.
- **Visualization**: Recharts for statistical charts, React Force Graph for network visualizations.
- **Animations**: Framer Motion for smooth UI transitions.

### Backend (`/backend`)
- **API**: Python FastAPI (High performance, async support).
- **Services**: dedicated modules for fetching and processing data from HAL (`services/hal.py`) and DBLP (`services/dblp.py`).
- **Data Handling**: Integrates with local JSON datasets and real-time external APIs.

### Data (`/listic-database`)
- **Source of Truth**: Structured JSON files containing researcher lists, project metadata, and partner information.
- **Versioning**: Data is version-controlled to ensure stability.

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose (Recommended)
- Node.js 18+ & Python 3.9+ (For local development)

### Quick Start (Docker)
The easiest way to run the full stack is using Docker Compose from the root directory (or backend folder where `docker-compose.yml` resides).

```bash
cd backend
docker compose up --build
```
This will start:
- **Frontend** on `http://localhost:5173`
- **Backend API** on `http://localhost:8000`

### Local Development

**1. Backend Setup**
```bash
cd backend
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
python main.py
```

**2. Frontend Setup**
```bash
cd frontend
npm install
npm run dev
```

## 👥 Contributors
- **Skudo / Othmakboul** - Lead Developer & Architecture

## 📄 License
Internal LISTIC Project.
