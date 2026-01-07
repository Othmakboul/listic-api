from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from typing import List, Optional
from services.hal import get_hal_stats, get_project_stats
from services.dblp import get_dblp_stats

app = FastAPI(title="LISTIC Dashboard API")

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load data
DATA_PATH = "/home/skudo/Desktop/LISTIC/listic-database/listic personnes/listic_personnes.complete_structure.json"
DATA_PATH_PROJECTS = "/home/skudo/Desktop/LISTIC/listic-database/listic_projet/listic_projets.complete_structure.json"

local_data = {}
local_projects = {}

def load_data():
    global local_data, local_projects
    
    # Load Researchers
    if os.path.exists(DATA_PATH):
        try:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
                all_persons = []
                root = raw_data[0] if isinstance(raw_data, list) and len(raw_data) > 0 else raw_data
                data_content = root.get("data", {})
                
                for category, persons in data_content.items():
                    if isinstance(persons, list):
                        for p in persons:
                            if isinstance(p, dict):
                                p["category"] = category
                                if "_unique_id" not in p:
                                    p["_unique_id"] = p.get("name")
                                all_persons.append(p)
                                
                local_data = {p["_unique_id"]: p for p in all_persons}
                print(f"Loaded {len(local_data)} researchers.")
        except Exception as e:
            print(f"Error loading researchers: {e}")

    # Load Projects
    if os.path.exists(DATA_PATH_PROJECTS):
        try:
            with open(DATA_PATH_PROJECTS, "r", encoding="utf-8") as f:
                raw_projects = json.load(f)
                root_proj = raw_projects[0] if isinstance(raw_projects, list) and len(raw_projects) > 0 else raw_projects
                data_content_proj = root_proj.get("data", {})
                
                # Flatten projects structure
                # Structure is Category -> [List of Projects]
                all_projects_list = []
                for cat, projs in data_content_proj.items():
                    if isinstance(projs, list):
                        for p in projs:
                            if isinstance(p, dict):
                                p["type"] = cat # e.g. Nationaux, Internationaux
                                all_projects_list.append(p)
                
                local_projects = {p.get("_unique_id", p.get("NOM")): p for p in all_projects_list}
                print(f"Loaded {len(local_projects)} projects.")
        except Exception as e:
            print(f"Error loading projects: {e}")

@app.on_event("startup")
async def startup_event():
    load_data()

@app.get("/")
def read_root():
    return {"message": "LISTIC Dashboard API is running"}

@app.get("/researchers")
def get_researchers(category: Optional[str] = None):
    """
    Get list of researchers. Optional filter by category.
    """
    res = list(local_data.values())
    if category:
        res = [r for r in res if r.get("category") == category]
    return res

@app.get("/projects")
def get_projects():
    """
    Get list of global LISTIC projects.
    """
    return list(local_projects.values())

@app.get("/project/{uid}")
async def get_project_details(uid: str):
    """
    Get detailed info for a project + external stats (HAL).
    """
    # 1. Search in local_projects (keyed by ID or Name)
    proj = local_projects.get(uid)
    if not proj:
        # Fallback: search by values if uid is actually a name
        for p in local_projects.values():
            if p.get("NOM") == uid:
                proj = p
                break
    
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found")
    
    # 2. Extract name/acronym
    name = proj.get("NOM")
    
    # 3. Fetch external stats (HAL only for now, DBLP less relevant for proj acronyms usually)
    hal_stats = await get_project_stats(name)
    
    return {
        "profile": proj,
        "stats": {
            "hal": hal_stats
        }
    }

@app.get("/researcher/{uid}")
async def get_researcher_details(uid: str):
    """
    Get detailed info + HAL/DBLP stats.
    """
    person = local_data.get(uid)
    if not person:
        raise HTTPException(status_code=404, detail="Researcher not found")
    
    name = person.get("name")
    
    # Fetch external data in parallel
    hal_data = await get_hal_stats(name)
    dblp_data = await get_dblp_stats(name)
    
    return {
        "profile": person,
        "stats": {
            "hal": hal_data,
            "dblp": dblp_data
        }
    }
