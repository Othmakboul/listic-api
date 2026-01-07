import httpx
import urllib.parse
from collections import Counter

HAL_API_URL = "https://api.archives-ouvertes.fr/search/"

async def get_hal_stats(name: str):
    """
    Fetches statistics for a researcher from HAL API.
    """
    # Clean name for query (remove extra spaces)
    clean_name = " ".join(name.split())
    # Query: Search by author name strictly if possible, or text otherwise.
    # authFullName_t is a good field for full name text search.
    query = f'authFullName_t:"{clean_name}"'
    
    # Fields we want to retrieve to build stats
    fl = "title_s,producedDateY_i,docType_s,keyword_s,authFullName_s,journalTitle_s"
    
    params = {
        "q": query,
        "wt": "json",
        "fl": fl,
        "rows": 500, # Max rows to analyze
        "sort": "producedDateY_i desc"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(HAL_API_URL, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            
            if not docs:
                return {"found": False, "source": "HAL", "count": 0}

            # Process Stats
            years = []
            types = []
            keywords = []
            co_authors = []
            journals = []
            
            researcher_name_lower = name.lower()

            for d in docs:
                # Years
                if d.get("producedDateY_i"):
                    years.append(d.get("producedDateY_i"))
                
                # Types
                if d.get("docType_s"):
                    types.append(d.get("docType_s"))
                
                # Keywords
                if d.get("keyword_s"):
                    if isinstance(d.get("keyword_s"), list):
                        keywords.extend(d.get("keyword_s"))
                    else:
                        keywords.append(d.get("keyword_s"))

                # Co-authors (exclude self)
                if d.get("authFullName_s"):
                    authors = d.get("authFullName_s")
                    if isinstance(authors, str): authors = [authors]
                    for auth in authors:
                        if auth.lower() != researcher_name_lower:
                            co_authors.append(auth)
                            
                # Journals
                if d.get("journalTitle_s"):
                    journals.append(d.get("journalTitle_s"))
            
            # Count aggregations
            years_dist = dict(Counter(years))
            types_dist = dict(Counter(types))
            
            # Top Lists
            keywords_top = dict(Counter(keywords).most_common(20))
            collaborators_top = dict(Counter(co_authors).most_common(10))
            journals_top = dict(Counter(journals).most_common(10))
            
            return {
                "found": True,
                "source": "HAL",
                "total_publications": len(docs),
                "years_distribution": years_dist,
                "types_distribution": types_dist,
                "top_keywords": keywords_top,
                "top_collaborators": collaborators_top,
                "top_journals": journals_top,
                "recent_publications": docs[:5] # Top 5 recent
            }
            
        except Exception as e:
            print(f"Error fetching HAL data for {name}: {e}")
            return {"error": str(e), "source": "HAL"}

async def get_project_stats(project_name: str):
    """
    Fetches statistics for a project from HAL API by searching its acronym/name.
    """
    # Search in all text fields for the project acronym. 
    # Ideally checking specific fields like 'funding_s' or 'collaboration_s' is better but inconsistent.
    # We will use a general search for the acronym.
    query = f'"{project_name}"'
    
    fl = "title_s,producedDateY_i,docType_s,authFullName_s,journalTitle_s"
    
    params = {
        "q": query,
        "wt": "json",
        "fl": fl,
        "rows": 100,
        "sort": "producedDateY_i desc"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(HAL_API_URL, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            
            if not docs:
                return {"found": False, "count": 0}

            # Stats
            years = [d.get("producedDateY_i") for d in docs if d.get("producedDateY_i")]
            authors = []
            for d in docs:
                if d.get("authFullName_s"):
                    a = d.get("authFullName_s")
                    if isinstance(a, list): authors.extend(a)
                    else: authors.append(a)
            
            years_dist = dict(Counter(years))
            top_authors = dict(Counter(authors).most_common(10))
            
            return {
                "found": True,
                "total_publications": len(docs),
                "years_distribution": years_dist,
                "top_authors": top_authors,
                "recent_publications": docs[:5]
            }

        except Exception as e:
            print(f"Error fetching HAL project data for {project_name}: {e}")
            return {"error": str(e)}
