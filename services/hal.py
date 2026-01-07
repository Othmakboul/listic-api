import httpx
import urllib.parse
from collections import Counter
from typing import Optional

HAL_API_URL = "https://api.archives-ouvertes.fr/search/"

async def get_hal_stats(name: str, start_year: Optional[int] = None, end_year: Optional[int] = None, keyword: Optional[str] = None):
    """
    Fetches statistics for a researcher from HAL API.
    """
    # Clean name for query (remove extra spaces)
    clean_name = " ".join(name.split())
    # Query: Search by author name strictly if possible, or text otherwise.
    # authFullName_t is a good field for full name text search.
    query = f'authFullName_t:"{clean_name}"'
    
    # Fields we want to retrieve to build stats
    fl = "title_s,producedDateY_i,docType_s,keyword_s,authFullName_s,journalTitle_s,conferenceTitle_s"
    
    params = {
        "q": query,
        "wt": "json",
        "fl": fl,
        "rows": 500, # Max rows to analyze
        "sort": "producedDateY_i desc"
    }

    # Filters
    filters = []
    if start_year or end_year:
        s = start_year if start_year else "*"
        e = end_year if end_year else "*"
        filters.append(f"producedDateY_i:[{s} TO {e}]")
    
    if keyword:
        # Quote the keyword to handle spaces, and escape existing quotes if any
        safe_keyword = keyword.replace('"', '\\"')
        filters.append(f'keyword_s:"{safe_keyword}"')
        
        # Combine filters into a single string with AND
        params["fq"] = " AND ".join(filters)
    
    print(f"DEBUG HAL REQUEST: {HAL_API_URL} with params {params}")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(HAL_API_URL, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            
            # --- Manual Fallback Filtering ---
            # Ensure strict compliance with filters even if API is loose
            filtered_docs = []
            for d in docs:
                # Year Filter
                y = d.get("producedDateY_i")
                if start_year and y and y < start_year: continue
                if end_year and y and y > end_year: continue
                
                # Keyword Filter (Case insensitive partial match for robustness)
                if keyword:
                    kws = d.get("keyword_s", [])
                    if isinstance(kws, str): kws = [kws]
                    # Check if any keyword contains the search term
                    if not any(keyword.lower() in k.lower() for k in kws):
                        continue
                
                filtered_docs.append(d)
                
            docs = filtered_docs
            # ---------------------------------

            if not docs:
                return {"found": True, "source": "HAL", "count": 0, "total_publications": 0, "years_distribution": {}, "types_distribution": {}, "top_keywords": {}, "top_collaborators": {}, "top_journals": {}, "recent_publications": []}

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
async def get_listic_stats(start_year: Optional[int] = None, end_year: Optional[int] = None):
    """
    Fetches global statistics for the LISTIC lab using Facets.
    Supports optional year filtering.
    """
    query = 'structAcronym_s:"LISTIC"'
    
    # We use rows=0 because we only care about facets (counts), not the documents themselves.
    params = {
        "q": query,
        "wt": "json",
        "rows": 0,
        "facet": "true",
        "facet.field": [
            "producedDateY_i",
            "keyword_s",
            "docType_s",
            "authFullName_s",
            "journalTitle_s",
            "language_s",
            "structName_s"
        ],
        "facet.limit": 50, # Get top 50
        "facet.mincount": 1
    }
    
    # Add Filter Query for date range if provided
    if start_year or end_year:
        # Default boundary if one side missing
        s = start_year if start_year else "*"
        e = end_year if end_year else "*"
        params["fq"] = f"producedDateY_i:[{s} TO {e}]"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(HAL_API_URL, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            
            facet_counts = data.get("facet_counts", {}).get("facet_fields", {})
            
            # Helper to convert ["2023", 10, "2022", 5] list to [{"name": "2023", "value": 10}, ...]
            def parse_facet(flat_list):
                res = []
                for i in range(0, len(flat_list), 2):
                    res.append({
                        "name": str(flat_list[i]),
                        "value": flat_list[i+1]
                    })
                return res

            years_data = parse_facet(facet_counts.get("producedDateY_i", []))
            keywords_data = parse_facet(facet_counts.get("keyword_s", []))
            types_data = parse_facet(facet_counts.get("docType_s", []))
            authors_data = parse_facet(facet_counts.get("authFullName_s", []))
            journals_data = parse_facet(facet_counts.get("journalTitle_s", []))
            languages_data = parse_facet(facet_counts.get("language_s", []))
            structures_data = parse_facet(facet_counts.get("structName_s", []))
            
            # Post-process structures to exclude "LISTIC" itself from collaborators list
            structures_data = [s for s in structures_data if "LISTIC" not in s["name"].upper() and "LABORATOIRE D'INFORMATIQUE" not in s["name"].upper()]

            # Sort years numerically
            years_data.sort(key=lambda x: int(x["name"]) if x["name"].isdigit() else 0)
            
            return {
                "years": years_data,
                "keywords": keywords_data,
                "types": types_data,
                "authors": authors_data,
                "journals": journals_data,
                "languages": languages_data,
                "structures": structures_data,
                "total_docs": data.get("response", {}).get("numFound", 0)
            }
            
        except Exception as e:
            print(f"Error fetching LISTIC global stats: {e}")
            return {"error": str(e)}
