import httpx
from collections import Counter

DBLP_API_URL = "https://dblp.org/search/publ/api"

async def get_dblp_stats(name: str):
    """
    Fetches statistics for a researcher from DBLP API.
    """
    clean_name = " ".join(name.split())
    
    params = {
        "q": clean_name,
        "format": "json",
        "h": 500 # Max results
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(DBLP_API_URL, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            
            hits = data.get("result", {}).get("hits", {}).get("hit", [])
            
            if not hits:
                return {"found": False, "source": "DBLP", "count": 0}

            # Process Stats
            years = []
            types = []
            venue_counts = []
            co_authors = []
            
            cleaned_hits = []
            
            # Helper to safely hashable
            def safe_value(v):
                if isinstance(v, list): return tuple(v)
                return v

            researcher_name_lower = "".join(name.split()).lower() # DBLP author names are often First Last, but let's just crude compare parts if needed, or better, exclude exact match if possible.
            # actually DBLP returns "author" as list of dicts or strings.
    
            for hit in hits:
                info = hit.get("info", {})
                
                # Check if author name matches approximately (DBLP search is broad)
                authors_list = info.get("authors", {}).get("author", [])
                if isinstance(authors_list, str): authors_list = [authors_list]
                elif isinstance(authors_list, dict): authors_list = [authors_list.get("text", "")] # Sometimes complex object
                
                # Co-authors
                for auth in authors_list:
                    # Very basic exclusion of self. DBLP names might vary slightly.
                    # Normalize simple check
                    if auth and isinstance(auth, str):
                        if "".join(auth.split()).lower() != researcher_name_lower:
                            co_authors.append(auth)

                year = info.get("year")
                if year: years.append(int(year))
                
                type_ = info.get("type")
                if type_: types.append(safe_value(type_))
                
                venue = info.get("venue")
                if venue: 
                    if isinstance(venue, list):
                        venue_counts.extend(venue)
                    else:
                        venue_counts.append(venue)
                
                cleaned_hits.append({
                    "title": info.get("title"),
                    "year": year,
                    "venue": venue if not isinstance(venue, list) else venue[0], # Just take first if list
                    "type": type_ if not isinstance(type_, list) else type_[0],
                    "url": info.get("url")
                })
            
            years_dist = dict(Counter(years))
            types_dist = dict(Counter([str(t) for t in types])) # Force string for JSON
            venues_top = dict(Counter(venue_counts).most_common(10))
            collaborators_top = dict(Counter(co_authors).most_common(10))
            
            # Sort cleaned hits by year desc
            cleaned_hits.sort(key=lambda x: int(x.get("year", 0)) if x.get("year") else 0, reverse=True)

            return {
                "found": True,
                "source": "DBLP",
                "total_publications": len(hits),
                "years_distribution": years_dist,
                "types_distribution": types_dist,
                "top_venues": venues_top,
                "top_collaborators": collaborators_top,
                "recent_publications": cleaned_hits[:5]
            }
            
        except Exception as e:
            print(f"Error fetching DBLP data for {name}: {e}")
            return {"error": str(e), "source": "DBLP"}
