import json
import re
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

def normalize_text(text: str) -> str:
    """
    Normalize text for consistent searching
    """
    if not text:
        return ""
    return text.lower().strip()

def search_raw_ownership_data(query: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Fully optimized search algorithm - FIXED VERSION
    """
    if not query or not data:
        return []
    
    lower_case_query = normalize_text(query)
    query_tokens = lower_case_query.split()
    
    # Pre-compile all regex patterns once
    phrase_regex = re.compile(f"\\b{re.escape(lower_case_query)}\\b", re.IGNORECASE)
    token_regexes = [re.compile(f"\\b{re.escape(token)}\\b", re.IGNORECASE) for token in query_tokens]
    
    results = []
    processed_count = 0
    
    for entry in data:
        processed_count += 1
        
        # Quick rejection filter
        owner = normalize_text(entry.get("owner", ""))
        pidn = normalize_text(entry.get("pidn", ""))
        
        owner_has_token = any(token in owner for token in query_tokens)
        pidn_has_token = any(token in pidn for token in query_tokens)
        
        if not owner_has_token and not pidn_has_token:
            mailing = normalize_text(entry.get("mailing_address", ""))
            if not any(token in mailing for token in query_tokens):
                continue
        
        # Full field search
        fields_to_search = [
            owner,
            pidn,
            normalize_text(entry.get("tax_info", "")),
            normalize_text(entry.get("physical_address", "")),
            normalize_text(entry.get("mailing_address", "")),
        ]
        
        score = 0
        
        # Scoring logic
        if any(phrase_regex.search(field) for field in fields_to_search):
            score = 600
        elif all(
            any(token_regex.search(field) for field in fields_to_search)
            for token_regex in token_regexes
        ):
            score = 350
        elif any(
            any(token_regex.search(field) for field in fields_to_search)
            for token_regex in token_regexes
        ):
            score = 200
        elif any(
            any(token in field for field in fields_to_search)
            for token in query_tokens
        ):
            score = 110
        
        # Boost scores
        if fields_to_search[0]:
            score += 100
        if fields_to_search[3]:
            score += 50
        
        # FIXED: Store the original entry, not just score info
        if score >= 300:
            results.append({
                "entry": entry,  # Store the original entry data
                "score": score
            })
            
            if len(results) >= 200 and score >= 500:
                logger.info(f"🚀 Early exit after processing {processed_count} entries")
                break
    
    logger.info(f"🔍 Processed {processed_count} out of {len(data)} total entries")
    
    # FIXED: Sort and return the actual entry data
    results.sort(key=lambda x: x["score"], reverse=True)
    return [result["entry"] for result in results[:200]]  # Return the actual entries

class SearchEngine:
    """
    Search engine for ownership data
    """
    
    def __init__(self, search_index_path: str = None):
        if search_index_path is None:
            # Use absolute path to avoid working directory issues
            from pathlib import Path
            search_api_dir = Path(__file__).parent
            search_index_path = search_api_dir / "search_index.json"
        self.search_index_path = str(search_index_path)  # Convert Path to string
        self.search_data = None
        self._load_search_data()
    
    def _load_search_data(self):
        """
        Load the search index data
        """
        try:
            with open(self.search_index_path, 'r') as f:
                self.search_data = json.load(f)
            logger.info(f"✅ Loaded search index with {len(self.search_data)} entries")
        except FileNotFoundError:
            logger.error(f"❌ Search index file not found: {self.search_index_path}")
            self.search_data = []
        except Exception as e:
            logger.error(f"❌ Error loading search index: {e}")
            self.search_data = []
    
    def reload_search_data(self):
        """
        Reload the search index data (useful after updates)
        """
        self._load_search_data()
    
    def search(self, query: str, county_filter: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Search for ownership records with debug logging and optional county filtering
        """
        if not self.search_data:
            logger.warning("⚠️  No search data loaded")
            return []
        
        if not query or not query.strip():
            logger.warning("⚠️  Empty query")
            return []
        
        # Apply county filter if provided
        search_data = self.search_data
        if county_filter:
            original_count = len(search_data)
            search_data = [entry for entry in search_data if entry.get("county") in county_filter]
            logger.info(f"🏛️  Filtered to {len(search_data)} entries from {original_count} (counties: {county_filter})")
        
        logger.info(f"🔍 Searching for: '{query}' in {len(search_data)} entries")
        
        # Debug: show first entry structure
        if search_data:
            logger.info(f"🔍 Sample entry keys: {list(search_data[0].keys())}")
            sample_owner = search_data[0].get("owner", "")
            logger.info(f"🔍 Sample owner field: '{sample_owner}'")
        
        results = search_raw_ownership_data(query, search_data)
        logger.info(f"📊 Found {len(results)} results")
        
        return results
    
    def get_search_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the search index
        
        Returns:
            Dictionary with search index statistics
        """
        if not self.search_data:
            return {"total_entries": 0, "counties": []}
        
        counties = {}
        for entry in self.search_data:
            county = entry.get("county", "unknown")
            counties[county] = counties.get(county, 0) + 1
        
        return {
            "total_entries": len(self.search_data),
            "counties": counties
        } 