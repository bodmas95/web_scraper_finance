import json
import os
from openai import OpenAI
from pathlib import Path
from datetime import datetime
import sys
import re
from typing import Dict, List, Tuple, Any
from bpce_api_setup.call_bpce_llm import call_llm_api


# Create client pointing to your vLLM base URL
client = OpenAI(
    api_key="dummy",  # vLLM usually ignores this, but the client requires something
    # base_url="http://sldahkpdpas04.hk.intranet:8000/v1",
    base_url="http://sldahkpdpas04.hk.intranet:8000/v1/chat/completions",
)
 

def call_llm(query:str):

    # Call chat completion
    response = client.chat.completions.create(
    model="gemma3-27b-it/",
    # model = "DeepSeek-R1-7B",
    temperature=0,
    top_p=1,
    presence_penalty=0.1,
    frequency_penalty=0.1,
    messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": query},
        ],
    )
    print("======== API Response : 200 =========\n")
    return response.choices[0].message.content
       
# # Test code 
# print(call_llm("what is your name"))    

def save_report_to_file(report_content: str, report_name: str, client_name: str) -> Path:
    """Save report content to a markdown file."""
    reports_folder = create_reports_folder(client_name)
    safe_client_name = client_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    safe_report_name = report_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    filename = f"{safe_client_name}_{safe_report_name}.md"
    file_path = reports_folder / filename
    
    full_content = f"# {client_name} - {report_name}\n\n{report_content}"
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(full_content)
    
    return file_path


def split_corpus_into_chunks(corpus, chunk_size=50000, overlap=10000):
    """Split corpus into overlapping chunks"""
    words = corpus.split()
    chunks = []
    
    if len(words) <= chunk_size:
        return [corpus]  # No need to split
    
    start = 0
    chunk_num = 1
    
    while start < len(words):
        # Calculate end position for current chunk
        end = start + chunk_size
        
        # Get chunk words
        if end >= len(words):
            # Last chunk - take all remaining words
            chunk_words = words[start:]
        else:
            chunk_words = words[start:end]
        
        # Join words back to text
        chunk_text = ' '.join(chunk_words)
        chunks.append((chunk_text, chunk_num))
        
        # If this was the last chunk, break
        if end >= len(words):
            break
            
        # Calculate start position for next chunk (with overlap)
        start = end - overlap
        chunk_num += 1
    
    return chunks

def save_report_to_file_with_chunk(report_content, report_name, client, chunk_num=None):
    """Modified save function to handle chunk numbering"""
    if chunk_num is not None:
        # Add chunk number to filename
        base_name, ext = os.path.splitext(report_name)
        filename = f"{base_name}_chunk_{chunk_num}{ext}"
    else:
        filename = report_name
    
    # Your existing save logic here, but use 'filename' instead of 'report_name'
    # Return the file path
    file_path = save_report_to_file(report_content, filename, client)
    return file_path

def create_reports_folder(client):
    """Create reports folder if it doesn't exist."""
    reports_folder = Path(f"reports_{client}_{str(datetime.now().strftime("%d%b"))}")
    reports_folder.mkdir(exist_ok=True)
    return reports_folder



def reorganize_citations_for_display(report_content: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    """
    Reorganize report content to separate citations from analysis.
    
    This function extracts all citations from each section and returns:
    1. Report content without citations (for main display)
    2. Citations dictionary grouped by section (for display at the end)
    
    Args:
        report_content: The report dictionary containing sections with Analysis, Citation, and source_table
        
    Returns:
        Tuple of (report_without_citations, citations_by_section)
        - report_without_citations: Dict with same structure but Citation fields removed
        - citations_by_section: Dict mapping section names to their citation lists
    """
    report_without_citations = {}
    citations_by_section = {}
    
    for section_name, section_data in report_content.items():
        # Skip non-section fields like 'error', 'raw_response', etc.
        if section_name in ['error', 'raw_response']:
            report_without_citations[section_name] = section_data
            continue
        
        # Handle list-based section data (most common format)
        if isinstance(section_data, list) and len(section_data) > 0:
            item = section_data[0]
            
            # Extract citations if present
            if 'Citation' in item and item['Citation']:
                citations_by_section[section_name] = item['Citation']
            
            # Create new item without Citation field
            new_item = {}
            for key, value in item.items():
                if key != 'Citation':
                    new_item[key] = value
            
            report_without_citations[section_name] = [new_item]
        
        # Handle dict-based section data
        elif isinstance(section_data, dict):
            # Extract citations if present
            if 'Citation' in section_data and section_data['Citation']:
                citations_by_section[section_name] = section_data['Citation']
            
            # Create new dict without Citation field
            new_section_data = {}
            for key, value in section_data.items():
                if key != 'Citation':
                    new_section_data[key] = value
            
            report_without_citations[section_name] = new_section_data
        
        # Handle string or other types (pass through as-is)
        else:
            report_without_citations[section_name] = section_data
    
    return report_without_citations, citations_by_section


def escape_dollar_for_markdown(text: str) -> str:
    """
    Escape dollar signs in text to prevent Markdown LaTeX interpretation.
    
    This function replaces '$' with '\$' to prevent Streamlit's markdown renderer
    from treating dollar signs as LaTeX math delimiters.
    
    Args:
        text: The text string containing dollar signs
        
    Returns:
        Text with dollar signs escaped for markdown display
    """
    if not isinstance(text, str):
        return text
    pattern = r'(?<!\\)\$(?=\s?\d|\s?\b)'
    
    return re.sub(pattern, r'\$', text)


def render_citations_section(citations_by_section: Dict[str, List[str]]) -> str:
    """
    Render all citations grouped by section as markdown.
    
    Args:
        citations_by_section: Dictionary mapping section names to their citation lists
        
    Returns:
        Markdown string with all citations formatted by section
    """
    if not citations_by_section:
        return ""
    
    markdown_lines = []
    markdown_lines.append("## 📚 Citations")
    markdown_lines.append("")
    markdown_lines.append("---")
    markdown_lines.append("")
    
    for section_name, citations in citations_by_section.items():
        if citations:  # Only show sections that have citations
            markdown_lines.append(f"### Citations ({section_name})")
            for citation in citations:
                markdown_lines.append(f"• {citation}")
            markdown_lines.append("")
    
    return "\n".join(markdown_lines)


# #---------------------------------------------------------
# # Testing of apis

# # test = (call_llm_api(query = "Who are you ? tell me your name in 1 line max",model_name="gpt-5-mini-2025-08-07", temperature=0.3, max_tokens=30))
# test = call_llm(query = "Who are you ? tell me your name in 1 line max")

# print(type(test))
# print(test)
# print(f"\n\n\n{test.content}")

