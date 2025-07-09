import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import json
import base64
import io
import re
from typing import Dict, List, Optional, Any
import time
from dataclasses import dataclass
import hashlib
import warnings
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import plotly.io as pio
import numpy as np
import ast
from docx import Document
import openai
import re


# Suppress SSL warnings
warnings.filterwarnings('ignore', message='urllib3 v2 only supports OpenSSL')

# Handle additional imports for PDF with fallback
try:
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    st.error("ReportLab not installed. Run: pip install reportlab")

# Handle docx import with fallback
try:
    
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False
    st.error("python-docx not properly installed. Run: pip uninstall docx && pip install python-docx")

# Handle OpenAI import with fallback
try:
    
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="AI Jira Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern design
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(145deg, #f0f2f6, #ffffff);
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(255, 255, 255, 0.2);
        margin: 1rem 0;
    }
    
    .ai-response {
        background: linear-gradient(145deg, #e8f4fd, #ffffff);
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 4px solid #667eea;
        margin: 1rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }
</style>
""", unsafe_allow_html=True)

@dataclass
class JiraConfig:
    url: str
    username: str
    api_token: str
    projects: List[str]

class JiraAPI:
    def __init__(self, config: JiraConfig):
        self.config = config
        self.auth = HTTPBasicAuth(config.username, config.api_token)
        self.base_url = config.url.rstrip('/')
        
    def test_connection(self) -> tuple[bool, str]:
        """Test Jira connection"""
        try:
            url = f"{self.base_url}/rest/api/3/myself"
            response = requests.get(
                url,
                auth=self.auth,
                headers={"Accept": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                return True, "Connection successful"
            elif response.status_code == 401:
                return False, "Authentication failed - check username/API token"
            elif response.status_code == 403:
                return False, "Access forbidden - check permissions"
            elif response.status_code == 404:
                return False, "URL not found - check Jira URL format"
            else:
                return False, f"HTTP {response.status_code}: {response.text[:100]}"
                
        except requests.exceptions.ConnectTimeout:
            return False, "Connection timeout - check URL and network"
        except requests.exceptions.ConnectionError:
            return False, "Connection error - check URL format"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def get_projects(self) -> List[Dict]:
        """Get available projects"""
        try:
            response = requests.get(
                f"{self.base_url}/rest/api/3/project",
                auth=self.auth,
                headers={"Accept": "application/json"}
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception:
            return []
    
    def get_issues(self, project_keys: List[str], start_date: str, end_date: str) -> List[Dict]:
        """Get issues for specified projects within date range"""
        issues = []
        
        for project_key in project_keys:
            jql = f"project = {project_key} AND created >= '2025-01-01'"
            #AND created >= '{start_date}' AND created <= '{end_date}
            start_at = 0
            max_results = 100
            
            while True:
                try:
                    response = requests.get(
                        f"{self.base_url}/rest/api/3/search",
                        auth=self.auth,
                        headers={"Accept": "application/json"},
                        params={
                            "jql": jql,
                            "startAt": start_at,
                            "maxResults": max_results,
                            "fields": "key,summary,status,assignee,reporter,created,updated,resolutiondate,priority,issuetype,timetracking,description,labels,components,fixVersions,sprint"
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        issues.extend(data.get('issues', []))
                        
                        if len(data.get('issues', [])) < max_results:
                            break
                        start_at += max_results
                    else:
                        break
                        
                except Exception as e:
                    st.error(f"Error fetching issues for {project_key}: {str(e)}")
                    break
                    
        return issues
    
    def get_issues_with_expanded_fields(self, project_keys: List[str], start_date: str, end_date: str) -> List[Dict]:
        """Get issues with expanded fields including parent/epic information"""
        issues = []
        
        for project_key in project_keys:
            jql = f"project = {project_key}"
            #AND duedate >= '{start_date}' AND duedate <= '{end_date}'
            start_at = 0
            max_results = 100
            
            while True:
                try:
                    # Expand fields to include all custom fields and parent/epic
                    response = requests.get(
                        f"{self.base_url}/rest/api/3/search",
                        auth=self.auth,
                        headers={"Accept": "application/json"},
                        params={
                            "jql": jql,
                            "startAt": start_at,
                            "maxResults": max_results,
                            "expand": "names,schema",
                            "fields": "*all"  # Get all fields including custom fields
                        }
                    )
                    if response.status_code == 200:
                        data = response.json()
                        issues.extend(data.get('issues', []))
                        
                        if len(data.get('issues', [])) < max_results:
                            break
                        start_at += max_results
                    else:
                        break
                        
                except Exception as e:
                    st.error(f"Error fetching issues for {project_key}: {str(e)}")
                    break
                    
        return issues
    
    def get_worklogs(self, issue_keys: List[str]) -> List[Dict]:
        """Get worklog data for specified issues"""
        worklogs = []
        
        # Process in batches to avoid overwhelming the API
        batch_size = 100
        for i in range(0, len(issue_keys), batch_size):
            batch = issue_keys[i:i + batch_size]
            
            for issue_key in batch:
                try:
                    response = requests.get(
                        f"{self.base_url}/rest/api/3/issue/{issue_key}/worklog",
                        auth=self.auth,
                        headers={"Accept": "application/json"}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        for worklog in data.get('worklogs', []):
                            worklog['issue_key'] = issue_key
                            worklogs.append(worklog)
                            
                except Exception as e:
                    # Continue processing other issues even if one fails
                    pass
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
        return worklogs

class DocumentParser:
    @staticmethod
    def parse_docx(file_content: bytes) -> str:
        """Parse Word document and extract text"""
        if not DOCX_AVAILABLE:
            st.error("Document parsing unavailable. Install python-docx: pip install python-docx")
            return ""
            
        try:
            doc = Document(io.BytesIO(file_content))
            text = []
            
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text.append(paragraph.text.strip())
                    
            return "\n".join(text)
        except Exception as e:
            st.error(f"Error parsing document: {str(e)}")
            return ""
    
    @staticmethod
    def extract_prompts(text: str) -> List[str]:
        """Extract reporting prompts from document text"""
        # Look for patterns that indicate prompts
        prompt_patterns = [
            r"(?i)generate\s+.*?report",
            r"(?i)create\s+.*?analysis",
            r"(?i)show\s+.*?metrics",
            r"(?i)analyze\s+.*",
            r"(?i)calculate\s+.*",
            r"(?i)display\s+.*",
        ]
        
        prompts = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if len(line) > 10:  # Minimum length for meaningful prompt
                for pattern in prompt_patterns:
                    if re.search(pattern, line):
                        prompts.append(line)
                        break
                # Also include lines with question marks
            elif '?' in line and len(line) > 20:
                    prompts.append(line)
                    
        return prompts

class EnhancedAIAgent:
    def __init__(self):
        self.similarity_threshold = 0.7
        self.stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should'}
    
    def generate_insights(self, data: Dict[str, Any], prompt: str = "") -> str:
        """Generate intelligent insights from Jira data with enhanced analysis"""
        return self._generate_enhanced_insights(data, prompt)
    
    def perform_root_cause_analysis(self, df: pd.DataFrame, target_issue: str = None) -> Dict[str, Any]:
        """Perform comprehensive root cause analysis on issues"""
        try:
            # If no specific issue provided, analyze patterns in failed/blocked issues
            if target_issue:
                issue_data = df[df['key'] == target_issue]
                if issue_data.empty:
                    return {"error": f"Issue {target_issue} not found"}
                analysis_df = issue_data
            else:
                # Analyze problematic issues (bugs, failed, blocked, etc.)
                problem_statuses = ['Failed', 'Blocked', 'Rejected', 'Cancelled', 'Bug Found', 'Error']
                problem_types = ['Bug', 'Defect', 'Issue', 'Problem', 'Error']
                
                analysis_df = df[
                    (df['status'].str.contains('|'.join(problem_statuses), case=False, na=False)) | 
                    (df['issuetype'].str.contains('|'.join(problem_types), case=False, na=False)) |
                    (df['priority'].isin(['High', 'Critical', 'Highest', 'Blocker']))
                ]
            
            if analysis_df.empty:
                return {"message": "No problematic issues found for root cause analysis"}
            
            # Perform comprehensive root cause analysis
            root_causes = self._identify_root_causes(analysis_df, df)
            similar_issues = self._find_similar_issues(analysis_df, df)
            resolution_patterns = self._analyze_resolution_patterns(similar_issues, df)
            common_patterns = self._extract_common_patterns(analysis_df)
            recommendations = self._generate_recommendations(root_causes, resolution_patterns, common_patterns)
            
            return {
                "total_analyzed": len(analysis_df),
                "root_causes": root_causes,
                "similar_issues": similar_issues,
                "resolution_patterns": resolution_patterns,
                "common_patterns": common_patterns,
                "recommendations": recommendations,
                "insights": self._generate_root_cause_insights(root_causes, similar_issues, resolution_patterns)
            }
            
        except Exception as e:
            return {"error": f"Root cause analysis failed: {str(e)}"}
    
    def find_similar_issues(self, df: pd.DataFrame, reference_issue: str) -> List[Dict]:
        """Find issues similar to a reference issue using advanced text analysis"""
        try:
            ref_issue = df[df['key'] == reference_issue]
            if ref_issue.empty:
                return []
            
            ref_summary = ref_issue.iloc[0]['summary']
            ref_description = ref_issue.iloc[0].get('description', '')
            ref_type = ref_issue.iloc[0]['issuetype']
            ref_priority = ref_issue.iloc[0].get('priority', 'Medium')
            
            # Calculate similarity scores
            similar_issues = []
            for idx, row in df.iterrows():
                if row['key'] == reference_issue:
                    continue
                
                # Calculate multi-factor similarity
                summary_similarity = self._calculate_similarity(ref_summary, row['summary'])
                
                # Description similarity (if available)
                desc_similarity = 0
                if ref_description and 'description' in row and row['description']:
                    desc_similarity = self._calculate_similarity(ref_description, row['description'])
                
                # Combined similarity score
                similarity_score = summary_similarity * 0.7 + desc_similarity * 0.3
                
                # Boost score for same issue type
                if row['issuetype'] == ref_type:
                    similarity_score += 0.15
                
                # Boost score for similar priority
                if row.get('priority') == ref_priority:
                    similarity_score += 0.1
                
                # Boost for similar components or labels
                if 'components' in ref_issue.columns and 'components' in row:
                    if ref_issue.iloc[0]['components'] == row['components']:
                        similarity_score += 0.05
                
                if similarity_score >= self.similarity_threshold:
                    resolution_info = self._get_resolution_info(row)
                    similar_issues.append({
                        'key': row['key'],
                        'summary': row['summary'],
                        'similarity_score': round(similarity_score, 3),
                        'status': row['status'],
                        'resolution_days': row.get('resolution_days', 'N/A'),
                        'resolution_info': resolution_info,
                        'assignee': row.get('assignee', 'Unassigned')
                    })
            
            return sorted(similar_issues, key=lambda x: x['similarity_score'], reverse=True)[:20]
            
        except Exception as e:
            return [{"error": f"Similarity analysis failed: {str(e)}"}]
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate enhanced text similarity using multiple techniques"""
        try:
            if not text1 or not text2:
                return 0.0
            
            # Normalize texts
            text1_lower = text1.lower()
            text2_lower = text2.lower()
            
            # Word-based similarity
            words1 = set(word for word in text1_lower.split() if word not in self.stop_words and len(word) > 2)
            words2 = set(word for word in text2_lower.split() if word not in self.stop_words and len(word) > 2)
            
            if not words1 or not words2:
                return 0.0
            
            # Jaccard similarity
            intersection = words1.intersection(words2)
            union = words1.union(words2)
            jaccard_similarity = len(intersection) / len(union) if union else 0
            
            # N-gram similarity (for catching similar phrases)
            ngram_similarity = self._calculate_ngram_similarity(text1_lower, text2_lower, n=2)
            
            # Technical terms boost
            tech_terms = {
                'error', 'bug', 'issue', 'problem', 'failure', 'exception', 'timeout', 
                'connection', 'database', 'api', 'server', 'client', 'performance',
                'crash', 'memory', 'leak', 'security', 'authentication', 'authorization',
                'null', 'undefined', 'invalid', 'missing', 'corrupt', 'deadlock'
            }
            tech_overlap = len(words1.intersection(words2).intersection(tech_terms))
            tech_boost = min(tech_overlap * 0.1, 0.3)
            
            # Error pattern matching
            error_patterns = [
                r'\b\d{3}\b',  # HTTP status codes
                r'exception|error|fail',
                r'null|undefined|missing',
                r'timeout|connection'
            ]
            
            pattern_match_boost = 0
            for pattern in error_patterns:
                if re.search(pattern, text1_lower) and re.search(pattern, text2_lower):
                    pattern_match_boost += 0.05
            
            # Combined score
            final_score = (
                jaccard_similarity * 0.5 + 
                ngram_similarity * 0.3 + 
                tech_boost + 
                pattern_match_boost
            )
            
            return min(final_score, 1.0)
            
        except Exception:
            return 0.0
    
    def _calculate_ngram_similarity(self, text1: str, text2: str, n: int = 2) -> float:
        """Calculate n-gram similarity between texts"""
        try:
            def get_ngrams(text, n):
                words = text.split()
                return set(' '.join(words[i:i+n]) for i in range(len(words)-n+1))
            
            ngrams1 = get_ngrams(text1, n)
            ngrams2 = get_ngrams(text2, n)
            
            if not ngrams1 or not ngrams2:
                return 0.0
            
            intersection = ngrams1.intersection(ngrams2)
            union = ngrams1.union(ngrams2)
            
            return len(intersection) / len(union) if union else 0
            
        except Exception:
            return 0.0
    
    def _identify_root_causes(self, problem_df: pd.DataFrame, full_df: pd.DataFrame) -> Dict[str, Any]:
        """Identify comprehensive root causes from problem patterns"""
        try:
            root_causes = {}
            
            # Analyze by assignee patterns
            if 'assignee' in problem_df.columns:
                assignee_problems = problem_df['assignee'].value_counts()
                if not assignee_problems.empty:
                    total_assignee_issues = full_df['assignee'].value_counts()
                    assignee_problem_rate = {}
                    
                    for assignee in assignee_problems.index:
                        if assignee in total_assignee_issues.index:
                            rate = (assignee_problems[assignee] / total_assignee_issues[assignee]) * 100
                            assignee_problem_rate[assignee] = {
                                'problem_count': int(assignee_problems[assignee]),
                                'total_count': int(total_assignee_issues[assignee]),
                                'problem_rate': round(rate, 1)
                            }
                    
                    root_causes['assignee_patterns'] = assignee_problem_rate
            
            # Analyze by component/area patterns
            if 'components' in problem_df.columns:
                component_problems = problem_df['components'].value_counts()
                if not component_problems.empty:
                    root_causes['component_patterns'] = {
                        comp: {'issue_count': int(count), 'percentage': round((count/len(problem_df))*100, 1)}
                        for comp, count in component_problems.head(5).items()
                    }
            
            # Analyze timing patterns
            if 'created_dt' in problem_df.columns:
                problem_df_clean = problem_df.dropna(subset=['created_dt'])
                if not problem_df_clean.empty:
                    problem_df_clean['hour'] = problem_df_clean['created_dt'].dt.hour
                    problem_df_clean['day_of_week'] = problem_df_clean['created_dt'].dt.day_name()
                    problem_df_clean['week_of_month'] = problem_df_clean['created_dt'].dt.day // 7 + 1
                    
                    # Hour patterns
                    hour_distribution = problem_df_clean['hour'].value_counts().sort_index()
                    peak_hours = hour_distribution.nlargest(3).index.tolist()
                    
                    # Day patterns
                    day_distribution = problem_df_clean['day_of_week'].value_counts()
                    peak_days = day_distribution.nlargest(2).index.tolist()
                    
                    root_causes['timing_patterns'] = {
                        'peak_hours': peak_hours,
                        'peak_days': peak_days,
                        'hour_distribution': dict(hour_distribution),
                        'day_distribution': dict(day_distribution)
                    }
            
            # Analyze issue type patterns
            if 'issuetype' in problem_df.columns:
                type_problems = problem_df['issuetype'].value_counts()
                root_causes['issue_type_patterns'] = {
                    issue_type: {
                        'count': int(count),
                        'percentage': round((count/len(problem_df))*100, 1)
                    }
                    for issue_type, count in type_problems.items()
                }
            
            # Analyze keyword patterns in summaries
            keywords = self._extract_problem_keywords(problem_df)
            if keywords:
                root_causes['keyword_patterns'] = keywords
            
            # Analyze labels if available
            if 'labels' in problem_df.columns:
                label_patterns = self._analyze_label_patterns(problem_df)
                if label_patterns:
                    root_causes['label_patterns'] = label_patterns
            
            return root_causes
            
        except Exception as e:
            return {"error": f"Root cause identification failed: {str(e)}"}
    
    def _extract_problem_keywords(self, df: pd.DataFrame) -> Dict[str, int]:
        """Extract common keywords from problem descriptions"""
        try:
            if 'summary' not in df.columns:
                return {}
            
            # Common problem indicators
            problem_keywords = {
                'error', 'fail', 'bug', 'issue', 'problem', 'crash', 'timeout',
                'slow', 'performance', 'memory', 'leak', 'security', 'connection',
                'database', 'api', 'authentication', 'null', 'undefined', 'missing',
                'incorrect', 'invalid', 'broken', 'deadlock', 'race', 'condition'
            }
            
            keyword_counts = {}
            
            for summary in df['summary'].dropna():
                words = summary.lower().split()
                for word in words:
                    clean_word = re.sub(r'[^\w\s]', '', word)
                    if clean_word in problem_keywords:
                        keyword_counts[clean_word] = keyword_counts.get(clean_word, 0) + 1
            
            # Return top keywords
            return dict(sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:10])
            
        except Exception:
            return {}
    
    def _analyze_label_patterns(self, df: pd.DataFrame) -> Dict[str, int]:
        """Analyze label patterns in problematic issues"""
        try:
            all_labels = []
            for labels_str in df['labels'].dropna():
                if labels_str:
                    labels = [label.strip() for label in labels_str.split(',')]
                    all_labels.extend(labels)
            
            if not all_labels:
                return {}
            
            label_counts = pd.Series(all_labels).value_counts()
            return dict(label_counts.head(10))
            
        except Exception:
            return {}
    
    def _extract_common_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract common patterns from problematic issues"""
        try:
            patterns = {}
            
            # Extract error message patterns
            if 'summary' in df.columns:
                error_patterns = self._extract_error_patterns(df['summary'])
                if error_patterns:
                    patterns['error_patterns'] = error_patterns
            
            # Extract resolution patterns for resolved issues
            resolved_df = df[df['status'].isin(['Done', 'Resolved', 'Closed'])]
            if not resolved_df.empty and 'resolution_days' in resolved_df.columns:
                resolution_stats = {
                    'avg_resolution_days': round(resolved_df['resolution_days'].mean(), 1),
                    'median_resolution_days': round(resolved_df['resolution_days'].median(), 1),
                    'resolution_rate': round((len(resolved_df) / len(df)) * 100, 1)
                }
                patterns['resolution_stats'] = resolution_stats
            
            return patterns
            
        except Exception:
            return {}
    


    def _extract_error_patterns(self, summaries: pd.Series) -> List[Dict[str, Any]]:
        """Extract common error patterns from issue summaries"""
        try:
            error_patterns = []
            
            # Common error pattern regexes
            patterns_to_check = [
                (r'(\w+)Error\b', 'Error Types'),
                (r'(\w+)Exception\b', 'Exception Types'),
                (r'HTTP\s*(\d{3})', 'HTTP Status Codes'),
                (r'timeout|timed out', 'Timeout Issues'),
                (r'null|undefined|NaN', 'Null Reference Issues'),
                (r'memory|heap|OOM', 'Memory Issues'),
                (r'connection|network', 'Connection Issues'),
                (r'permission|access denied|unauthorized', 'Permission Issues')
            ]
            
            for pattern, category in patterns_to_check:
                matches = []
                for summary in summaries.dropna():
                    found = re.findall(pattern, summary, re.IGNORECASE)
                    matches.extend(found)
                
                if matches:
                    match_counts = pd.Series(matches).value_counts()
                    error_patterns.append({
                        'category': category,
                        'patterns': dict(match_counts.head(5))
                    })
            
            return error_patterns
            
        except Exception:
            return []
    
    def _find_similar_issues(self, problem_df: pd.DataFrame, full_df: pd.DataFrame) -> List[Dict]:
        """Find groups of similar issues with enhanced pattern matching"""
        try:
            similar_groups = {}
            processed_issues = set()
            
            # Group by similar summaries and descriptions
            for idx, row in problem_df.iterrows():
                if row['key'] in processed_issues:
                    continue
                
                summary_words = set(word.lower() for word in row['summary'].split() 
                                  if word.lower() not in self.stop_words and len(word) > 2)
                
                # Find all similar issues
                group_issues = [row['key']]
                group_words = summary_words.copy()
                
                for idx2, row2 in full_df.iterrows():
                    if row2['key'] == row['key'] or row2['key'] in processed_issues:
                        continue
                    
                    similarity = self._calculate_similarity(row['summary'], row2['summary'])
                    if similarity >= 0.5:  # Lower threshold for grouping
                        group_issues.append(row2['key'])
                        words2 = set(word.lower() for word in row2['summary'].split() 
                                   if word.lower() not in self.stop_words and len(word) > 2)
                        group_words.update(words2)
                
                if len(group_issues) > 1:
                    # Extract common pattern from group
                    common_words = list(group_words)[:5]
                    pattern = ' '.join(common_words)
                    
                    group_key = f"group_{len(similar_groups) + 1}"
                    similar_groups[group_key] = {
                        'issues': group_issues[:10],  # Limit for display
                        'pattern': pattern,
                        'issue_count': len(group_issues),
                        'sample_summary': row['summary']
                    }
                    
                    processed_issues.update(group_issues)
            
            # Return sorted by issue count
            result = []
            for group_key, group_data in similar_groups.items():
                result.append({
                    'pattern': group_data['pattern'],
                    'issue_count': group_data['issue_count'],
                    'issues': group_data['issues'],
                    'sample_summary': group_data['sample_summary']
                })
            
            return sorted(result, key=lambda x: x['issue_count'], reverse=True)[:10]
            
        except Exception as e:
            return [{"error": f"Similar issue detection failed: {str(e)}"}]
    
    def _analyze_resolution_patterns(self, similar_issues: List[Dict], full_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze how similar issues were resolved with detailed insights"""
        try:
            if not similar_issues:
                return {"message": "No similar issues found for resolution analysis"}
            
            resolution_patterns = {}
            
            for group in similar_issues:
                if 'error' in group:
                    continue
                    
                all_issues = full_df[full_df['key'].isin(group['issues'])]
                resolved_issues = all_issues[
                    all_issues['status'].isin(['Done', 'Resolved', 'Closed'])
                ]
                
                if not resolved_issues.empty:
                    pattern_data = {
                        'total_issues': len(all_issues),
                        'resolved_count': len(resolved_issues),
                        'resolution_rate': round((len(resolved_issues) / len(all_issues)) * 100, 1)
                    }
                    
                    # Resolution time analysis
                    if 'resolution_days' in resolved_issues.columns:
                        res_days = resolved_issues['resolution_days'].dropna()
                        if not res_days.empty:
                            pattern_data.update({
                                'avg_resolution_days': round(res_days.mean(), 1),
                                'median_resolution_days': round(res_days.median(), 1),
                                'min_resolution_days': round(res_days.min(), 1),
                                'max_resolution_days': round(res_days.max(), 1)
                            })
                    
                    # Resolution method analysis (from assignees who resolved)
                    if 'assignee' in resolved_issues.columns:
                        top_resolvers = resolved_issues['assignee'].value_counts().head(3)
                        pattern_data['top_resolvers'] = dict(top_resolvers)
                    
                    resolution_patterns[group['pattern']] = pattern_data
            
            return resolution_patterns
            
        except Exception as e:
            return {"error": f"Resolution pattern analysis failed: {str(e)}"}
    
    def _get_resolution_info(self, issue_row: pd.Series) -> Dict[str, Any]:
        """Get resolution information for an issue"""
        try:
            resolution_info = {
                'status': issue_row.get('status', 'Unknown'),
                'is_resolved': issue_row.get('status', '') in ['Done', 'Resolved', 'Closed']
            }
            
            if 'resolution_days' in issue_row and pd.notna(issue_row['resolution_days']):
                resolution_info['resolution_days'] = round(issue_row['resolution_days'], 1)
            
            if 'assignee' in issue_row:
                resolution_info['resolved_by'] = issue_row['assignee']
            
            return resolution_info
            
        except Exception:
            return {}
    
    def _generate_recommendations(self, root_causes: Dict, resolution_patterns: Dict, common_patterns: Dict) -> List[str]:
        """Generate enhanced actionable recommendations based on comprehensive analysis"""
        recommendations = []
        
        try:
            # Assignee-based recommendations
            if 'assignee_patterns' in root_causes:
                assignee_data = root_causes['assignee_patterns']
                high_problem_assignees = [
                    (assignee, data) for assignee, data in assignee_data.items() 
                    if data['problem_rate'] > 30
                ]
                
                for assignee, data in high_problem_assignees[:3]:
                    recommendations.append(
                        f"🎯 **{assignee}**: {data['problem_rate']}% problem rate "
                        f"({data['problem_count']}/{data['total_count']} issues). "
                        f"Consider: Additional training, pair programming, or workload review."
                    )
            
            # Component-based recommendations
            if 'component_patterns' in root_causes:
                top_components = list(root_causes['component_patterns'].items())[:2]
                for component, data in top_components:
                    if data['percentage'] > 20:
                        recommendations.append(
                            f"🔧 **{component} Component**: {data['percentage']}% of problems. "
                            f"Actions: Conduct code review, add unit tests, consider refactoring."
                        )
            
            # Timing-based recommendations
            if 'timing_patterns' in root_causes:
                timing = root_causes['timing_patterns']
                if timing.get('peak_hours'):
                    peak_hours_str = ', '.join([f"{h}:00" for h in timing['peak_hours']])
                    recommendations.append(
                        f"⏰ **Deployment Timing**: High problem rates at {peak_hours_str}. "
                        f"Consider: Avoid deployments during these hours, increase monitoring."
                    )
                
                if timing.get('peak_days'):
                    peak_days_str = ', '.join(timing['peak_days'])
                    recommendations.append(
                        f"📅 **Weekly Pattern**: Problems peak on {peak_days_str}. "
                        f"Consider: Schedule critical work mid-week, review weekend processes."
                    )
            
            # Keyword-based recommendations
            if 'keyword_patterns' in root_causes:
                top_keywords = list(root_causes['keyword_patterns'].items())[:3]
                if top_keywords:
                    keyword_str = ', '.join([f"{k} ({v})" for k, v in top_keywords])
                    recommendations.append(
                        f"🔍 **Common Issues**: Frequent keywords: {keyword_str}. "
                        f"Focus testing and monitoring on these areas."
                    )
            
            # Resolution-based recommendations
            if resolution_patterns:
                high_success_patterns = [
                    (pattern, data) for pattern, data in resolution_patterns.items()
                    if isinstance(data, dict) and data.get('resolution_rate', 0) > 80
                ]
                
                for pattern, data in high_success_patterns[:2]:
                    avg_days = data.get('avg_resolution_days', 'N/A')
                    recommendations.append(
                        f"✅ **Success Pattern**: '{pattern[:50]}...' - {data['resolution_rate']}% resolved. "
                        f"Avg resolution: {avg_days} days. Apply similar approach to new issues."
                    )
                
                # Identify slow resolution patterns
                slow_patterns = [
                    (pattern, data) for pattern, data in resolution_patterns.items()
                    if isinstance(data, dict) and data.get('avg_resolution_days', 0) > 30
                ]
                
                for pattern, data in slow_patterns[:1]:
                    recommendations.append(
                        f"⚠️ **Slow Resolution**: '{pattern[:50]}...' takes {data['avg_resolution_days']} days avg. "
                        f"Consider: Creating standard procedures, automation, or escalation paths."
                    )
            
            # Error pattern recommendations
            if 'error_patterns' in common_patterns:
                for error_category in common_patterns['error_patterns'][:2]:
                    category = error_category['category']
                    top_pattern = list(error_category['patterns'].items())[0] if error_category['patterns'] else None
                    if top_pattern:
                        recommendations.append(
                            f"🚨 **{category}**: Most common: {top_pattern[0]} ({top_pattern[1]} occurrences). "
                            f"Implement specific error handling and monitoring."
                        )
            
            # General process recommendations
            if not recommendations:
                recommendations.append(
                    "📊 **General**: Insufficient patterns for specific recommendations. "
                    "Continue monitoring and ensure comprehensive issue documentation."
                )
            
            # Add a summary recommendation
            if len(recommendations) > 3:
                recommendations.insert(0, 
                    "🎯 **Priority Actions**: Focus on top 3 recommendations below for maximum impact:"
                )
            
            return recommendations
            
        except Exception:
            return ["⚠️ Unable to generate recommendations due to data analysis issues."]
    
    def _generate_root_cause_insights(self, root_causes: Dict, similar_issues: List, resolution_patterns: Dict) -> str:
        """Generate narrative insights from root cause analysis"""
        try:
            insights = []
            
            # Overall pattern insight
            if similar_issues and len(similar_issues) > 0:
                total_similar = sum(group['issue_count'] for group in similar_issues if 'issue_count' in group)
                insights.append(
                    f"🔍 Identified {len(similar_issues)} distinct problem patterns affecting {total_similar} issues total."
                )
            
            # Team insight
            if 'assignee_patterns' in root_causes:
                assignee_data = root_causes['assignee_patterns']
                if assignee_data:
                    highest_rate = max(data['problem_rate'] for data in assignee_data.values())
                    insights.append(
                        f"👥 Team problem rates range from minimal to {highest_rate:.1f}%, indicating varying skill levels or workload challenges."
                    )
            
            # Timing insight
            if 'timing_patterns' in root_causes:
                timing = root_causes['timing_patterns']
                if 'peak_hours' in timing and timing['peak_hours']:
                    insights.append(
                        f"⏰ Clear temporal patterns detected - problems cluster around specific times, suggesting process or system issues."
                    )
            
            # Resolution insight
            if resolution_patterns:
                avg_rates = [data.get('resolution_rate', 0) for data in resolution_patterns.values() if isinstance(data, dict)]
                if avg_rates:
                    overall_resolution = sum(avg_rates) / len(avg_rates)
                    insights.append(
                        f"✅ Overall resolution rate: {overall_resolution:.1f}% - "
                        f"{'Good problem-solving capability' if overall_resolution > 70 else 'Room for improvement in resolution processes'}."
                    )
            
            return " | ".join(insights) if insights else "Analysis complete - review detailed findings above."
            
        except Exception:
            return "Insight generation completed with partial results."
    
    def _generate_enhanced_insights(self, data: Dict[str, Any], prompt: str) -> str:
        """Generate enhanced insights with pattern recognition and predictive analysis"""
        try:
            insights = []
            
            if 'issues_df' in data:
                df = data['issues_df']
                
                # Basic metrics with context
                total_issues = len(df)
                insights.append(f"📊 **Dataset**: {total_issues} issues analyzed")
                
                # Quality metrics with trends
                if 'issuetype' in df.columns:
                    bug_count = len(df[df['issuetype'].str.contains('Bug|Defect', case=False, na=False)])
                    if bug_count > 0:
                        bug_ratio = (bug_count / total_issues) * 100
                        quality_indicator = "🔴 Critical" if bug_ratio > 30 else "🟡 Moderate" if bug_ratio > 15 else "🟢 Good"
                        insights.append(f"🐛 **Quality {quality_indicator}**: {bug_ratio:.1f}% bugs ({bug_count} issues)")
                
                # Performance insights with benchmarks
                if 'status' in df.columns:
                    completion_rate = (df['status'].isin(['Done', 'Resolved', 'Closed'])).mean() * 100
                    performance = "Excellent" if completion_rate > 80 else "Good" if completion_rate > 60 else "Needs Improvement"
                    insights.append(f"✅ **Performance - {performance}**: {completion_rate:.1f}% completion rate")
                
                # Team insights with balance analysis
                if 'assignee' in df.columns:
                    team_size = df['assignee'].nunique()
                    avg_workload = total_issues / team_size if team_size > 0 else 0
                    workload_std = df.groupby('assignee').size().std()
                    balance = "Well-balanced" if workload_std < 5 else "Slightly uneven" if workload_std < 10 else "Highly uneven"
                    insights.append(f"👥 **Team ({balance})**: {team_size} members, {avg_workload:.1f} avg issues/person")
                
                # Priority insights with urgency indicator
                if 'priority' in df.columns:
                    high_priority = len(df[df['priority'].isin(['High', 'Critical', 'Highest', 'Blocker'])])
                    if high_priority > 0:
                        priority_ratio = (high_priority / total_issues) * 100
                        urgency = "🔥 Urgent attention needed" if priority_ratio > 25 else "⚠️ Monitor closely" if priority_ratio > 10 else "✓ Under control"
                        insights.append(f"⚡ **Priority {urgency}**: {priority_ratio:.1f}% high/critical ({high_priority} issues)")
                
                # Resolution time insights with efficiency rating
                if 'resolution_days' in df.columns:
                    resolved_df = df.dropna(subset=['resolution_days'])
                    if not resolved_df.empty:
                        avg_resolution = resolved_df['resolution_days'].mean()
                        median_resolution = resolved_df['resolution_days'].median()
                        efficiency = "Fast" if avg_resolution < 7 else "Moderate" if avg_resolution < 21 else "Slow"
                        insights.append(f"⏱️ **Resolution {efficiency}**: {avg_resolution:.1f} days avg, {median_resolution:.1f} days median")
                
                # Trend analysis
                if 'created_dt' in df.columns:
                    df_clean = df.dropna(subset=['created_dt'])
                    if len(df_clean) > 7:
                        recent_issues = df_clean[df_clean['created_dt'] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]
                        recent_rate = len(recent_issues) / 7
                        trend = "📈 Increasing" if recent_rate > avg_workload else "📉 Decreasing" if recent_rate < avg_workload * 0.8 else "➡️ Stable"
                        insights.append(f"📊 **Trend**: {trend} ({recent_rate:.1f} issues/day recently)")
            
            # Add context-specific insights based on prompt
            if prompt:
                prompt_lower = prompt.lower()
                if 'root cause' in prompt_lower:
                    insights.append("🔍 **Root Cause**: Use the root cause analysis feature for detailed problem patterns")
                elif 'performance' in prompt_lower:
                    insights.append("📈 **Performance**: Focus on completion rates and resolution times for improvement areas")
                elif 'quality' in prompt_lower:
                    insights.append("🎯 **Quality**: Monitor bug rates and implement preventive measures")
            
            return " | ".join(insights) if insights else "📊 Data analysis complete - ready for detailed exploration"
            
        except Exception as e:
            return f"⚠️ Enhanced analysis error: {str(e)}"

def main():
    st.markdown('<h1 class="main-header">🤖 AI Jira Reporting Agent</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if 'jira_data' not in st.session_state:
        st.session_state.jira_data = {}
    if 'prompts' not in st.session_state:
        st.session_state.prompts = []
    if 'jira_connected' not in st.session_state:
        st.session_state.jira_connected = False
    if 'jira_projects' not in st.session_state:
        st.session_state.jira_projects = []
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Jira Configuration
        st.subheader("Jira Settings")
        jira_url = st.text_input("Jira URL", placeholder="https://yourcompany.atlassian.net")
        jira_username = st.text_input("Username/Email", placeholder="name@company.com")
        jira_token = st.text_input("API Token", type="password", placeholder="Generated API Token")
                                  
        # Project selection
        if jira_url and jira_username and jira_token:
            config = JiraConfig(jira_url, jira_username, jira_token, [])
            jira_api = JiraAPI(config)
            
            if st.button("Connect to Jira"):
                success, message = jira_api.test_connection()
                if success:
                    st.success(f"✅ {message}")
                    st.session_state.jira_connected = True
                    
                    # Get and store projects
                    projects = jira_api.get_projects()
                    if projects:
                        st.session_state.jira_projects = projects
                else:
                    st.error(f"❌ {message}")
                    st.session_state.jira_connected = False
                    
                    # Show debugging info
                    with st.expander("Debug Info"):
                        st.write(f"**URL:** {jira_api.base_url}/rest/api/3/myself")
                        st.write(f"**Username:** {jira_username}")
                        st.write("**Checklist:**")
                        st.write("- URL format: https://yourcompany.atlassian.net")
                        st.write("- Username: Your Atlassian email")
                        st.write("- API Token: From Atlassian Account → Security → API tokens")
            
            # Show project selection if connected
            if st.session_state.jira_connected and st.session_state.jira_projects:
                st.subheader("Select Projects")
                project_options = [f"{p['key']} - {p['name']}" for p in st.session_state.jira_projects]
                selected_projects = st.multiselect(
                    "Choose projects to analyze:",
                    options=project_options,
                    help="Select one or more projects for analysis",
                    key="project_selection"
                )
                
                if selected_projects:
                    project_keys = [p.split(' - ')[0] for p in selected_projects]
                    config.projects = project_keys
                    st.success(f"Selected projects: {', '.join(project_keys)}")
                    
                    # Store config in session state
                    st.session_state.jira_config = config
        
        # Date range
        st.subheader("Date Range")
        start_date = st.date_input("Start Date", value=datetime(2025, 1, 1))
        end_date = st.date_input("End Date", value=datetime.now())
        
        # Store dates in session state for use in sanity checks
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date
        
        # Fetch Data Section
        if st.button("🔄 Fetch Jira Data", type="primary", use_container_width=True):
            if hasattr(st.session_state, 'jira_config') and st.session_state.jira_config.projects:
                config = st.session_state.jira_config
                jira_api = JiraAPI(config)
                
                with st.spinner("Fetching Jira data..."):
                    # Fetch issues with expanded fields
                    issues = jira_api.get_issues_with_expanded_fields(
                        config.projects,
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d')
                    )
                    
                    if issues:
                        # Process issues data with expanded fields
                        issues_data = []
                        all_issue_keys = []  # Collect all issue keys for worklog fetching
                        
                        for issue in issues:
                            # Extract project key from issue key (e.g., "PROJ-123" -> "PROJ")
                            project_key = issue['key'].split('-')[0] if '-' in issue['key'] else 'UNKNOWN'
                            all_issue_keys.append(issue['key'])
                            
                            # Extract parent/epic information
                            parent_summary = None
                            epic_summary = None
                            
                            # Try to get parent information
                            if 'parent' in issue['fields'] and issue['fields']['parent']:
                                parent_summary = issue['fields']['parent'].get('fields', {}).get('summary', '')
                            
                            # Try to get epic information (custom field might vary)
                            # Common epic link field names
                            epic_fields = ['customfield_10014', 'customfield_10008', 'customfield_10100', 'epic']
                            for epic_field in epic_fields:
                                if epic_field in issue['fields'] and issue['fields'][epic_field]:
                                    if isinstance(issue['fields'][epic_field], dict):
                                        epic_summary = issue['fields'][epic_field].get('fields', {}).get('summary', '')
                                        if epic_summary:
                                            break
                            
                            if 'customfield_10182' in issue['fields'] and issue['fields']['customfield_10182']:
                                story_points_field = issue['fields'].get('customfield_10182')
                            else:
                                story_points_field = 0
                            
                            if 'customfield_10010' in issue['fields'] and issue['fields']['customfield_10010']:
                                sprint_field = issue['fields']['customfield_10010']
                            else:
                                sprint_field = 'None'
                                   
                            # Combine parent/epic summary
                            parent_epic_summary = parent_summary or epic_summary or ''
                            
                            # Extract dates
                            if 'duedate' in issue['fields'] and issue['fields']['duedate']:
                                due_date_value = issue['fields'].get('duedate')
                            else:
                                due_date_value = 'None'
                                
                            if 'customfield_10252' in issue['fields'] and issue['fields']['customfield_10252']:
                                close_date_value = issue['fields'].get('customfield_10252')
                            else:
                                close_date_value = 'None'
                                                         
                            #Old EMRF Country Fields
                            if 'customfield_10540' in issue['fields'] and issue['fields']['customfield_10540']:
                                country = issue['fields']['customfield_10540']['value']
                            else:
                                country = 'None'
                                                                         
                            # Try to find start date from custom fields
                            start_date_fields = ['customfield_10015', 'customfield_10016', 'startDate']
                            for start_field in start_date_fields:
                                if start_field in issue['fields'] and issue['fields'][start_field]:
                                    start_date_value = issue['fields'][start_field]
                                else:
                                    start_date_value = 'None'
                                    break
                                
                            # Get project name
                            project_name = issue['fields'].get('project', {}).get('name', project_key)
                            
                            if 'customfield_12602' in issue['fields'] and issue['fields']['customfield_12602']:
                                reporting_country = issue['fields']['customfield_12602']
                            else:
                                reporting_country = 'None'
                                
                            if 'customfield_12603' in issue['fields'] and issue['fields']['customfield_12603']:
                                reporting_process = issue['fields']['customfield_12603']
                            else:
                                reporting_process = 'None'
                                
                            if 'customfield_11719' in issue['fields'] and issue['fields']['customfield_11719']:
                                hdeps_delivery_name = issue['fields']['customfield_11719']['value']
                            else:
                                hdeps_delivery_name = 'None'
                            
                        
                            #customfield_10037  -- Time to first response
                            #customfield_12635  -- Time to First Repsonse New SLA
                            # Extract time_to_first_response from customfield_10037
                            if 'customfield_10037' in issue['fields'] and issue['fields']['customfield_10037']:
                                completed_cycles = issue['fields']['customfield_10037'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_response = completed_cycles[0].get('elapsedTime', {}).get('millis', None)
                                else:
                                    time_to_first_response = None
                            else:
                                time_to_first_response = None
                                
                            if 'customfield_12635' in issue['fields'] and issue['fields']['customfield_12635']:
                                completed_cycles = issue['fields']['customfield_12635'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_response_new = completed_cycles[0].get('elapsedTime', {}).get('millis', None)
                                else:
                                    time_to_first_response_new = None
                            else:
                                time_to_first_response_new = None

                            # Create combined field with fallback logic
                            first_response_time = time_to_first_response if time_to_first_response is not None else (
                                time_to_first_response_new if time_to_first_response_new is not None else 'None'
                            )
                            
                            # Also extract the SLA goals for both fields
                            # Time to first response goal from customfield_10037
                            if 'customfield_10037' in issue['fields'] and issue['fields']['customfield_10037']:
                                completed_cycles = issue['fields']['customfield_10037'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_response_goal = completed_cycles[0].get('goalDuration', {}).get('millis', None)
                                else:
                                    time_to_first_response_goal = None
                            else:
                                time_to_first_response_goal = None
                        
                            # Time to first response new goal from customfield_12635
                            if 'customfield_12635' in issue['fields'] and issue['fields']['customfield_12635']:
                                completed_cycles = issue['fields']['customfield_12635'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_response_new_goal = completed_cycles[0].get('goalDuration', {}).get('millis', None)
                                else:
                                    time_to_first_response_new_goal = None
                            else:
                                time_to_first_response_new_goal = None

                            # Create combined goal field with same fallback logic
                            first_response_goal = time_to_first_response_goal if time_to_first_response_goal is not None else (
                                time_to_first_response_new_goal if time_to_first_response_new_goal is not None else 'None')    
                            
                            #customfield_10036  -- Time to resolution
                            #customfield_12636  -- Time To Resolution New SLA
                            # Extract time_to_first_resolution from customfield_10036
                            if 'customfield_10036' in issue['fields'] and issue['fields']['customfield_10036']:
                                completed_cycles = issue['fields']['customfield_10036'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_resolution = completed_cycles[0].get('elapsedTime', {}).get('millis', None)
                                else:
                                    time_to_first_resolution = None
                            else:
                                time_to_first_resolution = None
                                
                            # Extract Time To Resolution New SLA from customfield_12636
                            if 'customfield_12636' in issue['fields'] and issue['fields']['customfield_12636']:
                                completed_cycles = issue['fields']['customfield_12636'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_resolution_new = completed_cycles[0].get('elapsedTime', {}).get('millis', None)
                                else:
                                    time_to_first_resolution_new = None
                            else:
                                time_to_first_resolution_new = None
                                
                            # Create combined field with fallback logic
                            first_resolution_time = time_to_first_resolution if time_to_first_resolution is not None else (
                                time_to_first_resolution_new if time_to_first_resolution_new is not None else 'None'
                            )
                            
                            # Also extract the SLA goals for both fields
                            # Time to first resolution goal from customfield_10036
                            if 'customfield_10036' in issue['fields'] and issue['fields']['customfield_10036']:
                                completed_cycles = issue['fields']['customfield_10036'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_resolution_goal = completed_cycles[0].get('goalDuration', {}).get('millis', None)
                                else:
                                    time_to_first_resolution_goal = None
                            else:
                                time_to_first_resolution_goal = None
                            

                            # Time to first response new goal from customfield_12636
                            if 'customfield_12636' in issue['fields'] and issue['fields']['customfield_12636']:
                                completed_cycles = issue['fields']['customfield_12636'].get('completedCycles', [])
                                if completed_cycles:
                                    time_to_first_resolution_new_goal = completed_cycles[0].get('goalDuration', {}).get('millis', None)
                                else:
                                    time_to_first_resolution_new_goal = None
                            else:
                                time_to_first_resolution_new_goal = None
                                
                            # Create combined goal field with same fallback logic
                            first_resolution_goal = time_to_first_resolution_goal if time_to_first_resolution_goal is not None else (
                                time_to_first_resolution_new_goal if time_to_first_resolution_new_goal is not None else 'None')
        
                                
                            if 'customfield_10337' in issue['fields'] and issue['fields']['customfield_10337']:
                                resolution_comment = issue['fields']['customfield_10337']['value']
                            else:
                                resolution_comment = 'None'
                                
                            issues_data.append({
                                'key': issue['key'],
                                'project': project_key,
                                'project_name': project_name,
                                'summary': issue['fields']['summary'],
                                'status': issue['fields']['status']['name'],
                                'assignee': issue['fields']['assignee']['displayName'] if issue['fields']['assignee'] else 'Unassigned',
                                'reporter': issue['fields']['reporter']['displayName'] if issue['fields']['reporter'] else 'Unknown',
                                'created': issue['fields']['created'],
                                'updated': issue['fields'].get('updated', ''),
                                'resolutiondate': issue['fields'].get('resolutiondate', ''),
                                'priority': issue['fields']['priority']['name'] if issue['fields']['priority'] else 'None',
                                'issuetype': issue['fields']['issuetype']['name'],
                                'description': issue['fields'].get('description', ''),
                                'labels': ','.join(issue['fields'].get('labels', [])),
                                'components': ','.join([c['name'] for c in issue['fields'].get('components', [])]),
                                'fixVersions': ','.join([v['name'] for v in issue['fields'].get('fixVersions', [])]),
                                'parent_epic_summary': parent_epic_summary if parent_epic_summary else 'None',
                                'closed_date': close_date_value,
                                'start_date': start_date_value,
                                'due_date': due_date_value,
                                'story_points': story_points_field,
                                'sprint':sprint_field,
                                'hdeps_delivery_type': hdeps_delivery_name,
                                'country':country,
                                'reporting_country':reporting_country,
                                'reporting_process':reporting_process,
                                'first_response_time': first_response_time,
                                'first_resolution_time':first_resolution_time,
                                'first_response_goal': first_response_goal,
                                'first_resolution_goal':first_resolution_goal,
                                'ticket_resolution': resolution_comment,
                                'url': f"{jira_url}/browse/{issue['key']}"
                            })
                        
                        issues_df = pd.DataFrame(issues_data)
                        st.session_state.jira_data['issues_df'] = issues_df
                        
                        # Fetch worklogs for ALL issues (with reasonable limit)
                        #st.info(f"Fetching worklogs for {len(all_issue_keys)} issues...")
                        
                        # Limit worklog fetching to prevent timeout (adjust as needed)
                        #worklog_limit = min(len(all_issue_keys), 200)
                        #limited_issue_keys = all_issue_keys[:worklog_limit]
                        
                        #worklogs = jira_api.get_worklogs(limited_issue_keys)
                        
                        #if worklogs:
                            #worklogs_df = pd.DataFrame(worklogs)
                            #st.session_state.jira_data['worklogs_df'] = worklogs_df
                            #st.success(f"✅ Fetched {len(issues)} issues and {len(worklogs)} worklogs from {len(config.projects)} projects")
                        #else:
                            #st.warning(f"✅ Fetched {len(issues)} issues but no worklogs found")
                        
                        st.rerun()
                    else:
                        st.warning("No issues found in the specified date range")
            else:
                st.warning("Please configure Jira settings and select projects first.")
    
    # Main content area - Updated tabs
    tab1,tab2,tab3,tab4,tab5,tab6 = st.tabs(["✔️ Sanity Check", "📈 Operations Report", "🛠️ Support Report", "🔍Cause Code Analysis", "🧑‍💻 ProdOps Report","💬 AI Chat"])
    #Sanity Check Tab
    with tab1:
        display_sanity_check_tab()     
    #Operational Reporting Tab        
    with tab2:
        display_dtedc_analysis()
    #Support Report Tab     
    with tab3:
        display_support_overview()
        st.markdown("---")
        display_response_resolution()
    #Cause Code Tab
    with tab4:
        display_cause_code_analysis()
    #ProdOps Reporting Tab        
    with tab5:
        display_prodOps_analysis()
    #AI Chat Tab
    with tab6:
        st.header("AI Chat Assistant")
        st.write("Ask questions about your Jira data and get insights with visualizations")
        
        # Initialize chat history
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []
        
        # Chat interface
        if 'issues_df' in st.session_state.jira_data:
            # Enhanced AI features section
            col1 = st.columns(1)[0]
            
            with col1:
                st.info("🧠 **Enhanced AI Intelligence**")
                with st.expander("View AI Capabilities"):
                    st.markdown("""
                    **🔍 Root Cause Analysis:**
                    - "Perform root cause analysis on failed issues"
                    - "Find root causes for blocked tasks"
                    - "Analyze problems in [specific component]"
                    
                    **🔗 Similar Issue Detection:**
                    - "Find issues similar to [ISSUE-KEY]"
                    - "Show me all issues related to authentication errors"
                    - "Group similar bugs together"
                    
                    **📊 Advanced Analytics:**
                    - "Show workload trends for all projects"
                    - "Compare team performance across projects"
                    - "Analyze resolution patterns for bugs"
                    
                    **🎯 Intelligent Filtering:**
                    - "Show high priority bugs assigned to John"
                    - "Analyze completed issues from last month"
                    - "Compare this sprint vs last sprint"
                    
                    **📈 Predictive Insights:**
                    - "When will the backlog be completed?"
                    - "Predict next month's workload"
                    - "Estimate time to resolve open bugs"
                    """)
            
            # Chat input section
            col1, col2 = st.columns([3, 1])
            
            with col1:
                user_question = st.text_input(
                    "Ask about your Jira data:",
                    placeholder="e.g., Show me a bar chart of monthly workloads for all projects"
                )
            
            with col2:
                chart_preference = st.selectbox(
                    "Preferred Chart",
                    ["Auto", "Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot", "Histogram", "Sunburst", "Treemap", "Funnel", "Heatmap"],
                    help="Select preferred chart type (AI will adapt based on data)"
                )
            
            col1, col2 = st.columns([1, 4])
            with col1:
                if st.button("🚀 Ask AI"):
                    if user_question:
                        # Fixed: Pass chart_preference as part of the processing
                        process_ai_chat_question(user_question, chart_preference)
            
            with col2:
                if st.button("🗑️ Clear Chat"):
                    st.session_state.chat_history = []
                    st.rerun()
            
            # Display chat history
            if st.session_state.chat_history:
                st.subheader("Chat History")
        
        else:
            st.info("Please fetch Jira data first to start chatting with the AI assistant.")
    
def display_project_dashboard(df, project_name):
    """Display dashboard for a specific project"""
    try:
        # Enhanced AI Agent for insights
        ai_agent = EnhancedAIAgent()
        
        # Generate AI insights for the dashboard
        insights = ai_agent.generate_insights({'issues_df': df}, f"dashboard overview for {project_name}")
        if insights:
            st.markdown(f'<div class="ai-response">{insights}</div>', unsafe_allow_html=True)
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Issues", len(df))
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            open_issues = len(df[~df['status'].isin(['Done', 'Resolved', 'Closed'])])
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Open Issues", open_issues)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            if 'worklogs_df' in st.session_state.jira_data:
                wl_df = st.session_state.jira_data['worklogs_df']
                if 'issue_key' in wl_df.columns:
                    project_worklogs = wl_df[wl_df['issue_key'].isin(df['key'])]
                    if 'timeSpentSeconds' in project_worklogs.columns and not project_worklogs.empty:
                        total_hours = project_worklogs['timeSpentSeconds'].sum() / 3600
                    else:
                        total_hours = 0
                else:
                    total_hours = 0
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Hours Logged", f"{total_hours:.1f}")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Hours Logged", "N/A")
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            unique_assignees = df['assignee'].nunique()
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Team Members", unique_assignees)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Status distribution
            status_counts = df['status'].value_counts()
            fig_status = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title=f"Issue Status Distribution - {project_name}"
            )
            st.plotly_chart(fig_status, use_container_width=True)
        
        with col2:
            # Priority distribution
            if 'priority' in df.columns:
                priority_counts = df['priority'].value_counts()
                fig_priority = px.bar(
                    x=priority_counts.index,
                    y=priority_counts.values,
                    title=f"Issue Priority Distribution - {project_name}"
                )
                st.plotly_chart(fig_priority, use_container_width=True)
            else:
                st.info("Priority data not available")
        
        # Trend analysis if date data is available
        if 'created' in df.columns:
            try:
                df_trend = df.copy()
                # Robust datetime conversion
                df_trend['created_dt'] = pd.to_datetime(df_trend['created'], errors='coerce', utc=True)
                df_trend = df_trend.dropna(subset=['created_dt'])
                
                if not df_trend.empty:
                    df_trend['created_month'] = df_trend['created_dt'].dt.to_period('M')
                    monthly_counts = df_trend.groupby('created_month').size().reset_index(name='count')
                    monthly_counts['month_str'] = monthly_counts['created_month'].apply(lambda x: x.strftime('%b %Y'))
                    
                    fig_trend = px.line(
                        monthly_counts, 
                        x='month_str', 
                        y='count',
                        title=f"Monthly Issue Creation Trend - {project_name}",
                        markers=True
                    )
                    st.plotly_chart(fig_trend, use_container_width=True)
                
            except Exception as e:
                st.warning(f"Could not create trend analysis: {str(e)}")
                # Show simple message instead of full error
                st.info("Trend analysis unavailable - date format may need adjustment")
        
        # Data table
        st.subheader(f"Issues Overview - {project_name}")
        display_columns = ['key', 'summary', 'status', 'assignee']
        if 'priority' in df.columns:
            display_columns.append('priority')
        if 'issuetype' in df.columns:
            display_columns.append('issuetype')
        
        available_columns = [col for col in display_columns if col in df.columns]
        st.dataframe(df[available_columns], use_container_width=True)
        
    except Exception as e:
        st.error(f"Error displaying dashboard: {str(e)}")

def process_ai_chat_question(question: str, chart_preference: str = "Auto"):
    """Advanced AI chat processing with sophisticated analysis"""
    if 'issues_df' not in st.session_state.jira_data:
        return
    
    df = st.session_state.jira_data['issues_df'].copy()
    ai_agent = EnhancedAIAgent()
    
    # Prepare enhanced dataset
    df = enhance_dataframe(df)
    
    chart = None
    response = ""
    
    try:
        # Check for similar issue queries
        if "similar to" in question.lower() or "issues like" in question.lower():
            # Extract issue key from question

            issue_key_match = re.search(r'[A-Z]+-\d+', question)
            if issue_key_match:
                issue_key = issue_key_match.group()
                similar_issues = ai_agent.find_similar_issues(df, issue_key)
                
                if similar_issues and not any('error' in issue for issue in similar_issues):
                    # Create visualization for similar issues
                    similar_df = pd.DataFrame(similar_issues[:10])
                    fig = px.bar(similar_df, x='key', y='similarity_score', 
                                title=f"Issues Similar to {issue_key}",
                                hover_data=['summary', 'status', 'resolution_info'])
                    
                    response = f"Found {len(similar_issues)} similar issues to {issue_key}. "
                    response += f"Top match: {similar_issues[0]['key']} (similarity: {similar_issues[0]['similarity_score']:.2f}). "
                    
                    # Add resolution insights
                    resolved_similar = [i for i in similar_issues if i.get('resolution_info', {}).get('is_resolved')]
                    if resolved_similar:
                        avg_resolution = sum(i['resolution_info'].get('resolution_days', 0) for i in resolved_similar) / len(resolved_similar)
                        response += f"Average resolution time for similar issues: {avg_resolution:.1f} days."
                    
                    chart = fig
                else:
                    response = f"No similar issues found for {issue_key}."
            else:
                response = "Please specify an issue key (e.g., PROJ-123) to find similar issues."
        
        # Check for root cause analysis queries
        elif any(phrase in question.lower() for phrase in ["root cause", "why", "problem analysis", "investigate"]):
            # Perform root cause analysis
            analysis_result = ai_agent.perform_root_cause_analysis(df)
            
            if 'error' not in analysis_result:
                # Create comprehensive visualization
                if 'root_causes' in analysis_result and 'assignee_patterns' in analysis_result['root_causes']:
                    assignee_data = analysis_result['root_causes']['assignee_patterns']
                    
                    # Create dataframe for visualization
                    assignee_df = pd.DataFrame([
                        {
                            'assignee': assignee,
                            'problem_rate': data['problem_rate'],
                            'problem_count': data['problem_count'],
                            'total_count': data['total_count']
                        }
                        for assignee, data in assignee_data.items()
                    ])
                    
                    if not assignee_df.empty:
                        fig = px.scatter(assignee_df, x='total_count', y='problem_rate', 
                                       size='problem_count', hover_data=['assignee'],
                                       title="Team Problem Analysis",
                                       labels={'problem_rate': 'Problem Rate (%)', 'total_count': 'Total Issues'})
                        chart = fig
                
                # Build comprehensive response
                response = f"Root Cause Analysis Complete: {analysis_result.get('total_analyzed', 0)} problematic issues analyzed. "
                
                if 'insights' in analysis_result:
                    response += analysis_result['insights'] + " "
                
                if 'recommendations' in analysis_result:
                    response += "\n\nKey Recommendations:\n"
                    for i, rec in enumerate(analysis_result['recommendations'][:3], 1):
                        response += f"{i}. {rec}\n"
            else:
                response = f"Root cause analysis error: {analysis_result['error']}"
        
        else:
            # Advanced intent detection with NLP-like processing
            analysis_result = advanced_intent_analysis(question, df, chart_preference)
            
            if analysis_result:
                chart, response = analysis_result
            else:
                # Fallback to smart general analysis
                chart, response = smart_general_analysis(df, question, chart_preference)
        
        # Add to chat history
        st.session_state.chat_history.append((question, response, chart))
        st.rerun()
        
    except Exception as e:
        response = f"Analysis error: {str(e)}. The system is learning from this to improve future responses."
        st.session_state.chat_history.append((question, response, None))
        st.rerun()

def advanced_intent_analysis(question, df, chart_preference="Auto"):
    """Advanced intent analysis with sophisticated pattern matching and filtering"""
    question_lower = question.lower()
    
    # Extract entities and parameters including filters
    entities = extract_entities_and_filters(question_lower)
    entities['chart_preference'] = chart_preference
    
    # Apply intelligent filtering first
    filtered_df = apply_intelligent_filters(df, entities, question_lower)
    
    # If filtering resulted in empty dataset, inform user
    if filtered_df.empty:
        return create_no_data_response(entities)
    
    # Extract analysis parameters
    time_period = entities.get('time_period', 'daily')
    target_field = entities.get('target_field')
    analysis_type = entities.get('analysis_type')
    
    # Worklog-specific intelligence
    if any(word in question_lower for word in ['worklog', 'time spent', 'logged time', 'hours', 'effort']):
        return analyze_worklog_intelligence(filtered_df, question_lower, entities)
    
    # Workload analysis (including "workloads")
    elif any(word in question_lower for word in ['workload', 'workloads', 'work load']):
        return analyze_workload_intelligence(filtered_df, question_lower, entities)
    
    # Period-based analysis intelligence
    elif any(word in question_lower for word in ['monthly', 'weekly', 'quarterly', 'yearly', 'period', 'breakdown']):
        return analyze_by_period_intelligence(filtered_df, question_lower, entities)
    
    # Comparative intelligence
    elif any(word in question_lower for word in ['compare', 'vs', 'versus', 'difference', 'contrast']):
        return comparative_intelligence(filtered_df, question_lower, entities)
    
    # Trend intelligence
    elif any(word in question_lower for word in ['trend', 'pattern', 'change over', 'evolution']):
        return trend_intelligence(filtered_df, question_lower, entities)
    
    # Performance intelligence
    elif any(word in question_lower for word in ['performance', 'efficiency', 'productivity', 'velocity']):
        return performance_intelligence(filtered_df, question_lower, entities)
    
    # Custom metric intelligence
    elif any(word in question_lower for word in ['ratio', 'percentage', 'rate', 'average', 'median']):
        return metric_intelligence(filtered_df, question_lower, entities)
    
    # Prediction intelligence
    elif any(word in question_lower for word in ['predict', 'forecast', 'estimate', 'when will', 'how long']):
        return prediction_intelligence(filtered_df, question_lower, entities)
    
    # Default to smart general analysis with filtered data
    return smart_general_analysis(filtered_df, question, chart_preference, entities)

def extract_entities_and_filters(question):
    """Extract entities, parameters, and filters from natural language"""
    entities = {}
    filters = {}
    
    # Time periods
    time_patterns = {
        'daily': ['daily', 'day', 'per day'],
        'weekly': ['weekly', 'week', 'per week'],
        'monthly': ['monthly', 'month', 'per month'],
        'quarterly': ['quarterly', 'quarter', 'per quarter'],
        'yearly': ['yearly', 'year', 'annual', 'per year'],
        'sprint': ['sprint', 'iteration']
    }
    
    for period, patterns in time_patterns.items():
        if any(pattern in question for pattern in patterns):
            entities['time_period'] = period
            break
    
    # Fields
    field_patterns = {
        'assignee': ['assignee', 'team member', 'person', 'who', 'assigned to'],
        'priority': ['priority', 'urgent', 'critical', 'important'],
        'status': ['status', 'state', 'progress'],
        'issuetype': ['type', 'issue type', 'kind'],
        'project': ['project', 'component'],
        'epic': ['epic', 'epic link'],
        'sprint': ['sprint', 'iteration'],
        'reporter': ['reporter', 'created by', 'reported by']
    }
    
    for field, patterns in field_patterns.items():
        if any(pattern in question for pattern in patterns):
            entities['target_field'] = field
            break
    
    # Analysis types
    analysis_patterns = {
        'breakdown': ['breakdown', 'break down', 'split', 'distribute'],
        'summary': ['summary', 'overview', 'total', 'sum'],
        'comparison': ['compare', 'vs', 'versus', 'difference'],
        'trend': ['trend', 'over time', 'pattern', 'change'],
        'distribution': ['distribution', 'spread', 'allocation']
    }
    
    for analysis, patterns in analysis_patterns.items():
        if any(pattern in question for pattern in patterns):
            entities['analysis_type'] = analysis
            break
    
    # Extract filters from natural language
    filters.update(extract_status_filters(question))
    filters.update(extract_assignee_filters(question))
    filters.update(extract_priority_filters(question))
    filters.update(extract_type_filters(question))
    filters.update(extract_project_filters(question))
    filters.update(extract_date_filters(question))
    
    entities['filters'] = filters
    return entities

def extract_status_filters(question):
    """Extract status-based filters"""
    filters = {}
    
    # Status keywords
    status_patterns = {
        'open': ['open', 'to do', 'backlog', 'new'],
        'in_progress': ['in progress', 'in development', 'in review', 'active'],
        'done': ['done', 'completed', 'finished', 'resolved', 'closed'],
        'blocked': ['blocked', 'impediment', 'stuck'],
        'testing': ['testing', 'qa', 'verification']
    }
    
    # Direct status mentions
    if any(word in question for word in ['status =', 'status:', 'where status']):
        # Extract quoted status
        status_match = re.search(r'status\s*[:=]\s*["\']([^"\']+)["\']', question)
        if status_match:
            filters['status'] = status_match.group(1)
    
    # Status category filters
    for category, patterns in status_patterns.items():
        if any(pattern in question for pattern in patterns):
            if category == 'open':
                filters['status_category'] = ['Open', 'To Do', 'Backlog', 'New']
            elif category == 'in_progress':
                filters['status_category'] = ['In Progress', 'In Development', 'In Review']
            elif category == 'done':
                filters['status_category'] = ['Done', 'Completed', 'Resolved', 'Closed']
            elif category == 'blocked':
                filters['status_category'] = ['Blocked', 'Impediment']
            elif category == 'testing':
                filters['status_category'] = ['Testing', 'QA', 'Verification']
            break
    
    return filters

def extract_assignee_filters(question):
    """Extract assignee-based filters"""
    filters = {}
    
    # Look for assignee mentions
    if 'assigned to' in question or 'assignee' in question:
        # Extract names in quotes
        name_match = re.search(r'(?:assigned to|assignee)\s*[:=]?\s*["\']([^"\']+)["\']', question)
        if name_match:
            filters['assignee'] = name_match.group(1)
        else:
            # Try to find names after "assigned to" or "assignee"
            name_match = re.search(r'(?:assigned to|assignee)\s*[:=]?\s*(\w+)', question)
            if name_match:
                filters['assignee'] = name_match.group(1)
    
    # Unassigned filter
    if 'unassigned' in question or 'no assignee' in question:
        filters['assignee'] = 'Unassigned'
    
    return filters

def extract_priority_filters(question):
    """Extract priority-based filters"""
    filters = {}
    
    priority_patterns = {
        'high': ['high priority', 'urgent', 'critical', 'highest', 'blocker'],
        'medium': ['medium priority', 'normal', 'standard'],
        'low': ['low priority', 'minor', 'lowest']
    }
    
    for priority, patterns in priority_patterns.items():
        if any(pattern in question for pattern in patterns):
            if priority == 'high':
                filters['priority_category'] = ['High', 'Highest', 'Critical', 'Urgent', 'Blocker']
            elif priority == 'medium':
                filters['priority_category'] = ['Medium', 'Normal', 'Standard']
            elif priority == 'low':
                filters['priority_category'] = ['Low', 'Lowest', 'Minor']
            break
    
    return filters

def extract_type_filters(question):
    """Extract issue type filters"""
    filters = {}
    
    type_patterns = {
        'bug': ['bug', 'defect', 'issue', 'problem'],
        'story': ['story', 'user story', 'feature'],
        'task': ['task', 'work item', 'todo'],
        'epic': ['epic', 'theme']
    }
    
    for issue_type, patterns in type_patterns.items():
        if any(pattern in question for pattern in patterns):
            filters['issuetype'] = issue_type.title()
            break
    
    return filters

def extract_project_filters(question):
    """Extract project-based filters"""
    filters = {}
    
    if 'project' in question:
        project_match = re.search(r'project\s*[:=]?\s*["\']([^"\']+)["\']', question)
        if project_match:
            filters['project'] = project_match.group(1)
        else:
            # Try to match project keys (e.g., PROJ-123 pattern)
            project_key_match = re.search(r'\b([A-Z]{2,})\b', question)
            if project_key_match:
                filters['project'] = project_key_match.group(1)
    
    return filters

def extract_date_filters(question):
    """Extract date-based filters"""
    filters = {}
    
    # Date range patterns
    date_patterns = {
        'last_week': ['last week', 'past week'],
        'last_month': ['last month', 'past month'],
        'this_week': ['this week', 'current week'],
        'this_month': ['this month', 'current month'],
        'last_quarter': ['last quarter', 'past quarter'],
        'this_quarter': ['this quarter', 'current quarter']
    }
    
    for period, patterns in date_patterns.items():
        if any(pattern in question for pattern in patterns):
            filters['date_range'] = period
            break
    
    # Specific month names
    months = ['january', 'february', 'march', 'april', 'may', 'june', 
              'july', 'august', 'september', 'october', 'november', 'december']
    
    for month in months:
        if month in question.lower():
            filters['month'] = month.capitalize()
            break
    
    return filters

def apply_intelligent_filters(df, entities, question):
    """Apply intelligent filtering based on extracted entities"""
    try:
        filtered_df = df.copy()
        filters = entities.get('filters', {})
        
        # Apply status filters
        if 'status' in filters:
            filtered_df = filtered_df[filtered_df['status'].str.contains(filters['status'], case=False, na=False)]
        elif 'status_category' in filters:
            filtered_df = filtered_df[filtered_df['status'].isin(filters['status_category'])]
        
        # Apply assignee filters
        if 'assignee' in filters:
            if filters['assignee'] == 'Unassigned':
                filtered_df = filtered_df[filtered_df['assignee'].isna() | (filtered_df['assignee'] == 'Unassigned')]
            else:
                filtered_df = filtered_df[filtered_df['assignee'].str.contains(filters['assignee'], case=False, na=False)]
        
        # Apply priority filters
        if 'priority_category' in filters:
            filtered_df = filtered_df[filtered_df['priority'].isin(filters['priority_category'])]
        
        # Apply type filters
        if 'issuetype' in filters:
            filtered_df = filtered_df[filtered_df['issuetype'].str.contains(filters['issuetype'], case=False, na=False)]
        
        # Apply project filters
        if 'project' in filters and 'project' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['project'].str.contains(filters['project'], case=False, na=False)]
        
        # Apply date filters
        if 'date_range' in filters and 'created_dt' in filtered_df.columns:
            now = pd.Timestamp.now(tz='UTC')
            
            if filters['date_range'] == 'last_week':
                start_date = now - pd.Timedelta(weeks=1)
                filtered_df = filtered_df[filtered_df['created_dt'] >= start_date]
            elif filters['date_range'] == 'last_month':
                start_date = now - pd.Timedelta(days=30)
                filtered_df = filtered_df[filtered_df['created_dt'] >= start_date]
            elif filters['date_range'] == 'this_week':
                start_of_week = now - pd.Timedelta(days=now.weekday())
                filtered_df = filtered_df[filtered_df['created_dt'] >= start_of_week]
            elif filters['date_range'] == 'this_month':
                start_of_month = now.replace(day=1)
                filtered_df = filtered_df[filtered_df['created_dt'] >= start_of_month]
        
        # Apply month filter if specified
        if 'month' in filters and 'created_dt' in filtered_df.columns:
            month_num = datetime.strptime(filters['month'], '%B').month
            filtered_df = filtered_df[filtered_df['created_dt'].dt.month == month_num]
        
        return filtered_df
        
    except Exception as e:
        st.warning(f"Filter application warning: {str(e)}")
        return df

def create_no_data_response(entities):
    """Create response when filtering results in no data"""
    filters = entities.get('filters', {})
    filter_description = []
    
    for key, value in filters.items():
        if isinstance(value, list):
            filter_description.append(f"{key}: {', '.join(value)}")
        else:
            filter_description.append(f"{key}: {value}")
    
    filter_text = ', '.join(filter_description) if filter_description else "applied filters"
    
    response = f"No data found matching the specified criteria ({filter_text}). "
    response += "Try adjusting your filters or check the available data."
    
    return None, response

def analyze_worklog_intelligence(df, question, entities):
    """Advanced worklog analysis with period intelligence"""
    try:
        if 'worklogs_df' not in st.session_state.jira_data:
            return None, "No worklog data available. Please ensure worklogs were fetched for all selected projects."
        
        wl_df = st.session_state.jira_data['worklogs_df'].copy()
        if wl_df.empty or 'timeSpentSeconds' not in wl_df.columns:
            return None, "Worklog data is empty or incomplete."
        
        # Filter worklogs to match the filtered issues
        if 'key' in df.columns and 'issue_key' in wl_df.columns:
            wl_df = wl_df[wl_df['issue_key'].isin(df['key'])]
            
            if wl_df.empty:
                return None, "No worklog data found for the filtered issues."
        
        # Enhance worklog data
        wl_df['hours'] = wl_df['timeSpentSeconds'] / 3600
        wl_df['started_dt'] = pd.to_datetime(wl_df['started'], errors='coerce')
        wl_df = wl_df.dropna(subset=['started_dt'])
        
        # Extract author information intelligently
        if 'author' in wl_df.columns:
            wl_df['author_name'] = wl_df['author'].apply(
                lambda x: x.get('displayName', x.get('name', 'Unknown')) if isinstance(x, dict) else str(x)
            )
        
        time_period = entities.get('time_period', 'daily')
        chart_preference = entities.get('chart_preference', 'Auto')
        
        # Period-based grouping
        if time_period == 'monthly':
            wl_df['period'] = wl_df['started_dt'].dt.to_period('M')
            group_col = 'period'
            title = "Monthly Worklog Analysis"
        elif time_period == 'weekly':
            wl_df['period'] = wl_df['started_dt'].dt.to_period('W')
            group_col = 'period'
            title = "Weekly Worklog Analysis"
        elif time_period == 'quarterly':
            wl_df['period'] = wl_df['started_dt'].dt.to_period('Q')
            group_col = 'period'
            title = "Quarterly Worklog Analysis"
        elif time_period == 'yearly':
            wl_df['period'] = wl_df['started_dt'].dt.to_period('Y')
            group_col = 'period'
            title = "Yearly Worklog Analysis"
        else:
            wl_df['period'] = wl_df['started_dt'].dt.date
            group_col = 'period'
            title = "Daily Worklog Analysis"
        
        # Multi-dimensional analysis
        if 'author_name' in wl_df.columns and any(word in question for word in ['by person', 'per person', 'team member']):
            # Period + Person breakdown
            period_person = wl_df.groupby([group_col, 'author_name'])['hours'].sum().reset_index()
            period_person['period_str'] = format_period_labels(period_person[group_col], time_period)
            
            # Choose chart based on preference
            if chart_preference == "Line Chart":
                fig = px.line(period_person, x='period_str', y='hours', color='author_name',
                            title=f"{title} by Team Member", markers=True)
            elif chart_preference == "Pie Chart":
                # Aggregate for pie chart
                author_totals = period_person.groupby('author_name')['hours'].sum().reset_index()
                fig = px.pie(author_totals, values='hours', names='author_name',
                           title=f"Total Worklog Distribution by Team Member")
            else:
                fig = px.bar(period_person, x='period_str', y='hours', color='author_name',
                            title=f"{title} by Team Member", barmode='stack')
            
            total_hours = wl_df['hours'].sum()
            avg_per_period = period_person.groupby('period_str')['hours'].sum().mean()
            top_contributor = period_person.groupby('author_name')['hours'].sum().idxmax()
            
            response = f"Worklog breakdown ({time_period}): {total_hours:.1f} total hours. "
            response += f"Average {avg_per_period:.1f}h per {time_period[:-2] if time_period.endswith('ly') else time_period}. "
            response += f"Top contributor: {top_contributor}. "
            
        else:
            # Period-only breakdown
            period_summary = wl_df.groupby(group_col)['hours'].agg(['sum', 'count', 'mean']).reset_index()
            period_summary['period_str'] = format_period_labels(period_summary[group_col], time_period)
            
            if chart_preference == "Line Chart":
                fig = px.line(period_summary, x='period_str', y='sum',
                            title=title, markers=True)
            elif chart_preference == "Pie Chart":
                fig = px.pie(period_summary, values='sum', names='period_str',
                           title=f"{title} - Distribution by Period")
            else:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=period_summary['period_str'], y=period_summary['sum'], 
                                   name='Total Hours', marker_color='lightblue'))
                fig.add_trace(go.Scatter(x=period_summary['period_str'], y=period_summary['count'], 
                                       mode='lines+markers', name='Log Entries', yaxis='y2', line=dict(color='red')))
                
                fig.update_layout(
                    title=title,
                    yaxis=dict(title="Hours"),
                    yaxis2=dict(title="Log Entries", overlaying='y', side='right')
                )
            
            total_hours = period_summary['sum'].sum()
            avg_hours = period_summary['sum'].mean()
            peak_period = period_summary.loc[period_summary['sum'].idxmax(), 'period_str']
            
            response = f"Worklog analysis ({time_period}): {total_hours:.1f} total hours. "
            response += f"Average {avg_hours:.1f}h per {time_period[:-2] if time_period.endswith('ly') else time_period}. "
            response += f"Peak period: {peak_period}. "
        
        # Add intelligent insights
        if len(period_summary) > 1:
            trend = "increasing" if period_summary['sum'].iloc[-1] > period_summary['sum'].iloc[0] else "decreasing"
            response += f"Trend: {trend}. "
        
        return fig, response
        
    except Exception as e:
        return None, f"Worklog analysis error: {str(e)}"

def create_pdf_report(title, content, charts=None):
    """Create PDF report from analysis results"""
    if not PDF_AVAILABLE:
        st.error("PDF generation not available. Please install reportlab: pip install reportlab")
        return None
    
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=20,
            spaceAfter=30,
            textColor=colors.HexColor('#1f77b4'),
            alignment=1  # Center alignment
        )
        story.append(Paragraph(title, title_style))
        story.append(Spacer(1, 20))
        
        # Content - clean the content for PDF
        content_clean = content.replace('\n', '<br/>')
        content_clean = content_clean.replace('**', '<b>').replace('**', '</b>')
        
        content_style = ParagraphStyle(
            'CustomContent',
            parent=styles['Normal'],
            fontSize=12,
            spaceAfter=12,
            leading=16
        )
        story.append(Paragraph(content_clean, content_style))
        story.append(Spacer(1, 20))
        
        # Add charts if provided
        if charts:
            for i, chart in enumerate(charts):
                if chart is not None:
                    try:
                        # Convert plotly chart to image
                        img_bytes = pio.to_image(chart, format="png", width=800, height=500, scale=2)
                        
                        # Create image for PDF
                        img = Image(io.BytesIO(img_bytes))
                        img.drawWidth = 6*inch
                        img.drawHeight = 4*inch
                        
                        story.append(img)
                        story.append(Spacer(1, 15))
                        
                    except Exception as e:
                        # If chart conversion fails, add a placeholder
                        error_text = f"Chart {i+1}: Unable to render visualization ({str(e)})"
                        story.append(Paragraph(error_text, styles['Italic']))
                        story.append(Spacer(1, 12))
        
        # Add metadata section
        story.append(Spacer(1, 30))
        
        # Add a line separator
        line_style = ParagraphStyle(
            'Line',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.grey
        )
        story.append(Paragraph('_' * 80, line_style))
        story.append(Spacer(1, 10))
        
        # Add timestamp and metadata
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta_style = ParagraphStyle(
            'Metadata',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.grey,
            alignment=1  # Center alignment
        )
        
        story.append(Paragraph(f"Generated on: {timestamp}", meta_style))
        story.append(Paragraph("AI Jira Reporting Agent", meta_style))
        
        # Build the PDF
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
        
    except Exception as e:
        st.error(f"PDF generation error: {str(e)}")
        return None
    
def format_period_labels(period_series, time_period):
    """Format period labels with full month names and proper formatting"""
    try:
        if time_period == 'monthly':
            # Convert to full month names (Jan 2025, Feb 2025, etc.)
            return period_series.apply(lambda x: x.strftime('%b %Y') if hasattr(x, 'strftime') else str(x))
        elif time_period == 'quarterly':
            # Convert to quarter format (Q1 2025, Q2 2025, etc.)
            return period_series.apply(lambda x: f"Q{x.quarter} {x.year}" if hasattr(x, 'quarter') else str(x))
        elif time_period == 'yearly':
            # Convert to year format (2025, 2026, etc.)
            return period_series.apply(lambda x: str(x.year) if hasattr(x, 'year') else str(x))
        elif time_period == 'weekly':
            # Convert to week format (Week 1 2025, Week 2 2025, etc.)
            def format_week(x):
                if hasattr(x, 'week') and hasattr(x, 'year'):
                    return f"Week {x.week} {x.year}"
                elif hasattr(x, 'start_time'):
                    return f"Week {x.start_time.isocalendar().week} {x.start_time.year}"
                else:
                    return str(x)
            return period_series.apply(format_week)
        else:
            return period_series.astype(str)
    except Exception as e:
        # Fallback to string conversion if formatting fails
        return period_series.astype(str)

def analyze_workload_intelligence(df, question, entities):
    """Advanced workload analysis with period intelligence and status filtering"""
    try:
        original_count = len(df)
        
        # Handle the specific case of "workloads for Done Issues"
        if any(word in question for word in ['done', 'completed', 'finished', 'resolved', 'closed']):
            # Filter for completed issues first
            done_statuses = ['Done', 'Resolved', 'Closed', 'Complete', 'Completed', 'Finished']
            if 'status' in df.columns:
                df = df[df['status'].isin(done_statuses)]
            
            if df.empty:
                return None, f"No completed issues found for workload analysis. Original dataset had {original_count} issues, but none were in Done/Resolved/Closed status."
        
        # Get time period from entities or question
        time_period = entities.get('time_period', 'monthly')
        
        # Enhance workload analysis based on available data
        if 'worklogs_df' in st.session_state.jira_data and not st.session_state.jira_data['worklogs_df'].empty:
            # Use actual worklog data for true workload analysis
            return analyze_worklog_intelligence(df, question, entities)
        else:
            # Use issue count as workload proxy
            return analyze_issue_workload_by_period(df, question, entities, time_period)
        
    except Exception as e:
        return None, f"Workload analysis error: {str(e)}"

def analyze_issue_workload_by_period(df, question, entities, time_period):
    """Analyze workload using issue counts by period"""
    try:
        if 'created_dt' not in df.columns:
            return None, "Date information required for workload analysis."
        
        df_clean = df.dropna(subset=['created_dt'])
        
        if df_clean.empty:
            return None, "No data available for workload analysis after filtering."
        
        # Create period groupings
        if time_period == 'monthly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('M')
            title = "Monthly Workload Analysis"
            period_label = "Month"
        elif time_period == 'weekly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('W')
            title = "Weekly Workload Analysis"
            period_label = "Week"
        elif time_period == 'quarterly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('Q')
            title = "Quarterly Workload Analysis"
            period_label = "Quarter"
        elif time_period == 'yearly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('Y')
            title = "Yearly Workload Analysis"
            period_label = "Year"
        else:
            df_clean['period'] = df_clean['created_dt'].dt.date
            title = "Daily Workload Analysis"
            period_label = "Date"
        
        chart_preference = entities.get('chart_preference', 'Auto')
        
        # Multi-dimensional workload analysis
        if 'assignee' in df_clean.columns and any(word in question for word in ['by person', 'per person', 'team member', 'assignee']):
            # Period + Person workload breakdown
            workload_analysis = df_clean.groupby(['period', 'assignee']).agg({
                'key': 'count',
                'priority': lambda x: (x.isin(['High', 'Critical'])).sum() if 'priority' in df_clean.columns else 0
            }).rename(columns={'key': 'issue_count', 'priority': 'high_priority_count'})
            
            workload_analysis = workload_analysis.reset_index()
            workload_analysis['period_str'] = format_period_labels(workload_analysis['period'], time_period)
            
            # Create chart based on preference
            if chart_preference == "Line Chart":
                fig = px.line(workload_analysis, x='period_str', y='issue_count', color='assignee',
                            title=f"{title} by Team Member", markers=True)
            elif chart_preference == "Pie Chart":
                # Aggregate for pie chart
                assignee_totals = workload_analysis.groupby('assignee')['issue_count'].sum().reset_index()
                fig = px.pie(assignee_totals, values='issue_count', names='assignee',
                           title=f"Total Workload by Team Member")
            else:
                # Default to stacked bar
                fig = px.bar(
                    workload_analysis, 
                    x='period_str', 
                    y='issue_count', 
                    color='assignee',
                    title=f"{title} by Team Member",
                    labels={'period_str': period_label, 'issue_count': 'Issues Completed'},
                    barmode='stack'
                )
            
            # Calculate insights
            total_issues = workload_analysis['issue_count'].sum()
            total_periods = workload_analysis['period'].nunique()
            avg_per_period = total_issues / total_periods if total_periods > 0 else 0
            
            # Top contributor analysis
            contributor_totals = workload_analysis.groupby('assignee')['issue_count'].sum()
            top_contributor = contributor_totals.idxmax()
            top_contributor_count = contributor_totals.max()
            
            response = f"Workload Analysis ({time_period}): {total_issues} issues across {total_periods} periods. "
            response += f"Average {avg_per_period:.1f} issues per {period_label.lower()}. "
            response += f"Top contributor: {top_contributor} ({top_contributor_count} issues). "
            
            if 'high_priority_count' in workload_analysis.columns:
                high_priority_total = workload_analysis['high_priority_count'].sum()
                response += f"High priority issues: {high_priority_total} ({(high_priority_total/total_issues*100):.1f}%). "
            
        else:
            # Period-only workload breakdown
            workload_summary = df_clean.groupby('period').agg({
                'key': 'count',
                'assignee': 'nunique',
                'priority': lambda x: (x.isin(['High', 'Critical'])).sum() if 'priority' in df_clean.columns else 0
            }).rename(columns={'key': 'issue_count', 'assignee': 'team_members', 'priority': 'high_priority_count'})
            
            workload_summary = workload_summary.reset_index()
            workload_summary['period_str'] = format_period_labels(workload_summary['period'], time_period)
            workload_summary['avg_per_person'] = workload_summary['issue_count'] / workload_summary['team_members']
            
            # Create chart based on preference
            if chart_preference == "Line Chart":
                fig = px.line(workload_summary, x='period_str', y='issue_count', 
                            title=title, markers=True)
            elif chart_preference == "Pie Chart":
                fig = px.pie(workload_summary, values='issue_count', names='period_str',
                           title=f"{title} - Distribution by Period")
            else:
                # Default to comprehensive chart
                fig = go.Figure()
                
                # Issue count bars
                fig.add_trace(go.Bar(
                    x=workload_summary['period_str'], 
                    y=workload_summary['issue_count'],
                    name='Total Issues',
                    marker_color='lightblue'
                ))
                
                # Average per person line
                fig.add_trace(go.Scatter(
                    x=workload_summary['period_str'], 
                    y=workload_summary['avg_per_person'],
                    mode='lines+markers',
                    name='Avg per Person',
                    yaxis='y2',
                    line=dict(color='red', width=2)
                ))
                
                fig.update_layout(
                    title=title,
                    xaxis_title=period_label,
                    yaxis=dict(title="Total Issues"),
                    yaxis2=dict(title="Average per Person", overlaying='y', side='right'),
                    showlegend=True
                )
            
            # Calculate insights
            total_issues = workload_summary['issue_count'].sum()
            avg_issues_per_period = workload_summary['issue_count'].mean()
            peak_period = workload_summary.loc[workload_summary['issue_count'].idxmax(), 'period_str']
            peak_count = workload_summary['issue_count'].max()
            
            response = f"Workload Analysis ({time_period}): {total_issues} total issues. "
            response += f"Average {avg_issues_per_period:.1f} issues per {period_label.lower()}. "
            response += f"Peak period: {peak_period} ({peak_count} issues). "
            
            if 'high_priority_count' in workload_summary.columns:
                high_priority_total = workload_summary['high_priority_count'].sum()
                response += f"High priority work: {high_priority_total} issues ({(high_priority_total/total_issues*100):.1f}%). "
            
            # Add trend analysis
            if len(workload_summary) > 1:
                recent_avg = workload_summary['issue_count'].tail(3).mean()
                earlier_avg = workload_summary['issue_count'].head(3).mean()
                if recent_avg > earlier_avg * 1.1:
                    response += "📈 Workload trending upward. "
                elif recent_avg < earlier_avg * 0.9:
                    response += "📉 Workload trending downward. "
                else:
                    response += "📊 Workload stable. "
        
        return fig, response
        
    except Exception as e:
        return None, f"Issue workload analysis error: {str(e)}"
    
def analyze_by_period_intelligence(df, question, entities):
    """Intelligent period-based analysis"""
    try:
        time_period = entities.get('time_period', 'monthly')
        target_field = entities.get('target_field', 'status')
        
        if 'created_dt' not in df.columns:
            return None, "Date information required for period analysis."
        
        df_clean = df.dropna(subset=['created_dt'])
        
        # Create period groupings
        if time_period == 'monthly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('M')
        elif time_period == 'weekly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('W')
        elif time_period == 'quarterly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('Q')
        elif time_period == 'yearly':
            df_clean['period'] = df_clean['created_dt'].dt.to_period('Y')
        else:
            df_clean['period'] = df_clean['created_dt'].dt.date
        
        # Multi-dimensional analysis
        if target_field and target_field in df_clean.columns:
            period_field = df_clean.groupby(['period', target_field]).size().reset_index(name='count')
            period_field['period_str'] = period_field['period'].astype(str)
            
            fig = px.bar(period_field, x='period_str', y='count', color=target_field,
                        title=f"{time_period.title()} {target_field.title()} Breakdown", barmode='stack')
            
            # Calculate insights
            total_issues = df_clean.shape[0]
            periods = len(period_field['period'].unique())
            avg_per_period = total_issues / periods
            
            response = f"{time_period.title()} breakdown by {target_field}: {total_issues} total issues across {periods} periods. "
            response += f"Average {avg_per_period:.1f} issues per {time_period[:-2] if time_period.endswith('ly') else time_period}. "
            
        else:
            # Simple period analysis
            period_summary = df_clean.groupby('period').size().reset_index(name='count')
            period_summary['period_str'] = period_summary['period'].astype(str)
            
            fig = px.line(period_summary, x='period_str', y='count',
                         title=f"{time_period.title()} Issue Creation Trend", markers=True)
            
            response = f"{time_period.title()} analysis: {len(period_summary)} periods analyzed. "
            response += f"Average {period_summary['count'].mean():.1f} issues per {time_period[:-2] if time_period.endswith('ly') else time_period}. "
        
        return fig, response
        
    except Exception as e:
        return None, f"Period analysis error: {str(e)}"

def comparative_intelligence(df, question, entities):
    """Advanced comparative analysis"""
    try:
        # Detect what to compare
        compare_fields = []
        if 'priority' in df.columns and 'priority' in question:
            compare_fields.append('priority')
        if 'assignee' in df.columns and any(word in question for word in ['team', 'assignee', 'person']):
            compare_fields.append('assignee')
        if 'issuetype' in df.columns and 'type' in question:
            compare_fields.append('issuetype')
        
        if not compare_fields:
            compare_fields = ['status']  # Default
        
        comparison_results = {}
        
        for field in compare_fields:
            if field in df.columns:
                field_stats = df.groupby(field).agg({
                    'key': 'count',
                    'status': lambda x: (x.isin(['Done', 'Resolved', 'Closed'])).mean() * 100
                }).rename(columns={'key': 'total', 'status': 'completion_rate'})
                
                comparison_results[field] = field_stats
        
        # Create comparison visualization
        if len(comparison_results) == 1:
            field, stats = list(comparison_results.items())[0]
            fig = go.Figure()
            fig.add_trace(go.Bar(x=stats.index, y=stats['total'], name='Total Issues', marker_color='lightblue'))
            fig.add_trace(go.Scatter(x=stats.index, y=stats['completion_rate'], 
                                   mode='lines+markers', name='Completion %', yaxis='y2', line=dict(color='red')))
            
            fig.update_layout(
                title=f"Comparative Analysis: {field.title()}",
                yaxis=dict(title="Total Issues"),
                yaxis2=dict(title="Completion Rate (%)", overlaying='y', side='right')
            )
            
            best_performer = stats['completion_rate'].idxmax()
            response = f"Comparative analysis ({field}): Best performer: {best_performer} ({stats.loc[best_performer, 'completion_rate']:.1f}% completion). "
            response += f"Total range: {stats['total'].min()}-{stats['total'].max()} issues. "
            
        else:
            # Multi-field comparison
            fig = px.scatter(df, x='priority' if 'priority' in df.columns else df.columns[0], 
                           y='assignee' if 'assignee' in df.columns else df.columns[1],
                           title="Multi-dimensional Comparison")
            response = "Multi-field comparison completed. "
        
        return fig, response
        
    except Exception as e:
        return None, f"Comparative analysis error: {str(e)}"
    
def trend_intelligence(df, question, entities):
    """Advanced trend analysis with pattern recognition"""
    try:
        if 'created_dt' not in df.columns:
            return None, "Date information required for trend analysis."
        
        df_clean = df.dropna(subset=['created_dt'])
        
        # Multi-timeframe trend analysis
        daily_trend = df_clean.groupby('created_date').size()
        weekly_trend = df_clean.groupby(df_clean['created_dt'].dt.to_period('W')).size()
        monthly_trend = df_clean.groupby(df_clean['created_dt'].dt.to_period('M')).size()
        
        # Pattern detection
        patterns = detect_patterns(daily_trend)
        
        # Create comprehensive trend visualization
        fig = go.Figure()
        
        # Daily trend
        fig.add_trace(go.Scatter(x=daily_trend.index, y=daily_trend.values, 
                               mode='lines', name='Daily', line=dict(color='lightblue')))
        
        # Weekly trend (resampled to daily for overlay)
        weekly_dates = [period.start_time for period in weekly_trend.index]
        fig.add_trace(go.Scatter(x=weekly_dates, y=weekly_trend.values, 
                               mode='lines+markers', name='Weekly Avg', line=dict(color='red', width=2)))
        
        fig.update_layout(title="Advanced Trend Intelligence", xaxis_title="Date", yaxis_title="Issues")
        
        response = f"Trend analysis: {patterns['description']}. "
        response += f"Daily variance: {daily_trend.std():.1f}. "
        response += f"Weekly stability: {weekly_trend.std()/weekly_trend.mean():.2f} (lower=more stable). "
        
        return fig, response
        
    except Exception as e:
        return None, f"Trend analysis error: {str(e)}"
    

def performance_intelligence(df, question, entities):
    """Advanced performance metrics analysis"""
    try:
        performance_metrics = {}
        
        # Completion rate
        if 'status' in df.columns:
            completion_rate = (df['status'].isin(['Done', 'Resolved', 'Closed'])).mean() * 100
            performance_metrics['Completion Rate'] = f"{completion_rate:.1f}%"
        
        # Team velocity
        if 'created_dt' in df.columns:
            df_clean = df.dropna(subset=['created_dt'])
            daily_velocity = df_clean.groupby('created_date').size().mean()
            performance_metrics['Daily Velocity'] = f"{daily_velocity:.1f} issues/day"
        
        # Resolution efficiency
        if 'resolution_days' in df.columns:
            avg_resolution = df['resolution_days'].mean()
            performance_metrics['Avg Resolution'] = f"{avg_resolution:.1f} days"
        
        # Team productivity
        if 'assignee' in df.columns:
            team_productivity = df.groupby('assignee').size().std()
            performance_metrics['Workload Balance'] = f"{team_productivity:.1f} (lower=better)"
        
        # Create performance dashboard
        metrics_df = pd.DataFrame(list(performance_metrics.items()), columns=['Metric', 'Value'])
        
        fig = go.Figure(data=[
            go.Table(header=dict(values=['Performance Metric', 'Value'],
                                fill_color='paleturquoise',
                                align='left'),
                    cells=dict(values=[metrics_df['Metric'], metrics_df['Value']],
                              fill_color='lavender',
                              align='left'))
        ])
        fig.update_layout(title="Performance Intelligence Dashboard")
        
        response = "Performance analysis: "
        response += " | ".join([f"{k}: {v}" for k, v in performance_metrics.items()])
        
        return fig, response
        
    except Exception as e:
        return None, f"Performance analysis error: {str(e)}"

def metric_intelligence(df, question, entities):
    """Advanced custom metric calculations"""
    try:
        custom_metrics = {}
        
        # Ratio calculations
        if 'bug' in question and 'issuetype' in df.columns:
            bug_ratio = (df['issuetype'].str.contains('Bug', case=False, na=False)).mean() * 100
            custom_metrics['Bug Ratio'] = f"{bug_ratio:.1f}%"
        
        # Average calculations
        if 'average' in question or 'avg' in question:
            if 'age_days' in df.columns:
                avg_age = df['age_days'].mean()
                custom_metrics['Average Age'] = f"{avg_age:.1f} days"
        
        # Median calculations
        if 'median' in question and 'resolution_days' in df.columns:
            median_resolution = df['resolution_days'].median()
            custom_metrics['Median Resolution'] = f"{median_resolution:.1f} days"
        
        # Rate calculations
        if 'rate' in question:
            if 'created_dt' in df.columns:
                df_clean = df.dropna(subset=['created_dt'])
                creation_rate = len(df_clean) / ((df_clean['created_dt'].max() - df_clean['created_dt'].min()).days + 1)
                custom_metrics['Creation Rate'] = f"{creation_rate:.1f} issues/day"
        
        if not custom_metrics:
            # Default metrics
            custom_metrics['Total Issues'] = str(len(df))
            if 'assignee' in df.columns:
                custom_metrics['Team Size'] = str(df['assignee'].nunique())
        
        # Visualization
        if len(custom_metrics) > 1:
            fig = go.Figure(data=[
                go.Table(header=dict(values=['Custom Metric', 'Value']),
                        cells=dict(values=[list(custom_metrics.keys()), list(custom_metrics.values())]))
            ])
        else:
            fig = px.bar(x=list(custom_metrics.keys()), y=[float(v.split()[0]) for v in custom_metrics.values()],
                        title="Custom Metrics")
        
        response = "Custom metrics: " + " | ".join([f"{k}: {v}" for k, v in custom_metrics.items()])
        
        return fig, response
        
    except Exception as e:
        return None, f"Metric analysis error: {str(e)}"

def prediction_intelligence(df, question, entities):
    """Advanced prediction and forecasting"""
    try:
        predictions = {}
        
        # Completion predictions
        if 'when will' in question or 'completion' in question:
            if 'created_dt' in df.columns:
                open_issues = len(df[~df['status'].isin(['Done', 'Resolved', 'Closed'])])
                df_clean = df.dropna(subset=['created_dt'])
                
                if len(df_clean) > 7:
                    recent_velocity = df_clean.groupby('created_date').size().tail(7).mean()
                    if recent_velocity > 0:
                        days_to_completion = open_issues / recent_velocity
                        completion_date = datetime.now() + timedelta(days=days_to_completion)
                        predictions['Estimated Completion'] = completion_date.strftime('%Y-%m-%d')
                        predictions['Days Remaining'] = f"{days_to_completion:.0f} days"
        
        # Trend predictions
        if 'trend' in question or 'forecast' in question:
            if 'created_dt' in df.columns:
                df_clean = df.dropna(subset=['created_dt'])
                daily_counts = df_clean.groupby('created_date').size()
                
                if len(daily_counts) > 14:
                    recent_trend = daily_counts.tail(7).mean() - daily_counts.head(7).mean()
                    trend_direction = "increasing" if recent_trend > 0 else "decreasing"
                    predictions['Trend Direction'] = trend_direction
                    predictions['Trend Magnitude'] = f"{abs(recent_trend):.1f} issues/day change"
        
        # Create prediction visualization
        if predictions:
            pred_df = pd.DataFrame(list(predictions.items()), columns=['Prediction', 'Value'])
            fig = go.Figure(data=[
                go.Table(header=dict(values=['Prediction Type', 'Forecast']),
                        cells=dict(values=[pred_df['Prediction'], pred_df['Value']]))
            ])
            fig.update_layout(title="Predictive Intelligence")
            
            response = "Predictions: " + " | ".join([f"{k}: {v}" for k, v in predictions.items()])
        else:
            fig = None
            response = "Insufficient data for reliable predictions. Need more historical data points."
        
        return fig, response
        
    except Exception as e:
        return None, f"Prediction error: {str(e)}"
    
def smart_general_analysis(df, question, entities=None):
    """Smart general analysis that adapts to available data"""
    try:
        # Determine best analysis based on available data and question context
        question_lower = question.lower()
        
        # Check if filtering was applied
        filter_info = ""
        if entities and entities.get('filters'):
            filters = entities['filters']
            filter_parts = []
            for key, value in filters.items():
                if isinstance(value, list):
                    filter_parts.append(f"{key}: {', '.join(value)}")
                else:
                    filter_parts.append(f"{key}: {value}")
            if filter_parts:
                filter_info = f" (Filtered by: {', '.join(filter_parts)})"
        
        if 'priority' in df.columns and any(word in question_lower for word in ['priority', 'urgent', 'important']):
            priority_counts = df['priority'].value_counts()
            fig = px.sunburst(
                names=list(priority_counts.index) + ['All Issues'],
                parents=['All Issues'] * len(priority_counts) + [''],
                values=list(priority_counts.values) + [priority_counts.sum()],
                title=f"Priority Distribution Analysis{filter_info}"
            )
            high_critical = priority_counts.get('High', 0) + priority_counts.get('Critical', 0)
            response = f"Priority breakdown: {dict(priority_counts)}. Critical/High priority represents {(high_critical / len(df) * 100):.1f}% of workload."
            
        elif 'status' in df.columns:
            status_counts = df['status'].value_counts()
            completion_statuses = ['Done', 'Resolved', 'Closed', 'Complete']
            completed = sum(status_counts.get(status, 0) for status in completion_statuses)
            completion_rate = (completed / len(df) * 100) if len(df) > 0 else 0
            
            colors_map = ['#2E8B57' if status in completion_statuses else '#FF6B6B' for status in status_counts.index]
            fig = px.bar(x=status_counts.index, y=status_counts.values, color=status_counts.index,
                        color_discrete_sequence=colors_map, title=f"Project Status Overview - {completion_rate:.1f}% Complete{filter_info}")
            
            response = f"Status analysis: {completion_rate:.1f}% completion rate ({completed}/{len(df)} issues). "
            response += f"Active work: {len(df) - completed} issues remaining. "
            
        else:
            # Fallback to basic summary
            fig = px.histogram(df, x='assignee' if 'assignee' in df.columns else df.columns[0], 
                             title=f"Data Overview{filter_info}")
            response = f"Data summary: {len(df)} total records analyzed across {df.shape[1]} attributes. "
            
        if filter_info:
            response += f" Analysis applied to filtered dataset{filter_info}."
            
        return fig, response
        
    except Exception as e:
        return None, f"General analysis error: {str(e)}"
    
def enhance_dataframe(df):
    """Enhance dataframe with calculated fields"""
    try:
        # Handle datetime conversion robustly
        if 'created' in df.columns:
            df['created_dt'] = pd.to_datetime(df['created'], errors='coerce', utc=True)
            # Only process rows with valid dates
            valid_dates = df['created_dt'].notna()
            if valid_dates.any():
                df.loc[valid_dates, 'created_date'] = df.loc[valid_dates, 'created_dt'].dt.date
                df.loc[valid_dates, 'created_month'] = df.loc[valid_dates, 'created_dt'].dt.to_period('M')
                df.loc[valid_dates, 'created_year'] = df.loc[valid_dates, 'created_dt'].dt.year
                df.loc[valid_dates, 'created_week'] = df.loc[valid_dates, 'created_dt'].dt.isocalendar().week
                df.loc[valid_dates, 'created_quarter'] = df.loc[valid_dates, 'created_dt'].dt.quarter
        
        if 'updated' in df.columns:
            df['updated_dt'] = pd.to_datetime(df['updated'], errors='coerce', utc=True)
        
        # Calculate resolution times where possible
        if 'resolutiondate' in df.columns and 'created_dt' in df.columns:
            df['resolved_dt'] = pd.to_datetime(df['resolutiondate'], errors='coerce', utc=True)
            valid_resolution = df['resolved_dt'].notna() & df['created_dt'].notna()
            if valid_resolution.any():
                df.loc[valid_resolution, 'resolution_days'] = (df.loc[valid_resolution, 'resolved_dt'] - df.loc[valid_resolution, 'created_dt']).dt.days
                df.loc[valid_resolution, 'resolution_hours'] = (df.loc[valid_resolution, 'resolved_dt'] - df.loc[valid_resolution, 'created_dt']).dt.total_seconds() / 3600
        
        # Age calculation
        if 'created_dt' in df.columns:
            valid_created = df['created_dt'].notna()
            if valid_created.any():
                df.loc[valid_created, 'age_days'] = (pd.Timestamp.now(tz='UTC') - df.loc[valid_created, 'created_dt']).dt.days
            
        return df
    except Exception as e:
        st.warning(f"Data enhancement warning: {str(e)}")
        return df
    
def detect_patterns(series):
    """Detect patterns in time series data"""
    try:
        if len(series) < 7:
            return {'description': 'Insufficient data for pattern detection'}
        
        # Calculate various metrics
        recent_avg = series.tail(7).mean()
        historical_avg = series.mean()
        volatility = series.std() / series.mean()
        
        # Trend detection
        if recent_avg > historical_avg * 1.2:
            trend = "strong upward trend"
        elif recent_avg < historical_avg * 0.8:
            trend = "strong downward trend"
        elif recent_avg > historical_avg * 1.1:
            trend = "moderate upward trend"
        elif recent_avg < historical_avg * 0.9:
            trend = "moderate downward trend"
        else:
            trend = "stable pattern"
        
        # Volatility assessment
        if volatility > 0.5:
            stability = "high volatility"
        elif volatility > 0.3:
            stability = "moderate volatility"
        else:
            stability = "low volatility"
        
        return {
            'description': f"{trend} with {stability}",
            'trend': trend,
            'volatility': volatility,
            'stability': stability
        }
        
    except Exception:
        return {'description': 'Pattern analysis unavailable'}
    
#! Pre-Requisite Functions For the Reports
def get_excluded_epics():
    """Get the list of epics to exclude from DTE Delivery Calendar analysis"""
    return [
        "AIML OPS ENGINEERING",
        "ARA - R&D 2025",
        "TTR DTEDC – Administrative Activities, Additional Assignments & Other",
        "TTR DTEDC – Daily Stand-ups, Team Meetings, Non-Operational Meetings",
        "TTR DTEDC – Operational Trainings, Knowledge Transfers & Shadowing",
        "TTR DTEDC – Personal Development Trainings"
    ]

def filter_dte_delivery_stories(df):
    """Apply standard DTE Delivery Calendar filters"""
    
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()))
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()))
    
    # Filter by project name
    filtered_df = df[df['project_name'] == 'DTE Delivery Calendar'].copy() if 'project_name' in df.columns else df.copy()
    
    # Ensure 'due_date' column is in datetime format
    filtered_df['due_date'] = pd.to_datetime(filtered_df['due_date'], errors='coerce')
    
    #filter by due date 
    filtered_df = filtered_df[(filtered_df['due_date'] >= start_date) & (filtered_df['due_date'] <= end_date)]
    
    # Filter by issue type
    if 'issuetype' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['issuetype'] == 'Story']
    
    # Exclude specific epics
    if 'parent_epic_summary' in filtered_df.columns:
        excluded_epics = get_excluded_epics()
        # Use regex pattern to match epics
        pattern = '|'.join([re.escape(epic) for epic in excluded_epics])
        filtered_df = filtered_df[~filtered_df['parent_epic_summary'].str.contains(pattern, case=False, na=False)]
    
    return filtered_df

def check_missing_due_date(df, end_date):
    """Check if due dates are set correctly for resolved issues"""
    try:
        # Apply DTE filters
        filtered_df = filter_dte_delivery_stories(df)
        
        # Further filter for closed items in the end date month
        if 'closed_date' in filtered_df.columns:
            # Convert dates
            filtered_df['closed_date'] = pd.to_datetime(filtered_df['closed_date'], errors='coerce')
            end_date_month = pd.to_datetime(end_date).to_period('M')
            
            # Filter for issues resolved in the end date month
            resolved_in_month = filtered_df[
                filtered_df['closed_date'].dt.to_period('M') == end_date_month
            ]
            
            # Check for missing due dates
            if 'due_date' in resolved_in_month.columns:
                missing_due_dates = resolved_in_month[
                    resolved_in_month['due_date'].isna() | 
                    (resolved_in_month['due_date'] == '')
                ]
                
                if len(missing_due_dates) > 0:
                    # Return failure with table of issues
                    display_cols = ['key', 'summary', 'parent_epic_summary']
                    available_cols = [col for col in display_cols if col in missing_due_dates.columns]
                    return False, missing_due_dates[available_cols]
                else:
                    return True, "✅ All Due Dates are Set Correctly"
            else:
                return False, "Due date field not found in data"
        else:
            return False, "Resolution date field not found in data"
            
    except Exception as e:
        return False, f"Error in resolved date check: {str(e)}"

def check_missing_closed_date(df, end_date):
    """Check for missing closed dates"""
    try:
        # Apply DTE filters
        filtered_df = filter_dte_delivery_stories(df)
        # Exclude ToDo and Cancelled statuses
        if 'status' in filtered_df.columns:
            filtered_df = filtered_df[~filtered_df['status'].isin(['ToDo', 'To Do', 'Cancelled'])]
        
               # Further filter for closed items in the end date month
        if 'closed_date' in filtered_df.columns:
            # Convert dates
            filtered_df['closed_date'] = pd.to_datetime(filtered_df['closed_date'], errors='coerce')
            end_date_month = pd.to_datetime(end_date).to_period('M')
            
            # Filter for issues resolved in the end date month
            resolved_in_month = filtered_df[
                filtered_df['closed_date'].dt.to_period('M') == end_date_month
            ]
            
            # Check for missing due dates
            if 'due_date' in resolved_in_month.columns:
                missing_closed_dates = resolved_in_month[
                    resolved_in_month['closed_date'].isna() | 
                    (resolved_in_month['closed_date'] == '')
                ]
                
                if len(missing_closed_dates) > 0:
                    # Return failure with table of issues
                    display_cols = ['key', 'summary', 'parent_epic_summary']
                    available_cols = [col for col in display_cols if col in missing_closed_dates.columns]
                    return False, missing_closed_dates[available_cols]
                else:
                    return True, "✅ All Closed Dates are Set Correctly"
            else:
                return False, "Created date field not found in data"
        else:
            return False, "Closed date field not found in data"
            
    except Exception as e:
        return False, f"Error in missing closed date check: {str(e)}"

def check_future_closed_date(df, end_date):
    """Check for future closed dates"""
    try:
        # Apply DTE filters
        filtered_df = filter_dte_delivery_stories(df)
        
        # Check for closed dates greater than end date
        if 'closed_date' in filtered_df.columns:
            # Convert dates
            filtered_df['closed_date'] = pd.to_datetime(filtered_df['closed_date'], errors='coerce')
            end_date_dt = pd.to_datetime(end_date)
            
            # Filter for issues with future closed dates
            future_closed = filtered_df[
                (filtered_df['closed_date'] > end_date_dt) & 
                filtered_df['closed_date'].notna()
            ]
            
            if len(future_closed) > 0:
                # Return failure with table of issues
                display_cols = ['key', 'summary', 'parent_epic_summary']
                available_cols = [col for col in display_cols if col in future_closed.columns]
                return False, future_closed[available_cols]
            else:
                return True, "✅ No Future Closed Dates are Found."
        else:
            return False, "Closed date field not found in data"
            
    except Exception as e:
        return False, f"Error in future closed date check: {str(e)}"
    
def check_open_deliveries(df):
    """Check for Open Deliveries"""
    try:
        # Apply DTE filters
        openDeliveries = filter_dte_delivery_stories(df)
        
        if 'status' in openDeliveries.columns:
            openDeliveries = openDeliveries[openDeliveries['status'].isin(['To Do'])]

        
            if len(openDeliveries) > 0:
                # Return failure with table of issues
                display_cols = ['key', 'summary', 'parent_epic_summary']
                available_cols = [col for col in display_cols if col in openDeliveries.columns]
                return False, openDeliveries[available_cols]            
            else:
                return True, "✅ No Open Deliveries are Found."   
        else:
            return False, "No Open Deliveries found in data"     
    except Exception as e:
        return False, f"Error in Open Deliveries check: {str(e)}"
    
def check_inprogress_deliveries(df):
    """Check for In Progress Deliveries"""
    try:
        # Apply DTE filters
        inProgressDeliveries = filter_dte_delivery_stories(df)
        
        if 'status' in inProgressDeliveries.columns:
            inProgressDeliveries = inProgressDeliveries[inProgressDeliveries['status'].isin(['In Progress'])]

            if len(inProgressDeliveries) > 0:
                # Return failure with table of issues
                display_cols = ['key', 'summary', 'parent_epic_summary']
                available_cols = [col for col in display_cols if col in inProgressDeliveries.columns]
                return False, inProgressDeliveries[available_cols]
            else:
                return True, "✅ No InProgress Deliveries are Found."  
        else:
            return False, "No In Progress Deliveries found in data"                    
    except Exception as e:
        return False, f"Error in InProgress Deliveries check: {str(e)}"
    
def check_cancelled_deliveries(df):
    """Check for Cancelled Deliveries"""
    try:
        # Apply DTE filters
        cancelledDeliveries = filter_dte_delivery_stories(df)
        
        if 'status' in cancelledDeliveries.columns:
            cancelledDeliveries = cancelledDeliveries[cancelledDeliveries['status'].isin(['Cancelled'])]
                # Return failure with table of issues

        
            if len(cancelledDeliveries) > 0:
                display_cols = ['key', 'summary', 'parent_epic_summary']
                available_cols = [col for col in display_cols if col in cancelledDeliveries.columns]
                return False, cancelledDeliveries[available_cols]
            else:
                return True, "✅ No Cancelled Deliveries are Found."   
        else:
            return False, "No Cancelled Deliveries found in data"                   
    except Exception as e:
        return False, f"Error in Cancelled Deliveries check: {str(e)}"

def check_delayed_deliveries(df, end_date):
    """Check for Delayed Deliveries"""
    try:
        # Apply DTE filters
        delayedDeliveries = filter_dte_delivery_stories(df)

        # Ensure date columns are in datetime format and timezone-naive
        delayedDeliveries['due_date'] = pd.to_datetime(
            delayedDeliveries['due_date'], errors='coerce'
        ).dt.tz_localize(None)

        delayedDeliveries['closed_date'] = pd.to_datetime(
            delayedDeliveries['closed_date'], errors='coerce'
        ).dt.tz_localize(None)

        # Ensure end_date is a timezone-naive Timestamp
        end_date = pd.to_datetime(end_date).tz_localize(None)

        if 'status' in delayedDeliveries.columns:
            delayedDeliveries = delayedDeliveries[
                (delayedDeliveries['status'].isin(['Closed', 'Done', 'Completed'])) &
                (
                    (delayedDeliveries['due_date'] < delayedDeliveries['closed_date']) |
                    (delayedDeliveries['closed_date'].isna()) |
                    (delayedDeliveries['closed_date'] == '')
                )
            ]

            # Calculate DelayDays
            delay_closed = (delayedDeliveries['closed_date'] - delayedDeliveries['due_date']).dt.days
            delay_noClosed = pd.Series(end_date - delayedDeliveries['due_date']).dt.days

            delayedDeliveries['DelayDays'] = np.where(
                delayedDeliveries['closed_date'].notna(),
                delay_closed,
                delay_noClosed)
            
            if len(delayedDeliveries) > 0:
                display_cols = ['key', 'summary', 'parent_epic_summary']
                available_cols = [col for col in display_cols if col in delayedDeliveries.columns]
                return False, delayedDeliveries[available_cols + ['DelayDays']]
            else:
                return True, "✅ No Delayed Deliveries are Found."
        else:
            return False, "No Delayed Deliveries found in data"          
    except Exception as e:
        return False, f"Error in Delayed Deliveries check: {str(e)}"

 
#! Actual Sanity Check & Operational Dashboard Methods 
def display_sanity_check_tab():
    """Display the Sanity Check tab"""
    st.header("✔️ Data Quality Checks")
    
    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to run sanity checks.")
        return
    
    df = st.session_state.jira_data['issues_df']
    end_date = st.session_state.get('end_date', datetime.now())
    
    # Check if DTE Delivery Calendar project exists
    if 'project_name' in df.columns:
        dte_exists = 'DTE Delivery Calendar' in df['project_name'].values
        if not dte_exists:
            st.warning("DTE Delivery Calendar project not found in the fetched data. Sanity checks are specific to this project.")
            return
    else:
        st.error("Project name field not found in data. Please ensure data is fetched with expanded fields.")
        return
    
    st.write("Running data quality checks for DTE Delivery Calendar project...")
    
    st.subheader("📊 Data Quality Summary") 
    # Show filtered data statistics
    dte_data = filter_dte_delivery_stories(df)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total DTE Stories", len(dte_data))
    with col2:
        if 'status' in dte_data.columns:
            todo = len(dte_data[dte_data['status'].isin(['To Do'])])
            st.metric("To Do Stories", todo)
        else:
            st.metric("To Do Stories", "N/A")
    with col3:
        if 'status' in dte_data.columns:
            inProgress = len(dte_data[dte_data['status'].isin(['In Progress'])])
            st.metric("In Progress Stories", inProgress)
        else:
            st.metric("In Progress Stories", "N/A")
    with col4:
        if 'status' in dte_data.columns:
            cancelled = len(dte_data[dte_data['status'].isin(['Cancelled'])])
            st.metric("Cancelled Stories", cancelled)
        else:
            st.metric("Cancelled Stories", "N/A")
    with col5:
        if 'status' in dte_data.columns:
            completed = len(dte_data[dte_data['status'].isin(['Done', 'Resolved', 'Closed'])])
            st.metric("Completed Stories", completed)
        else:
            st.metric("Completed Stories", "N/A")

    st.markdown("---")
    # Run checks
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("1️⃣ Resolved Without Due Date Check")
        with st.spinner("Checking resolved dates..."):
            success, result = check_missing_due_date(df, end_date)
            if success:
                st.success(result)
            else:
                st.error("❌ Issues found with missing due dates")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
    
    with col2:
        st.subheader("2️⃣ Missing Closed Date Check")
        with st.spinner("Checking for missing closed dates..."):
            success, result = check_missing_closed_date(df, end_date)
            if success:
                st.success(result)
            else:
                st.error("❌ Issues found with missing closed dates")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
    
    with col3:
        st.subheader("3️⃣ Future Closed Date Check")
        with st.spinner("Checking for future closed dates..."):
            success, result = check_future_closed_date(df, end_date)
            if success:
                st.success(result)
            else:
                st.error("❌ Issues found with future closed dates")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
                    
    st.markdown("---")
    col4, col5, col6 = st.columns(3)
    
    with col4:
        st.subheader("4️⃣ Open Deliveries Check")
        with st.spinner("Checking Open Deliveries..."):
            success, result = check_open_deliveries(df)
            if success:
                st.success(result)
            else:
                st.error("❌ Open Deliveries Found")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
    
    with col5:
        st.subheader("5️⃣ In Progress Deliveries Check")
        with st.spinner("Checking for In Progress Deliveries..."):
            success, result = check_inprogress_deliveries(df)
            if success:
                st.success(result)
            else:
                st.error("❌ In Progress Deliveries Found")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
    
    with col6:
        st.subheader("6️⃣ Cancelled Deliveries Check")
        with st.spinner("Checking for Cancelled Deliveries..."):
            success, result = check_cancelled_deliveries(df)
            if success:
                st.success(result)
            else:
                st.error("❌ Cancelled Deliveries Found")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
                    
    st.markdown("---")
    col7 = st.columns(1)[0]
    
    with col7:
        st.subheader("7️⃣ Delayed Deliveries Check")
        with st.spinner("Checking Delayed Deliveries..."):
            success, result = check_delayed_deliveries(df, end_date)
            if success:
                st.success(result)
            else:
                st.error("❌ Delayed Deliveries Found")
                if isinstance(result, pd.DataFrame):
                    st.dataframe(result, use_container_width=True)
                else:
                    st.write(result)
    
    # Summary section
    st.markdown('---')
    st.subheader("🎟️ All DTE Delivery Calendar Ticket Summary")
    dte_deliveries = filter_dte_delivery_stories(df)
    display_cols = ['issuetype', 'key', 'summary','priority', 'status', 'assignee',  'due_date', 'closed_date'] 
    available_columns = [col for col in display_cols if col in dte_deliveries.columns]
    st.dataframe(dte_deliveries[available_columns], use_container_width=True)

def display_operations_report_tab():
    """Display the Operations Report tab"""
    st.header("📈 Operations Report")
    
    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to generate operations report.")
        return
    
    df = st.session_state.jira_data['issues_df']
    
    # Apply pre-requisite filters
    filtered_df = filter_dte_delivery_stories(df)
    
    # Exclude cancelled status
    if 'status' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['status'] != 'Cancelled']
    
    if len(filtered_df) == 0:
        st.warning("No data available after applying DTE Delivery Calendar filters.")
        return
    
    # Ensure we have the necessary date columns
    if 'start_date' in filtered_df.columns:
        filtered_df['start_dt'] = pd.to_datetime(filtered_df['start_date'], errors='coerce')
    else:
        # Fallback to created date if start_date not available
        if 'created' in filtered_df.columns:
            filtered_df['start_dt'] = pd.to_datetime(filtered_df['created'], errors='coerce')
        else:
            st.error("No start date or created date field found in data.")
            return
    
    if 'resolutiondate' in filtered_df.columns:
        filtered_df['closed_dt'] = pd.to_datetime(filtered_df['resolutiondate'], errors='coerce')
    
    if 'due_date' in filtered_df.columns:
        filtered_df['due_dt'] = pd.to_datetime(filtered_df['due_date'], errors='coerce')
    
    # Monthly Breakdown
    st.subheader("📅 Monthly Story Breakdown")
    
    # Create monthly analysis
    monthly_data = []
    
    # Get unique months from start dates
    filtered_df_clean = filtered_df.dropna(subset=['start_dt'])
    if len(filtered_df_clean) > 0:
        months = filtered_df_clean['start_dt'].dt.to_period('M').unique()
        months = sorted(months)
        
        for month in months:
            # Total stories starting in month
            total_in_month = len(filtered_df_clean[
                filtered_df_clean['start_dt'].dt.to_period('M') == month
            ])
            
            # Stories closed in month
            if 'closed_dt' in filtered_df_clean.columns:
                closed_in_month = len(filtered_df_clean[
                    filtered_df_clean['closed_dt'].dt.to_period('M') == month
                ])
            else:
                closed_in_month = 0
            
            # Calculate percentage
            percentage = (closed_in_month / total_in_month * 100) if total_in_month > 0 else 0
            
            monthly_data.append({
                'Month': month.strftime('%B %Y'),  # Full month name
                'Total Stories': total_in_month,
                'Closed Stories': closed_in_month,
                'Completion %': f"{percentage:.1f}%"
            })
        
        # Display monthly breakdown
        monthly_df = pd.DataFrame(monthly_data)
        
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(monthly_df, use_container_width=True)
        
        with col2:
            # Create visualization
            fig = px.bar(monthly_df, x='Month', y=['Total Stories', 'Closed Stories'],
                        title="Monthly Story Progress", barmode='group')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available with valid start dates.")
    
####### Work on the Operational Tab ####
def analyze_dtedc_deliveries(df, start_date, end_date):
    try:
        # Ensure datetime format
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        filtered_df = filter_dte_delivery_stories(df)

        if 'summary' in filtered_df.columns:
            # Normalize summaries to lowercase for consistent matching
            filtered_df['summary_lower'] = filtered_df['summary'].str.lower()

            # Include rows that:
            # - contain "downstream"
            # - OR do not contain "upstream" at all
            filtered_df = filtered_df[
                filtered_df['summary_lower'].str.contains("downstream", na=False) |
                ~filtered_df['summary_lower'].str.contains("upstream", na=False)
            ]

            # Drop the helper column if no longer needed
            filtered_df.drop(columns=['summary_lower'], inplace=True)

        def rename_parent_based_on_summary(row):
            if pd.isna(row['summary']):
                return row.get('parent_epic_summary', '')
            summary = str(row['summary'])
            if 'DE_DE' in summary:
                return 'DE_DE_SPLIT'
            elif 'IT_IT' in summary:
                return 'EMR IT'
            elif 'UK_UK' in summary:
                return 'EMR UK IMRD'
            else:
                return row.get('parent_epic_summary', '')

        filtered_df['renamed_parent'] = filtered_df.apply(rename_parent_based_on_summary, axis=1)

        de_de_stories = filtered_df[filtered_df['renamed_parent'] == 'DE_DE_SPLIT']
        expanded_rows = []
        for _, row in de_de_stories.iterrows():
            for parent in ['EMR DE MDI', 'EMR DE MIDAS', 'EMR DE PDI', 'EMR DE SBPDS Projection Factors']:
                new_row = row.copy()
                new_row['renamed_parent'] = parent
                new_row['is_split'] = True
                expanded_rows.append(new_row)

        filtered_df = filtered_df[filtered_df['renamed_parent'] != 'DE_DE_SPLIT']
        if expanded_rows:
            expanded_df = pd.DataFrame(expanded_rows)
            filtered_df = pd.concat([filtered_df, expanded_df], ignore_index=True)

        if 'due_date' in filtered_df.columns:
            thismonthstartdate = end_date.replace(day=1).strftime('%Y-%m-%d')
            filtered_df['due_date'] = pd.to_datetime(filtered_df['due_date'], errors='coerce')
            last_month_stories = filtered_df[
                (filtered_df['due_date'] >= thismonthstartdate) & 
                (filtered_df['due_date'] <= end_date)
            ].copy()
        else:
            return {"error": "Due date field not found in data"}

        calculation_df = last_month_stories

        if 'closed_date' in calculation_df.columns:
            calculation_df['closed_date'] = pd.to_datetime(calculation_df['closed_date'], errors='coerce')

        total_in_scope = len(calculation_df)
        cancelled_stories = calculation_df[last_month_stories['status'] == 'Cancelled'] if 'status' in last_month_stories.columns else last_month_stories
        cancelled_count = len(cancelled_stories)
        cancelled_rate = (cancelled_count / total_in_scope * 100) if total_in_scope > 0 else 0
        done_stories = calculation_df[calculation_df['status'] == 'Done'] if 'status' in calculation_df.columns else pd.DataFrame()
        done_count = len(done_stories)
        completion_rate = (done_count / total_in_scope * 100) if total_in_scope > 0 else 0

        if 'closed_date' in calculation_df.columns:
            on_time_stories = calculation_df[
                (calculation_df['status'] == 'Done') &
                (calculation_df['closed_date'].notna()) &
                (calculation_df['due_date'] >= calculation_df['closed_date'])
            ]
            on_time_count = len(on_time_stories)
            on_time_rate = (on_time_count / total_in_scope * 100) if total_in_scope > 0 else 0

            delayed_stories = calculation_df[
                (calculation_df['status'] == 'Done') &
                (calculation_df['closed_date'].notna()) &
                (calculation_df['due_date'] < calculation_df['closed_date'])
            ]
            delayed_count = len(delayed_stories)
            delay_rate = (delayed_count / total_in_scope * 100) if total_in_scope > 0 else 0
        else:
            on_time_count = on_time_rate = delayed_count = delay_rate = 0

        # Define a function to classify delivery status
        def classify_delivery(row):
            if row['status'] == 'Done' and pd.notna(row['closed_date']) and pd.notna(row['due_date']):
                if row['closed_date'] <= row['due_date']:
                    return 'On Time'
                else:
                    return 'Delayed'
            return None

        # Apply the classification
        calculation_df['Delivery_Status'] = calculation_df.apply(classify_delivery, axis=1)

        # Create binary columns
        calculation_df['On Time Deliveries'] = (calculation_df['Delivery_Status'] == 'On Time').astype(int)
        calculation_df['Delayed Deliveries'] = (calculation_df['Delivery_Status'] == 'Delayed').astype(int)
        calculation_df['Total Deliveries'] = calculation_df['On Time Deliveries'] + calculation_df['Delayed Deliveries']

        # Group by renamed_parent and sum the delivery columns
        parent_delivery_summary = calculation_df.groupby('renamed_parent')[['On Time Deliveries', 'Delayed Deliveries', 'Total Deliveries']].sum()
        # Convert to dictionary
        parent_delivery_summary = parent_delivery_summary.to_dict(orient='index')

        parent_breakdown = calculation_df['renamed_parent'].value_counts().to_dict()

        return {
            "analysis_period": f"{end_date.strftime('%B %Y')}",
            "total_in_scope": total_in_scope,
            "metrics": {
                "completion_rate": {
                    "count": done_count,
                    "percentage": round(completion_rate, 1),
                    "description": f"{done_count} of {total_in_scope} Deliveries Completed"
                },
                "cancelled_rate": {
                    "count": cancelled_count,
                    "percentage": round(cancelled_rate, 1),
                    "description": f"{cancelled_count} of {total_in_scope} Deliveries Cancelled"
                },
                "on_time_rate": {
                    "count": on_time_count,
                    "percentage": round(on_time_rate, 1),
                    "description": f"{on_time_count} of {total_in_scope} Deliveries on Time"
                },
                "delay_rate": {
                    "count": delayed_count,
                    "percentage": round(delay_rate, 1),
                    "description": f"{delayed_count} of {total_in_scope} Deliveries Delayed"
                }
            },
            "parent_breakdown": parent_breakdown,
            "parent_delivery_summary" : parent_delivery_summary,
            "data": calculation_df
        }

    except Exception as e:
        return {"error": f"Analysis failed: {str(e)}"}

def display_dtedc_analysis():
    st.header("📊 Operational Delivery Analysis")

    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to run sanity checks.")
        return

    df = st.session_state.jira_data['issues_df']
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()))
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()))

    if 'project_name' in df.columns:
        if 'DTE Delivery Calendar' not in df['project_name'].values:
            st.warning("DTE Delivery Calendar project not found in the fetched data.")
            return
    else:
        st.error("Project name field not found in data.")
        return

    results = analyze_dtedc_deliveries(df, start_date, end_date)

    if "error" in results:
        st.error(f"Analysis Error: {results['error']}")
        return

    st.subheader(f"Analysis Period: {results['analysis_period']}")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Deliveries", results['total_in_scope'])
    with col2:
        st.metric("Completion Rate", f"{results['metrics']['completion_rate']['percentage']}%", f"{results['metrics']['completion_rate']['count']} deliveries")
    with col3:
        st.metric("Cancelled Rate", f"{results['metrics']['cancelled_rate']['percentage']}%", f"{results['metrics']['cancelled_rate']['count']} deliveries")
    with col4:
        st.metric("On Time Rate", f"{results['metrics']['on_time_rate']['percentage']}%", f"{results['metrics']['on_time_rate']['count']} deliveries")
    with col5:
        st.metric("Delay Rate", f"{results['metrics']['delay_rate']['percentage']}%", f"{results['metrics']['delay_rate']['count']} deliveries")

    st.subheader("📈 Delivery Breakdown")
    if results['parent_delivery_summary']:
        
        #parent_df = pd.DataFrame(results['parent_delivery_summary'].items(), columns=['Parent','Total Count'])
        parent_df = pd.DataFrame.from_dict(results['parent_delivery_summary'], orient='index').reset_index()
        parent_df.rename(columns={'index': 'Parent'}, inplace=True)
        parent_df = parent_df[~parent_df['Parent'].str.contains('Weekly Source Jobs', case=False)]
        parent_df['Parent'] = parent_df['Parent'].str.replace('2025', '', regex=False).str.strip()
        parent_df['Percentage'] = (parent_df['Total Deliveries'] / results['total_in_scope'] * 100).round(1)
        parent_df = parent_df.sort_values('Parent', ascending=True)
        
        # Filter out unwanted entries
        filtered_df = parent_df[~parent_df['Parent'].str.contains('Weekly Source Jobs', case=False)]
        
        # Define grouping function
        def map_parent_category(name):
            if 'Alert Engine' in name:
                return 'Alert Engine'
            elif 'AIML' in name:
                return 'AIML'
            elif 'ARA' in name:
                return 'ARA'
            elif 'CDD' in name:
                return 'DataIQ'
            elif 'DataIQ' in name:
                return 'DataIQ'
            elif 'EMR' in name:
                return 'EMR'
            elif 'LPD' in name:
                return 'LPD'
            else:
                return 'Other'

        # Apply grouping
        filtered_df['Group'] = filtered_df['Parent'].apply(map_parent_category)
        # Aggregate counts
        grouped_df = filtered_df.groupby('Group')['Total Deliveries'].sum().reset_index()
        grouped_df.rename(columns={'Group': 'Parent'}, inplace=True)


        col1, col2 = st.columns(2)
        with col1:
            st.header('Delivery Breakdown by Parent')
            st.dataframe(parent_df, use_container_width=True)
        with col2:
            st.header("Operations Delivery Breakdown")
            #fig = px.pie(grouped_df, values='Total Count', names='Parent')
            # Create bar chart
            
            fig = px.bar(
                grouped_df,
                x='Parent',
                y='Total Deliveries',
                text='Total Deliveries',
                color_discrete_sequence=['#0033A0']  # IQVIA blue
            )

            # Update layout with fixed dimensions and larger, clearer text
            fig.update_layout(
                # Fixed dimensions
                width=600,
                height=600,
                autosize=False,  # Prevents automatic resizing
                
                # X-axis styling
                xaxis=dict(
                    tickangle=-45,
                    tickfont=dict(size=14, color='black', family='Arial Black'),
                    title=dict(
                        text='Parent',
                        font=dict(size=16, color='black', family='Arial Black')
                    )
                ),
                
                # Y-axis styling
                yaxis=dict(
                    tickfont=dict(size=14, color='black', family='Arial Black'),
                    title=dict(
                        text='Total Deliveries',
                        font=dict(size=16, color='black', family='Arial Black')
                    ),
                    showgrid=True,
                    gridcolor='lightgray'
                ),
                
                # Layout properties
                plot_bgcolor='white',
                margin=dict(l=80, r=60, t=80, b=120),  # Extra bottom margin for rotated labels
                dragmode=False  # Prevents dragging/resizing
            )
            
            # Update bar text properties: make bold and increase font size
            fig.update_traces(
                textposition='outside',
                textfont=dict(
                    size=18,  # Larger font size for bar labels
                    family="Arial Black, sans-serif",  # Bold font family
                    color='black'
                )
            )
            
            st.plotly_chart(fig)
            
    # Define the data
    data = {
        "Acronym": ["ARA", "AIML", "CDD", "LPD", "EMR", "Alert Engine"],
        "Definition": [
            "Onboarding, either from CDD or a data owner. The data is checked, prepared, and the ETL process is run, followed by automated tests to ensure completion. After integrating and performing QA in DEV, DEMO, and LIVE environments, a 'Go Live' notification is sent to stakeholders once everything is verified.",
            "Client delivery in AIML platform apps like PJ, ST, expert ecosystem, consumer profile for multiple regions like US and X-US (UK, BE, GE, IT, CA etc) and also takes care of Pfizer monthly reports and regular weekly claims refreshes.",
            "Loading of multiple data assets to Central Data Distribution Platform for ARA and OMOP Team.",
            "Involves validating the integrity and accuracy of data files loaded via the ETL Informatica into Oracle databases ensuring it meets QC guidelines and maintain data delivery frequency. Longitudinal Patient Data (LPD) is also a source of data for CDD (DataIQ) that serves OMOP or ARA (E360).",
            "Loading of data from supplier QC monitoring querying triggering of jobs and delivery to internal teams (UK DE IT US).",
            "Data is received from the data scientist team then to be QC'd (trend volume formats etc.) to be delivered to external pharmaceutical clients (11)."
        ]
    }

    # Create a DataFrame
    df = pd.DataFrame(data)

    # Streamlit app
    st.header('Key Definitions of Activities/Delivery')
    st.dataframe(df, use_container_width=True)
    
    # Metrics description table
    metrics_df = pd.DataFrame({
        "Metric": ["Completion Rate", "Cancelled Rate", "On Time Rate", "Delay Rate"],
        "Description": [
            results['metrics']['completion_rate']['description'],
            results['metrics']['cancelled_rate']['description'],
            results['metrics']['on_time_rate']['description'],
            results['metrics']['delay_rate']['description']
        ]
    })

    with st.expander("View Aggregated Metrics"):
        st.dataframe(metrics_df, use_container_width=True)
        
#ProdOps Section   
def filter_dev_star_stories(df):
    """Apply standard Dev Star Calendar filters"""
    # Filter by project name
    filtered_df = df[df['project_name'] == 'The Dev Star'].copy() if 'project_name' in df.columns else df.copy()
    
    # Filter by issue type
    if 'issuetype' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['issuetype'].isin(['Story', 'Research'])]
    return filtered_df


def analyze_the_dev_star(df, start_date, end_date):
    try:
        # Ensure datetime format
        start_date = pd.to_datetime(start_date, utc=True)
        end_date = pd.to_datetime(end_date, utc=True)

        filtered_df = filter_dev_star_stories(df)
        
        if 'resolutiondate' in filtered_df.columns:
            #thismonthstartdate = end_date.replace(day=1).strftime('%Y-%m-%d')
            filtered_df['resolutiondate'] = pd.to_datetime(filtered_df['resolutiondate'], errors='coerce', utc=True)
            last_month_stories = filtered_df[(filtered_df['resolutiondate'] <= end_date)].copy()
        else:
            return {"error": "Resolved date field not found in data"}
        

        calculation_df = last_month_stories
        total_in_scope = len(calculation_df)
        todo_stories = calculation_df[calculation_df['status'] == 'To Do'] if 'status' in calculation_df.columns else pd.DataFrame()
        todo_count = len(todo_stories)
        todo_rate = (todo_count / total_in_scope * 100) if total_in_scope > 0 else 0
        cancelled_stories = calculation_df[last_month_stories['status'] == 'Cancelled'] if 'status' in last_month_stories.columns else last_month_stories
        cancelled_count = len(cancelled_stories)
        cancelled_rate = (cancelled_count / total_in_scope * 100) if total_in_scope > 0 else 0
        inprogress_stories = calculation_df[calculation_df['status'] == 'In Progress'] if 'status' in calculation_df.columns else pd.DataFrame()
        inprogress_count = len(inprogress_stories)
        inprogress_rate = (inprogress_count / total_in_scope * 100) if total_in_scope > 0 else 0
        done_stories = calculation_df[calculation_df['status'] == 'Done'] if 'status' in calculation_df.columns else pd.DataFrame()
        done_count = len(done_stories)
        done_rate = (done_count / total_in_scope * 100) if total_in_scope > 0 else 0
        
        return {
            "analysis_period": f"{end_date.strftime('%B %Y')}",
            "total_in_scope": total_in_scope,
            "metrics": {
                "completion_count": {
                    "count": done_count,
                    "percentage": round(done_rate, 1),
                    "description": f"{done_count} of {total_in_scope} Deliveries Completed"
                },
                "cancelled_count": {
                    "count": cancelled_count,
                    "percentage": round(cancelled_rate, 1),
                    "description": f"{cancelled_count} of {total_in_scope} Deliveries Cancelled"
                },
                "todo_count": {
                    "count": todo_count,
                    "percentage": round(todo_rate, 1),
                    "description": f"{todo_count} of {total_in_scope} Deliveries on Time"
                },
                "inprogress_count": {
                    "count": inprogress_count,
                    "percentage": round(inprogress_rate, 1),
                    "description": f"{inprogress_count} of {total_in_scope} Deliveries Delayed"
                }
            },
            "data": calculation_df,
        }

    except Exception as e:
        return {"error": f"Analysis failed: {str(e)}"}

def extract_latest_sprint_with_dates(sprint_value):
    """Extract the latest sprint name based on the date in the sprint string."""
    
    # If it's a string that looks like a list, parse it
    if isinstance(sprint_value, str) and sprint_value.startswith('['):
        try:
            sprint_value = ast.literal_eval(sprint_value)
        except:
            # Fallback: extract strings between quotes
            matches = re.findall(r"'([^']*)'", sprint_value)
            if matches:
                sprint_value = matches
            else:
                return sprint_value
    
    # Handle non-list values - return as is
    if not isinstance(sprint_value, list):
        return sprint_value
    
    # Handle empty list
    if not sprint_value:
        return ''
    
    # If only one item, return it
    if len(sprint_value) == 1:
        return sprint_value[0]
    
    # For multiple items, find the one with the latest date
    latest_sprint = None
    latest_date_value = -1
    latest_index = 0
    
    for i, sprint in enumerate(sprint_value):
        try:
            # Look for date pattern YYYY.MM in the sprint name
            match = re.search(r'(\d{4})\.(\d{2})', str(sprint))
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
                # Create a comparable date value (YYYYMM)
                date_value = year * 100 + month
                
                # Keep track of the latest date
                if date_value > latest_date_value:
                    latest_date_value = date_value
                    latest_sprint = sprint
                    latest_index = i
        except:
            # If we can't parse the date, skip this sprint
            continue
    
    # Return the latest sprint if we found one with a date
    if latest_sprint is not None:
        return latest_sprint
    
    # Fallback: return the first item if no dates were found
    return sprint_value[0]

def create_advanced_sprint_chart(df, config=None):
    """Advanced sprint chart with sprint dates in x-axis labels."""
    
    # Default configuration
    default_config = {
        'sprint_column': 'Sprint',
        'sprint_start_column':'Sprint Start',
        'sprint_end_column':'Sprint End',
        'epic_column': 'Parent Name',
        'value_column': 'Total Story Points',
        'y_axis_title': 'Sum of Story Points per Sprint',
        'colors': None,
        'show_values': True,
        'show_totals': True,
        'height': 600,
        'width': 1100,
        'date_format': 'auto',
        'sort_epics_by': 'total',
    }
    
    # Merge with provided config
    if config:
        default_config.update(config)
    config = default_config
    
    # Process the data
    df_clean = df.copy()
    
    # Group by clean sprint and epic, aggregating dates
    grouped = df_clean.groupby(['Sprint', config['epic_column']]).agg({
        config['value_column']: 'sum',
        'Sprint Start': 'first',
        'Sprint End': 'first'
    }).reset_index()
    
    # Create pivot table
    pivot_df = grouped.pivot_table(
        index=config['epic_column'],
        columns='Sprint',
        values=config['value_column'],
        fill_value=0
    )
    
    # Get date mapping for x-axis labels
    date_mapping = grouped[['Sprint', 'Sprint Start', 'Sprint End']].drop_duplicates()
    date_dict = {}
    for _, row in date_mapping.iterrows():
        date_dict[row['Sprint']] = {
            'start': row['Sprint Start'],
            'end': row['Sprint End']
        }
    
    # Sort columns by sprint number
    def extract_sprint_sort_key(sprint_name):
        match = re.search(r'(\d{4})\.(\d{2})', str(sprint_name))
        if match:
            year, sprint_num = match.groups()
            return int(year) * 100 + int(sprint_num)
        return 0
    
    sorted_columns = sorted(pivot_df.columns, key=extract_sprint_sort_key)
    pivot_df = pivot_df[sorted_columns]
    
    # Sort epics
    if config['sort_epics_by'] == 'total':
        epic_order = pivot_df.sum(axis=1).sort_values(ascending=False).index.tolist()
    elif config['sort_epics_by'] == 'name':
        epic_order = sorted(pivot_df.index)
    else:
        epic_order = pivot_df.index.tolist()
    
    # Create the chart
    fig = go.Figure()
    
    # Colors
    if config['colors'] is None:
        colors = ['#4E79A7', '#2C5282', '#F28E2B', '#59A14F', '#355E3B', 
                  '#76B7B2', '#E15759', '#AF7AA1', '#9C755F', '#BAB0AC']
    else:
        colors = config['colors']
    
    # Add bars
    for i, epic in enumerate(epic_order):
        values = pivot_df.loc[epic]
        
        if config['show_values']:
            text_values = [str(int(v)) if v > 0 else '' for v in values]
        else:
            text_values = None
        
        fig.add_trace(go.Bar(
            name=epic,
            x=sorted_columns,
            y=values,
            marker_color=colors[i % len(colors)],
            text=text_values,
            textposition='inside',
            textfont=dict(color='white', size=14),
            hovertemplate=f'<b>{epic}</b><br>%{{x}}<br>{config["value_column"]}: %{{y}}<extra></extra>'
        ))
    
    # Add totals
    if config['show_totals']:
        totals = pivot_df.sum(axis=0)
        for sprint in sorted_columns:
            if totals[sprint] > 0:
                fig.add_annotation(
                    x=sprint,
                    y=totals[sprint] * 1.02,
                    text=f'<b>{int(totals[sprint])}</b>',
                    showarrow=False,
                    font=dict(size=18, color='black', weight='bold')
                )
    
    # Create x-axis labels with dates
    xaxis_labels = []
    for sprint in sorted_columns:
        match = re.search(r'(\d{4})\.(\d{2})', str(sprint))
        if match:
            year, month = match.groups()
            sprint_label = f"{year}-{month}"
            
            # Add dates if available
            if sprint in date_dict:
                start_date = date_dict[sprint]['start']
                end_date = date_dict[sprint]['end']
                
                if start_date and end_date:
                    try:
                        # Format dates
                        start_dt = pd.to_datetime(start_date)
                        end_dt = pd.to_datetime(end_date)
                        date_range = f"{start_dt.strftime('%d.%m')} - {end_dt.strftime('%d.%m')}"
                        label = f'<b>{sprint_label}</b><br><span style="font-size:11px">({date_range})</span>'
                    except:
                        label = f'<b>{sprint_label}</b>'
                else:
                    label = f'<b>{sprint_label}</b>'
            else:
                label = f'<b>{sprint_label}</b>'
        else:
            label = str(sprint)
            
        xaxis_labels.append(label)
    
    # Update layout
    fig.update_layout(
        title={
            'text': config['title'],
            'font': dict(size=24, color='#333333'),
            'x': 0.5,
            'xanchor': 'center'
        },
        xaxis=dict(
            tickmode='array',
            tickvals=sorted_columns,
            ticktext=xaxis_labels,
            tickfont=dict(size=12),
            showgrid=False
        ),
        yaxis=dict(
            title={
                'text': config['y_axis_title'],
                'font': dict(size=14)
            },
            tickfont=dict(size=12),
            gridcolor='#E5E5E5',
            showgrid=True
        ),
        barmode='stack',
        plot_bgcolor='white',
        paper_bgcolor='white',
        height=config['height'],
        width=config['width'],
        legend=dict(
            x=1.02,
            y=0.98,
            font=dict(size=12)
        ),
        margin=dict(l=80, r=200, t=80, b=120)  # Increased bottom margin for date labels
    )
    
    return fig

# Extract sprint information more robustly
def extract_sprint_info(sprint_list, field):
        """Extract specific field from sprint list."""
        if not isinstance(sprint_list, list):
            return []
        
        values = []
        for sprint in sprint_list:
            if isinstance(sprint, dict) and field in sprint:
                values.append(sprint[field])
        return values

def display_prodOps_analysis():
    """Display The Dev Star ProdOps Delivery Analysis."""
    st.header("📊 ProdOps Delivery Analysis")

    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to run ProdOps Analysis.")
        return

    df = st.session_state.jira_data['issues_df']
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()),utc=True)
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()),utc=True)
    
    if 'project_name' in df.columns:
        if 'The Dev Star' not in df['project_name'].values:
            st.warning("The Dev Star project not found in the fetched data.")
            return
    else:
        st.error("Project name field not found in data.")
        return
    # Call your analyze_the_dev_star function
    results = analyze_the_dev_star(df, start_date, end_date)

    if "error" in results:
        st.error(f"Analysis Error: {results['error']}")
        return

    # Display analysis period
    st.subheader(f"Analysis Period: {results['analysis_period']}")

    # Display metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Stories", results['total_in_scope'])
    with col2:
        st.metric(
            "To Do Stories", 
            f"{results['metrics']['todo_count']['percentage']}%", 
            f"{results['metrics']['todo_count']['count']} stories"
        )
    with col3:
        st.metric(
            "In Progress Stories", 
            f"{results['metrics']['inprogress_count']['percentage']}%", 
            f"{results['metrics']['inprogress_count']['count']} stories"
        )
    with col4:
        st.metric(
            "Cancelled Stories", 
            f"{results['metrics']['cancelled_count']['percentage']}%", 
            f"{results['metrics']['cancelled_count']['count']} stories"
        )
    with col5:
        st.metric(
            "Complete Stories", 
            f"{results['metrics']['completion_count']['percentage']}%", 
            f"{results['metrics']['completion_count']['count']} stories",
            delta_color="inverse"
        )

    # Process sprint data
    st.subheader('Sprint Analysis')

    # Create a copy of the data
    finaldf = results['data'].copy()
    
    # Extract sprint information
    finaldf['sprintName'] = finaldf['sprint'].apply(lambda x: extract_sprint_info(x, 'name'))
    finaldf['sprintStart'] = finaldf['sprint'].apply(lambda x: extract_sprint_info(x, 'startDate'))
    finaldf['sprintEnd'] = finaldf['sprint'].apply(lambda x: extract_sprint_info(x, 'endDate'))
    
    # Convert lists to strings for display
    finaldf['parent_epic_summary'] = finaldf['parent_epic_summary'].apply(
        lambda x: str(x) if isinstance(x, list) else x
    )
    finaldf['sprintName'] = finaldf['sprintName'].apply(
        lambda x: str(x) if isinstance(x, list) else x
    )
    
    # Filter out rows with empty sprint names
    finaldf = finaldf[finaldf['sprintName'] != '[]']
    finaldf = finaldf[finaldf['sprintName'] != '']
    
    #Filter only sprints where sprint start is between the start date and end date  
    finaldf['sprintStart'] = finaldf['sprintStart'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else pd.NaT)
    finaldf['sprintEnd'] = finaldf['sprintEnd'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else pd.NaT)
    
    finaldf['sprintStart'] = pd.to_datetime(finaldf['sprintStart'], errors='coerce', utc=True)
    finaldf['sprintEnd'] = pd.to_datetime(finaldf['sprintEnd'], errors='coerce', utc=True)

    finaldf = finaldf[((finaldf['sprintStart'] >= start_date) & (finaldf['sprintStart'] <= end_date))]
    finaldf = finaldf[((finaldf['sprintEnd'] >= start_date) & (finaldf['sprintEnd'] <= end_date))]
    
    # Display data and chart
    col1 = st.columns(1)[0]
    
    with col1:
        # Group by sprint and epic, summing story points
        calculation_df = finaldf.groupby(
            ['sprintName', 'parent_epic_summary', 'sprintStart', 'sprintEnd'], 
            as_index=False
        )['story_points'].sum()
        
        # Display the grouped data
        st.subheader("Story Points by Sprint and Epic")
        display_df = calculation_df.copy()
        
        display_df['sprintStart'] = pd.to_datetime(display_df['sprintStart']).dt.date
        display_df['sprintEnd'] = pd.to_datetime(display_df['sprintEnd']).dt.date

        # Remove rows where parent_epic_summary is empty, None, NaN, or empty string
        display_df = display_df[
            (display_df['parent_epic_summary'].notna()) & 
            (display_df['parent_epic_summary'] != '') &
            (display_df['parent_epic_summary'] != '[]') &
            (display_df['parent_epic_summary'].str.strip() != '') & 
            (display_df['parent_epic_summary'] != 'None')
        ]

        # Rename columns
        display_df.rename(columns={'story_points': 'Total Story Points'}, inplace=True)
        display_df.rename(columns={'parent_epic_summary': 'Parent Name'}, inplace=True)
        display_df.rename(columns={'sprintStart': 'Sprint Start'}, inplace=True)
        display_df.rename(columns={'sprintEnd': 'Sprint End'}, inplace=True)
        display_df.rename(columns={'sprintName': 'Sprint'}, inplace=True)
        
        display_df['Sprint'] = display_df['Sprint'].apply(extract_latest_sprint_with_dates)

        st.dataframe(display_df, use_container_width=True)
        
        st. markdown('----')
        
        st.subheader('Completed Story Points per Sprint')
        # Chart configuration
        config = {'y_axis_title': 'Sum of Story Points per Sprint',
            'colors': ['#4E79A7', '#2C5282', '#F28E2B', '#59A14F', '#355E3B', 
                      '#76B7B2', '#E15759', '#AF7AA1', '#9C755F', '#BAB0AC'],
            'show_totals': True,
            'show_values': True,
            'height': 600,
            'title': '',
            'sprint_column': 'Sprint',
            'sprint_start_column': 'Sprint Start', 
            'sprint_end_column': 'Sprint End',     
            'epic_column': 'Parent Name',
            'value_column': 'Total Story Points'}
        
        # Create and display the chart
        try:
            fig = create_advanced_sprint_chart(display_df, config)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating chart: {str(e)}")
            st.info("Debug: Check if the data has the expected format and values.")
            
            # Show debug information
            with st.expander("Debug Information"):
                st.write("DataFrame shape:", calculation_df.shape)
                st.write("Column names:", calculation_df.columns.tolist())
                st.write("Data types:", calculation_df.dtypes)
                st.write("Sample data:", calculation_df.head())    
            

def filter_hdeps_support_stories(df):
    """Apply Standard HDEPS filters"""
    
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()),utc=True)
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()),utc=True)
    #thismonthstartdate = end_date.replace(day=1).strftime('%Y-%m-%d')
    
    # Filter by project name
    filtered_df = df[df['key'].str.contains('HDEPS', na=False)].copy() if 'project_name' in df.columns else df.copy()
    
    # Ensure 'due_date' column is in datetime format
    filtered_df['resolutiondate'] = pd.to_datetime(filtered_df['resolutiondate'], errors='coerce', utc=True)
    
    #filter by due date 
    filtered_df = filtered_df[(filtered_df['resolutiondate'] >= start_date) & (filtered_df['resolutiondate'] <= end_date)]
    
    # Filter by issue type
    if 'issuetype' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['issuetype'].isin(['[System] Incident', '[System] Service request','[System] Change'])]
        
    # Filter by issue type
    if 'hde_delivery_type' in filtered_df.columns:
        filtered_df = filtered_df[~((filtered_df['issuetype'] == '[System] Change') & 
                            (filtered_df['hde_delivery_type'] == 'ARA'))]
    
    return filtered_df

def create_support_overview(df, start_date, end_date):
    """
    Create support overview with multiple visualizations
    
    Args:
        df: DataFrame with support data
        start_date: Start date for analysis
        end_date: End date for analysis
    """
    
    # Define IQVIA color palette
    IQVIA_COLORS = {
        'blue': '#005587',
        'light_blue': '#4A90E2',
        'bright_blue': '#00B5E2',
        'dark_blue': '#003865',
        'grey': '#7C878E',
        'light_grey': '#A8B5C1',
        'green': '#78BE20',
        'dark_green': '#4A7C59',
        'orange': '#FF8C00',
        'red': '#DC143C'
    }
    
    # Prepare data
    df = filter_hdeps_support_stories(df)
    
    df = prepare_support_data(df)
    
    #working until here.
    # Task 1: Resolved Incidents, Requests & Changes
    task1_fig = create_resolved_items_chart(df, start_date, end_date, IQVIA_COLORS)
    
    # Task 2: Incidents per Project
    task2_fig = create_incidents_per_project_chart(df, start_date, end_date, IQVIA_COLORS)
    
    # Task 3: Incidents by Priority
    task3_fig = create_incidents_by_priority_chart(df, start_date, end_date, IQVIA_COLORS)
    
    # Task 4: Critical Incidents Table
    critical_incidents_df = create_critical_incidents_table(df, end_date)
    
    return {
        'resolved_items_chart': task1_fig,
        'incidents_per_project_chart': task2_fig,
        'incidents_by_priority_chart': task3_fig,
        'critical_incidents_table': critical_incidents_df}

def prepare_support_data(df):
    """Prepare and clean support data"""    
    # Filter by status
    valid_statuses = ['Done', 'Completed', 'Closed', 'Resolved']
    df = df[df['status'].isin(valid_statuses)]
    
    # Handle country mapping
    df = handle_country_mapping(df)
    
    # Add month column
    df['resolution_month'] = df['resolutiondate'].dt.strftime('%B')
    df['resolution_month_num'] = df['resolutiondate'].dt.month
    
    return df

def handle_country_mapping(df):
    """Handle country mapping and renaming"""
    
    # Issue key to country mapping
    issue_country_map = {
        'HDEPS-859': 'ARA', 'HDEPS-857': 'AIML', 'HDEPS-853': 'ARA',
        'HDEPS-848': 'AIML', 'HDEPS-845': 'AIML', 'HDEPS-837': 'AIML',
        'HDEPS-816': 'AIML', 'HDEPS-807': 'AIML', 'HDEPS-784': 'ARA',
        'HDEPS-783': 'ARA', 'HDEPS-781': 'ARA', 'HDEPS-780': 'ARA',
        'HDEPS-957': 'ARA', 'HDEPS-936': 'AIML', 'HDEPS-933': 'ARA',
        'HDEPS-918': 'ARA', 'HDEPS-917': 'AIML', 'HDEPS-874': 'ARA',
        'HDEPS-978': 'AIML', 'HDEPS-984': 'AIML', 'HDEPS-717': 'ARA',
        'HDEPS-719': 'ARA', 'HDEPS-723': 'ARA'
    }
    
    # Country renaming map
    country_rename_map = {
        'DE (Germany)': 'EMR Germany',
        'FR (France)': 'LPD France',
        'IT (Italy)': 'EMR Italy',
        'Multi Country (CDD)': 'DataIQ',
        'UK (MDI)': 'EMR UK',
        'BE (Belgium)': 'LPD Belgium',
        'US (HCPA Lab)': 'EMR US',
        'US (HCPA Lab)': 'EMR US'
    }
    
    # Apply country mapping
    if 'country' not in df.columns:
        # Use reporting_country if country is empty
        if 'reporting_country' in df.columns:
            df['country'] = df['country'].fillna(df['reporting_country'])
    elif 'country' in df.columns and (df['country'].isna().any() or (df['country'] == 'None').any()):
        if 'reporting_country' in df.columns:            
            df['country'] = df['country'].replace('None', pd.NA)
            df['country'] = df['country'].fillna(df['reporting_country'])
    
    # Apply issue-specific country mapping
    for issue_key, country in issue_country_map.items():
        df.loc[df['key'] == issue_key, 'country'] = country
    
    # Rename countries
    df['country'] = df['country'].replace(country_rename_map)
    
    return df

def create_resolved_items_chart(df, start_date, end_date, colors):
    """Task 1: Create resolved incidents, requests and changes chart"""
    
    # Filter out post-incidents
    df_filtered = df[~df['issuetype'].str.contains('Post', case=False, na=False)]
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='ME').strftime('%B').tolist()
    # March hardcoded values
    march_data = {
        'Incidents': 32,
        'Service Requests': 18,
        'Changes': 6
    }

    # Categories to track
    categories = ['Incidents', 'Service Requests', 'Changes']
    
    # Prepare data for plotting
    plot_data = []
    
    for month in months:
        if month == 'March':
            # Use hardcoded March data
            for category, count in march_data.items():
                plot_data.append({
                    'Month': month,
                    'Category': category,
                    'Count': count
                })
        else:
            # Calculate from data
            month_data = df_filtered[df_filtered['resolution_month'] == month]
            for category in categories:
                if category == 'Incidents':
                    count = len(month_data[month_data['issuetype'].str.contains('Incident', case=False, na=False)])
                elif category == 'Service Requests':
                    count = len(month_data[month_data['issuetype'].str.contains('Service Request', case=False, na=False)])
                elif category == 'Changes':
                    count = len(month_data[month_data['issuetype'].str.contains('Change', case=False, na=False)])
                else:
                    count = 0
                
                if count > 0:  # Only add non-zero values
                    plot_data.append({
                        'Month': month,
                        'Category': category,
                        'Count': count
                    })
    
    # Create DataFrame
    plot_df = pd.DataFrame(plot_data)    
    # Create figure
    fig = go.Figure()
    
    # Color mapping for months
    month_colors = [colors['blue'], colors['light_blue'], colors['bright_blue'], colors['grey']]
    
    # Add bars for each month
    for i, month in enumerate(months):
        month_data = plot_df[plot_df['Month'] == month]
        
        fig.add_trace(go.Bar(
            name=month,
            x=month_data['Category'],
            y=month_data['Count'],
            marker_color=month_colors[i % len(month_colors)],
            text=month_data['Count'],
            textposition='outside',
            textfont=dict(size=14, color='black', family='Arial Black'),  # Slightly smaller text
            width=0.15  # Much smaller bars to prevent overlapping
        ))

    # Update layout with proper spacing for grouped bars
    fig.update_layout(
        xaxis=dict(
            tickfont=dict(size=12, color='black', family='Arial Black'),  # Smaller tick font
            categoryorder='array',
            categoryarray=categories,
            title=dict(
                text='',  # Remove axis title to save space
                font=dict(size=12, color='black', family='Arial Black')
            )
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=12, color='black', family='Arial'),
            title=dict(
                text='Count',
                font=dict(size=12, color='black', family='Arial Black')
            )
        ),
        barmode='group',
        bargap=0.4,  # More space between category groups
        bargroupgap=0.05,  # Less space within groups for tighter clustering
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(size=10, color='black', family='Arial')  # Smaller legend font
        ),
        # Fixed dimensions - prevents resizing
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=50, r=40, t=60, b=50),  # Tighter margins for 600px width
        # Ensure fixed sizing in Streamlit
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_incidents_per_project_chart(df, start_date, end_date, colors):
    """Task 2: Create incidents per project horizontal bar chart"""
    
    # Filter incidents only (exclude post-incidents)
    df_incidents = df[
        df['issuetype'].str.contains('Incident', case=False, na=False) & 
        ~df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='ME').strftime('%B').tolist()
    last_month = months[-1]
    
    # March hardcoded values
    march_data = {
        'US (Alert Engine)': 13,
        'EMR Germany': 10,
        'ARA': 6,
        'AIML': 0,
        'DataIQ': 1,
        'EMR Italy': 1,
        'LPD France': 1,
        'US HCP': 0
    }
    
    # Prepare data
    plot_data = []
    country_last_month_count = {}
    
    for month in months:
        if month == 'March':
            # Use hardcoded data
            for country, count in march_data.items():
                if count > 0:  # Only non-zero
                    plot_data.append({
                        'Month': month,
                        'Country': country,
                        'Count': count
                    })
                    if month == last_month:
                        country_last_month_count[country] = count
        else:
            # Calculate from data
            month_data = df_incidents[df_incidents['resolution_month'] == month]
            if 'country' in month_data.columns:
                country_counts = month_data['country'].value_counts()
                
                for country, count in country_counts.items():
                    if count > 0:
                        plot_data.append({
                            'Month': month,
                            'Country': country,
                            'Count': count
                        })
                        if month == last_month:
                            country_last_month_count[country] = count
    
    # Sort countries by last month count
    sorted_countries = sorted(country_last_month_count.keys(), 
                            key=lambda x: country_last_month_count.get(x, 0), 
                            reverse=True)
    
    # Create DataFrame
    plot_df = pd.DataFrame(plot_data)
    
    # Create figure
    fig = go.Figure()
    
    # Color mapping
    month_colors = [colors['blue'], colors['light_blue'], colors['green'], colors['grey']]
    
    # Add bars for each month
    for i, month in enumerate(months):
        month_data = plot_df[plot_df['Month'] == month]
        
        # Order by sorted countries - FIX: Convert Series to dict properly
        ordered_data = []
        for country in sorted_countries:
            country_data = month_data[month_data['Country'] == country]
            if not country_data.empty:
                # Convert Series to dictionary properly
                row = country_data.iloc[0]
                ordered_data.append({
                    'Month': row['Month'],
                    'Country': row['Country'],
                    'Count': row['Count']
                })
            else:
                # Add zero entry for missing data to maintain grouping
                ordered_data.append({
                    'Month': month,
                    'Country': country,
                    'Count': 0
                })
        
        if ordered_data:
            ordered_df = pd.DataFrame(ordered_data)
            
            fig.add_trace(go.Bar(
                name=month,
                y=ordered_df['Country'],
                x=ordered_df['Count'],
                orientation='h',
                marker_color=month_colors[i % len(month_colors)],
                text=[str(count) if count > 0 else '' for count in ordered_df['Count']],  # Hide zero labels
                textposition='outside',
                textfont=dict(size=14, color='black', family='Arial Black'),  # Slightly smaller text
                width=0.15  # Much thinner bars to prevent overlap
            ))
    
    # Update layout with proper grouping
    fig.update_layout(
        yaxis=dict(
            tickfont=dict(size=11, color='black', family='Arial Black'),  # Smaller font
            categoryorder='array',
            categoryarray=sorted_countries[::-1],  # Reverse for proper display
            title=dict(
                text='Projects',
                font=dict(size=12, color='black', family='Arial Black')
            )
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=11, color='black', family='Arial'),
            title=dict(
                text='Incident Count',
                font=dict(size=12, color='black', family='Arial Black')
            ),
            range=[0, max(plot_df['Count']) * 1.3] if len(plot_df) > 0 else [0, 1]  # Handle empty DataFrame
        ),
        barmode='group',  # Critical for proper grouping
        bargap=0.3,  # Space between country groups
        bargroupgap=0.05,  # Minimal space within month groups
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(size=10, color='black', family='Arial')
        ),
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=100, r=70, t=60, b=60),  # Adjusted margins
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_incidents_by_priority_chart(df, start_date, end_date, colors):
    """Task 3: Create incidents by priority horizontal bar chart"""
    
    # Filter incidents only
    df_incidents = df[
        df['issuetype'].str.contains('Incident', case=False, na=False) & 
        ~df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='ME').strftime('%B').tolist()
    
    # March hardcoded values
    march_data = {
        'Critical': 2,
        'High': 12,
        'Medium': 8,
        'Low': 10
    }
    
    # Priority colors
    priority_colors = {
        'Critical': colors['red'],
        'High': colors['orange'],
        'Medium': colors['bright_blue'],
        'Low': colors['dark_blue']
    }
    
    # Priority order
    priority_order = ['Critical', 'High', 'Medium', 'Low']
    
    # Create figure
    fig = go.Figure()
    
    # Create one trace per priority with ALL months data
    for priority in priority_order:
        x_values = []
        y_values = []        
        text_values = []
        
        # Collect data for this priority across all months
        for month in months:
            if month == 'March':
                # Use hardcoded data
                count = march_data.get(priority, 0)
            else:
                # Calculate from data
                month_data = df_incidents[df_incidents['resolution_month'] == month]
                if 'priority' in month_data.columns:
                    count = len(month_data[month_data['priority'] == priority])
                else:
                    count = 0
            
            x_values.append(count)
            y_values.append(month)
            text_values.append(str(count) if count > 0 else '')
        
        # Add one trace for this priority across all months
        fig.add_trace(go.Bar(
            name=priority,
            x=x_values,
            y=y_values,  # All months for each priority
            orientation='h',
            marker_color=priority_colors[priority],
            text=text_values,
            textposition='outside',
            textfont=dict(size=14, color='black', family='Arial Black'),  # Adjusted text size
            width=0.2  # Much thinner bars to prevent overlap
        ))
                
    fig.update_layout(
        yaxis=dict(
            tickfont=dict(size=12, color='black', family='Arial Black'),
            categoryorder='array',
            categoryarray=months[::-1],  # Reverse for proper display
            title=dict(
                text='Months',
                font=dict(size=14, color='black', family='Arial Black')
            )
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=12, color='black', family='Arial'),
            title=dict(
                text='Incident Count',
                font=dict(size=14, color='black', family='Arial Black')
            )
        ),
        barmode='group',  # Groups bars side by side
        plot_bgcolor='white',
        paper_bgcolor='white',
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        bargap=0.4,  # More space between month groups
        bargroupgap=0.05,  # Small space between bars within each group
        legend=dict(
            orientation='v',  # Vertical legend
            yanchor='middle',
            y=0.5,
            xanchor='left',
            x=1.02,  # Position legend to the right
            font=dict(size=11, color='black', family='Arial')
        ),
        margin=dict(l=60, r=120, t=40, b=60),  # More right margin for legend
        showlegend=True,
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_critical_incidents_table(df, end_date):
    """Task 4: Create critical incidents table for last month"""
    
    # Get last month
    last_month = end_date.strftime('%B')

    # Filter critical incidents from last month
    critical_incidents = df[
        (df['resolution_month'] == last_month) &
        (df['priority'] == 'Critical') &
        df['issuetype'].str.contains('Incident', case=False, na=False) &
        ~df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    # Select relevant columns
    table_df = critical_incidents[['key', 'country', 'priority']].copy()
    table_df.columns = ['Key', 'Country', 'Priority']
    
    table_df = table_df.sort_values(by='Key', ascending=True)
    
    return table_df

def display_support_overview():
    """Display support overview in Streamlit"""
    st.header("📊 Support Overview")
    
    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to generate support overview.")
        return
    
    df = st.session_state.jira_data['issues_df']
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()))
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()))
    
    if 'project_name' in df.columns:
        if 'Production & Support' not in df['project_name'].values:
            st.warning("The Production & Support project not found in the fetched data.")
            return
    else:
        st.error("Project name field not found in data.")
        return
    
    # Create support overview
    results = create_support_overview(df, start_date, end_date)
    
    
    # Display Task 1
    st.subheader("📈 Resolved Incidents, Requests and Changes")
    st.plotly_chart(results['resolved_items_chart'], use_container_width=False, width=600, height=600)
    
    
    # Display Task 2
    st.subheader("🌍 Incidents per Project")
    st.plotly_chart(results['incidents_per_project_chart'], use_container_width=False)
    
    # Display Task 3
    st.subheader("🎯 Incidents by Priority")
    st.plotly_chart(results['incidents_by_priority_chart'], use_container_width=False)
    
    # Display Task 4
    st.subheader("🚨 Critical Incidents - Last Month")
    if not results['critical_incidents_table'].empty:
        st.dataframe(results['critical_incidents_table'], use_container_width=True, hide_index=True)
    else:
        st.info("No critical incidents found for the last month.")
    
    # Export functionality
    #if st.button("📥 Export Support Overview Report"):
        # Create PDF or Excel export here
        # st.success("Report exported successfully!")


#customfield_10037  -- Time to first response
#customfield_12635  -- Time to First Repsonse New SLA
#customfield_10036  -- Time to resolution
#customfield_12636  -- Time To Resolution New SLA

#'first_resolution_time':first_resolution_time,
#'first_response_time': first_response_time,
#'first_reponse_goal': first_response_goal,
#'first_resolution_goal':first_resolution_goal,
 
#Resolution & Response Analysis
def create_response_resolution_analysis(df, start_date, end_date):
    """
    Create response and resolution time analysis with multiple visualizations
    
    Args:
        df: DataFrame with support data
        start_date: Start date for analysis
        end_date: End date for analysis
    """
    
    # Prepare data
    df_prepared = prepare_response_resolution_data(df, start_date, end_date)

    # Get last month
    last_month = end_date.strftime('%B')
    # Task 1: First Response Time Overall Result - Last Month
    first_response_table = create_first_response_table(df_prepared, last_month)

    # Task 2: First Response Time Column Chart - All Months
    first_response_column = create_first_response_column_chart(df_prepared, start_date, end_date)
    
    # Task 3: First Response Time Scatter Chart - All Months
    #first_response_scatter_all = create_first_response_scatter_all(df_prepared, start_date, end_date)
    
    # Task 4: First Response Time Scatter Chart - Last Month
    first_response_scatter_month = create_first_response_scatter_month(df_prepared, last_month)
    
    # Task 5: Resolution Time Overall Result - Last Month
    resolution_time_table = create_resolution_time_table(df_prepared, last_month)
    
    # Task 6: Resolution Time Column Chart - All Months
    resolution_time_column = create_resolution_time_column_chart(df_prepared, start_date, end_date)
    
    # Task 7: Resolution Time Scatter Chart - All Months
    #resolution_time_scatter_all = create_resolution_time_scatter_all(df_prepared, start_date, end_date)
    
    # Task 8: Resolution Time Scatter Chart - Last Month
    resolution_time_scatter_month = create_resolution_time_scatter_month(df_prepared, last_month)
    
    return {
        'first_response_table': first_response_table,
        'first_response_column': first_response_column,
        #'first_response_scatter_all': first_response_scatter_all,
        'first_response_scatter_month': first_response_scatter_month,
        'resolution_time_table': resolution_time_table,
        'resolution_time_column': resolution_time_column,
        #'resolution_time_scatter_all': resolution_time_scatter_all,
        'resolution_time_scatter_month': resolution_time_scatter_month
    }

def prepare_response_resolution_data(df, start_date, end_date):
    """Prepare data for response and resolution analysis"""
    
    # Convert resolution date
    df['resolutiondate'] = pd.to_datetime(df.get('resolutiondate', df.get('resolutiondate', '')), errors='coerce', utc=True)
    
    # Filter by date range
    df = df[(df['resolutiondate'] >= start_date) & (df['resolutiondate'] <= end_date)]
    
    # Filter by status
    valid_statuses = ['Done', 'Completed', 'Closed', 'Resolved']
    df = df[df['status'].isin(valid_statuses)]
    
    # Filter incidents only (exclude post-incidents)
    df = df[
        df['issuetype'].str.contains('Incident', case=False, na=False) & 
        ~df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    
    # Add month column
    df['resolution_month'] = df['resolutiondate'].dt.strftime('%B')
    df['resolution_month_num'] = df['resolutiondate'].dt.month
        
    # Convert time fields from milliseconds
    if 'first_response_time' in df.columns:
        df['first_response_time'] = pd.to_numeric(df['first_response_time'], errors='coerce')
        df['first_response_minutes'] = df['first_response_time'] / 1000 / 60
        df['first_response_seconds'] = df['first_response_time'] / 1000
    
    if 'first_response_goal' in df.columns:
        df['first_response_goal'] = pd.to_numeric(df['first_response_goal'], errors='coerce')
        df['first_response_goal_minutes'] = df['first_response_goal'] / 1000 / 60
    
    if 'first_resolution_time' in df.columns:
        df['first_resolution_time'] = pd.to_numeric(df['first_resolution_time'], errors='coerce')
        df['resolution_minutes'] = df['first_resolution_time'] / 1000 / 60
        df['resolution_hours'] = df['first_resolution_time'] / 1000 / 3600
    
    if 'first_resolution_goal' in df.columns:
        df['first_resolution_goal'] = pd.to_numeric(df['first_resolution_goal'], errors='coerce')
        df['resolution_goal_hours'] = df['first_resolution_goal'] / 1000 / 3600
    
    return df

def create_first_response_table(df, last_month):
    """Task 1: Create first response time table for last month"""
    
    # Filter for last month
    df_month = df[df['resolution_month'] == last_month].copy()
    
    if len(df_month) == 0:
        return None
    
    # Calculate SLA metrics
    if 'first_response_time' in df.columns and 'first_response_goal' in df.columns:
        df_month['met_sla'] = df_month['first_response_time'] <= df_month['first_response_goal']
        total_incidents = len(df_month)
        met_sla_count = df_month['met_sla'].sum()
        sla_percentage = (met_sla_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # Get incidents that didn't meet SLA
        failed_sla = df_month[~df_month['met_sla']]['key'].tolist()
    else:
        sla_percentage = 0
        met_sla_count = 0
        total_incidents = len(df_month)
        failed_sla = []
    
    # Calculate average response time
    avg_response_seconds = df_month['first_response_seconds'].mean() if 'first_response_seconds' in df_month.columns else 0
    avg_minutes = int(avg_response_seconds // 60)
    avg_seconds = int(avg_response_seconds % 60)
    
    # Calculate percentage breakdowns
    below_5_min = (df_month['first_response_minutes'] < 5).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    below_10_min = (df_month['first_response_minutes'] < 10).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    below_60_min = (df_month['first_response_minutes'] < 60).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    
    # Create table data
    table_data = {
        'metrics': [
            {
                'Category': 'Overall First Response SLA result',
                'Value': f"{sla_percentage:.0f}% ({met_sla_count} out of {total_incidents} met SLA)"
            },
            {
                'Category': 'Average First Response Time',
                'Value': f"{avg_minutes} mins {avg_seconds:02d} secs"
            },
            {
                'Category': 'Below 5 mins',
                'Value': f"{below_5_min:.0f}%"
            },
            {
                'Category': 'Below 10 mins',
                'Value': f"{below_10_min:.0f}%"
            },
            {
                'Category': 'Below 60 mins',
                'Value': f"{below_60_min:.0f}%"
            }
        ],
        'failed_sla': failed_sla,
        'table_title': f" First Response Time - {last_month}"
    }
    
    return table_data

def create_first_response_column_chart(df, start_date, end_date):
    """Task 2: Create first response time column chart for all months"""
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='ME').strftime('%B').tolist()
    
    # Calculate average per month
    monthly_data = []
    
    for month in months:
        if month == 'March':
            # Hardcoded March value: 35:47
            monthly_data.append({
                'Month': month,
                'Average_Minutes': 35,
                'Average_Seconds': 47,
                'Label': '35m 47s'
            })
        else:
            month_df = df[df['resolution_month'] == month]
            if len(month_df) > 0 and 'first_response_seconds' in month_df.columns:
                avg_seconds = month_df['first_response_seconds'].mean()
                minutes = int(avg_seconds // 60)
                seconds = int(avg_seconds % 60)
                label = f"{minutes}m {seconds:02d}s"
                monthly_data.append({
                    'Month': month,
                    'Average_Minutes': minutes,
                    'Average_Seconds': seconds,
                    'Label': label
                })
    
    # Create DataFrame
    plot_df = pd.DataFrame(monthly_data)
    
    # Create figure
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=plot_df['Month'],
        y=plot_df['Average_Minutes'] + plot_df['Average_Seconds']/60,
        text=plot_df['Label'],
        textposition='outside',
        textfont=dict(size=16, color='black', family='Arial Black'),  # Larger text
        marker_color='darkblue',
        name='Average Response Time',
        width=0.4  # Much thinner bars
    ))
    
    # Update layout with fixed dimensions
    fig.update_layout(
        xaxis=dict(
            tickfont=dict(size=14, color='black', family='Arial Black'),
            title=dict(
                text='Months',
                font=dict(size=14, color='black', family='Arial Black')
            )
        ),
        yaxis=dict(
            title=dict(
                text='Time (minutes)',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            tickfont=dict(size=12, color='black', family='Arial'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        showlegend=False,
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=80, r=60, t=80, b=80),  # Balanced margins for 600x600
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_first_response_scatter_all(df, start_date, end_date):
    """Task 3: Create first response time scatter chart for all months"""
    
    # Sort by resolution date
    df_sorted = df.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_sorted['seq_num'] = range(1, len(df_sorted) + 1)
    
    # Get month boundaries
    month_boundaries = []
    months = []
    
    for month in df_sorted['resolution_month'].unique():
        month_df = df_sorted[df_sorted['resolution_month'] == month]
        if len(month_df) > 0:
            month_boundaries.append({
                'month': month,
                'start': month_df['seq_num'].min(),
                'end': month_df['seq_num'].max()
            })
            months.append(month)
    
    # Create figure
    fig = go.Figure()
    
    # Add scatter plot
    fig.add_trace(go.Scatter(
        x=df_sorted['seq_num'],
        y=df_sorted['first_response_minutes'],
        mode='markers',
        marker=dict(color='blue', size=6),
        showlegend=False
    ))
    
    # Add month dividers
    for i, boundary in enumerate(month_boundaries[:-1]):
        fig.add_vline(
            x=boundary['end'] + 0.5,
            line_width=1,
            line_dash="dash",
            line_color="gray"
        )
    
    # Add month labels
    for boundary in month_boundaries:
        mid_point = (boundary['start'] + boundary['end']) / 2
        fig.add_annotation(
            x=mid_point,
            y=-5,
            text=boundary['month'],
            showarrow=False,
            font=dict(size=11),
            yref='paper',
            yshift=-20
        )
    
    # Update layout
    fig.update_layout(
        xaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            title=''
        ),
        yaxis=dict(
            title='Time (minutes)',
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        height=600,
        margin=dict(b=80)
    )
    
    return fig

def create_first_response_scatter_month(df, last_month):
    """Task 4: Create first response time scatter chart for last month"""
    
    # Filter for last month
    df_month = df[df['resolution_month'] == last_month].copy()
    
    if len(df_month) == 0:
        return None
    
    # Sort by resolution date
    df_month = df_month.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_month['seq_num'] = range(1, len(df_month) + 1)
    
    df_month['first_response_time'] = df['first_response_time'].fillna(0)
    df_month['first_response_goal'] = df['first_response_goal'].fillna(0)
    df_month['first_response_minutes'] = df['first_response_minutes'].fillna(0)
    
    # Determine colors based on SLA
    colors = []
    for _, row in df_month.iterrows():
        if 'first_response_time' in df.columns and 'first_response_goal' in df.columns:
            if row['first_response_time'] > row['first_response_goal']:
                colors.append('red')
            else:
                colors.append('blue')
        else:
            colors.append('blue')
    
    # Create text labels
    text_labels = []
    for _, row in df_month.iterrows():
        label = str(int(round(row['first_response_minutes'])))
        # Add priority if > 20 minutes
        #if row['first_response_minutes'] > 20 and 'priority' in row:
            #label += f" ({row['priority']})"
        text_labels.append(label)
    
    # Create figure
    fig = go.Figure()
    
    # Add scatter plot with bigger dots and text
    fig.add_trace(go.Scatter(
        x=df_month['seq_num'],
        y=df_month['first_response_minutes'],
        mode='markers+text',
        marker=dict(
            color=colors, 
            size=14,  # Much bigger dots (increased from 8)
            line=dict(width=1, color='black')  # Add border for better visibility
        ),
        text=text_labels,
        textposition='top center',
        textfont=dict(
            size=16,  # Much bigger text (increased from 11)
            color='black',
            family='Arial Black'
        ),
        showlegend=False
    ))
    
    # Update layout with fixed dimensions
    fig.update_layout(
        xaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            title='',
            tickfont=dict(size=12, color='black', family='Arial')
        ),
        yaxis=dict(
            title=dict(
                text='Time (minutes)',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            tickfont=dict(size=12, color='black', family='Arial'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=80, r=60, t=80, b=60),  # Proper margins for 600x600
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_resolution_time_table(df, last_month):
    """Task 5: Create resolution time table for last month"""
    
    # Filter for last month and HDEPS project
    df_month = df[
        (df['resolution_month'] == last_month) & 
        (df.get('project', '') == 'HDEPS')
    ].copy()
    
    if len(df_month) == 0:
        # Try without project filter
        df_month = df[df['resolution_month'] == last_month].copy()
    
    if len(df_month) == 0:
        return None
    
    # Calculate SLA metrics
    if 'first_resolution_time' in df.columns and 'first_resolution_goal' in df.columns:
        df_month['met_sla'] = df_month['first_resolution_time'] <= df_month['first_resolution_goal']
        total_incidents = len(df_month)
        met_sla_count = df_month['met_sla'].sum()
        sla_percentage = (met_sla_count / total_incidents * 100) if total_incidents > 0 else 0
        
        # Get incidents that didn't meet SLA
        failed_sla = df_month[~df_month['met_sla']]['key'].tolist()
    else:
        sla_percentage = 0
        met_sla_count = 0
        total_incidents = len(df_month)
        failed_sla = []
    
    # Calculate average resolution time
    avg_hours = df_month['resolution_hours'].mean() if 'resolution_hours' in df_month.columns else 0
    hours = int(avg_hours)
    minutes = int((avg_hours - hours) * 60)
    
    # Calculate percentage breakdowns
    below_4_hours = (df_month['resolution_hours'] < 4).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    below_8_hours = (df_month['resolution_hours'] < 8).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    below_16_hours = (df_month['resolution_hours'] < 16).sum() / len(df_month) * 100 if len(df_month) > 0 else 0
    
    # Create table data
    table_data = {
        'metrics': [
            {
                'Category': 'Overall Resolution Time SLA result',
                'Value': f"{sla_percentage:.0f}% ({met_sla_count} out of {total_incidents} met SLA)"
            },
            {
                'Category': 'Average Resolution Time',
                'Value': f"{hours} hrs {minutes:02d} mins"
            },
            {
                'Category': 'Below 4 hours',
                'Value': f"{below_4_hours:.0f}%"
            },
            {
                'Category': 'Below 8 hours',
                'Value': f"{below_8_hours:.0f}%"
            },
            {
                'Category': 'Below 16 hours',
                'Value': f"{below_16_hours:.0f}%"
            }
        ],
        'failed_sla': failed_sla,
        'table_title': f"Resolution Time - {last_month}"
    }
    
    return table_data

def create_resolution_time_column_chart(df, start_date, end_date):
    """Task 6: Create resolution time column chart for all months"""
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%B').tolist()
    
    # Calculate average per month
    monthly_data = []
    
    for month in months:
        if month == 'March':
            # Hardcoded March value: 8:55
            monthly_data.append({
                'Month': month,
                'Average_Hours': 8.92,  # 8:55 in decimal
                'Label': '8hrs 55mins'
            })
        else:
            month_df = df[df['resolution_month'] == month]
            if len(month_df) > 0 and 'resolution_hours' in month_df.columns:
                avg_hours = month_df['resolution_hours'].mean()
                hours = int(avg_hours)
                minutes = int((avg_hours - hours) * 60)
                label = f"{hours}hrs {minutes:02d}mins"
                monthly_data.append({
                    'Month': month,
                    'Average_Hours': avg_hours,
                    'Label': label
                })
    
    # Create DataFrame
    plot_df = pd.DataFrame(monthly_data)
    
    # Create figure
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=plot_df['Month'],
        y=plot_df['Average_Hours'],
        text=plot_df['Label'],
        textposition='outside',
        textfont=dict(size=16, color='black', family='Arial Black'),  # Larger text
        marker_color='darkblue',
        name='Average Resolution Time',
        width=0.4  # Much thinner bars
    ))
    
    # Update layout with fixed dimensions
    fig.update_layout(
        xaxis=dict(
            tickfont=dict(size=14, color='black', family='Arial Black'),
            title=dict(
                text='Months',
                font=dict(size=14, color='black', family='Arial Black')
            )
        ),
        yaxis=dict(
            title=dict(
                text='Time (hours)',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            tickfont=dict(size=12, color='black', family='Arial'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        showlegend=False,
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=80, r=60, t=80, b=80),  # Balanced margins for 600x600
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def create_resolution_time_scatter_all(df, start_date, end_date):
    """Task 7: Create resolution time scatter chart for all months"""
    
    # Sort by resolution date
    df_sorted = df.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_sorted['seq_num'] = range(1, len(df_sorted) + 1)
    
    # Get month boundaries
    month_boundaries = []
    
    for month in df_sorted['resolution_month'].unique():
        month_df = df_sorted[df_sorted['resolution_month'] == month]
        if len(month_df) > 0:
            month_boundaries.append({
                'month': month,
                'start': month_df['seq_num'].min(),
                'end': month_df['seq_num'].max()
            })
    
    # Create figure
    fig = go.Figure()
    
    # Add scatter plot
    fig.add_trace(go.Scatter(
        x=df_sorted['seq_num'],
        y=df_sorted['resolution_hours'],
        mode='markers',
        marker=dict(color='blue', size=6),
        showlegend=False
    ))
    
    # Add month dividers
    for i, boundary in enumerate(month_boundaries[:-1]):
        fig.add_vline(
            x=boundary['end'] + 0.5,
            line_width=1,
            line_dash="dash",
            line_color="gray"
        )
    
    # Add month labels
    for boundary in month_boundaries:
        mid_point = (boundary['start'] + boundary['end']) / 2
        fig.add_annotation(
            x=mid_point,
            y=-0.5,
            text=boundary['month'],
            showarrow=False,
            font=dict(size=11),
            yref='paper',
            yshift=-20
        )
    
    # Update layout
    fig.update_layout(
        yaxis=dict(
            title='Time (Hours)',
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        height=600,
        margin=dict(b=80)
    )
    
    return fig

def create_resolution_time_scatter_month(df, last_month):
    """Task 8: Create resolution time scatter chart for last month"""
    
    # Filter for last month and HDEPS project
    df_month = df[
        (df['resolution_month'] == last_month) & 
        (df.get('project', '') == 'HDEPS')
    ].copy()
    
    if len(df_month) == 0:
        # Try without project filter
        df_month = df[df['resolution_month'] == last_month].copy()
    
    if len(df_month) == 0:
        return None
    
    # Sort by resolution date
    df_month = df_month.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_month['seq_num'] = range(1, len(df_month) + 1)
    
    df_month['first_resolution_time'] = df['first_resolution_time'].fillna(0)
    df_month['first_resolution_goal'] = df['first_resolution_goal'].fillna(0)
    df_month['resolution_hours'] = df['resolution_hours'].fillna(0)
    
    # Determine colors based on SLA
    colors = []
    for _, row in df_month.iterrows():
        if 'first_resolution_time' in df.columns and 'first_resolution_goal' in df.columns:
            if row['first_resolution_time'] > row['first_resolution_goal']:
                colors.append('red')
            else:
                colors.append('blue')
        else:
            colors.append('blue')
    
    # Create text labels
    text_labels = []
    for _, row in df_month.iterrows():
        label = str(int(round(row['resolution_hours'])))
        # Add priority if > 50 hours
        #if row['resolution_hours'] > 50 and 'priority' in row:
        #   label += f" ({row['priority']})"
        text_labels.append(label)
    
    # Create figure
    fig = go.Figure()
    
    # Add scatter plot with bigger dots and text
    fig.add_trace(go.Scatter(
        x=df_month['seq_num'],
        y=df_month['resolution_hours'],
        mode='markers+text',
        marker=dict(
            color=colors, 
            size=14,  # Much bigger dots (increased from 8)
            line=dict(width=1, color='black')  # Add border for better visibility
        ),
        text=text_labels,
        textposition='top center',
        textfont=dict(
            size=16,  # Much bigger text (increased from 11)
            color='black',
            family='Arial Black'
        ),
        showlegend=False
    ))
    
    # Update layout with fixed dimensions
    fig.update_layout(
        xaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            title='',
            tickfont=dict(size=12, color='black', family='Arial')
        ),
        yaxis=dict(
            title=dict(
                text='Time (Hours)',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            tickfont=dict(size=12, color='black', family='Arial'),
            showgrid=True,
            gridcolor='lightgray'
        ),
        plot_bgcolor='white',
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,  # Prevents automatic resizing
        margin=dict(l=80, r=60, t=80, b=60),  # Proper margins for 600x600
        dragmode=False  # Prevents dragging/resizing
    )
    
    return fig

def display_response_resolution():
    """Display response and resolution analysis in Streamlit"""
    st.header("⏱️ Response and Resolution Time Analysis")
    
    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to generate response and resolution analysis.")
        return
    
    df = st.session_state.jira_data['issues_df']
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()),utc=True)
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()),utc=True)
    month_name = end_date.strftime('%B')
    # Create analysis
    results = create_response_resolution_analysis(df, start_date, end_date)
    
    # Display in tabs
    tab1, tab2 = st.tabs(["First Response Time", "Resolution Time"])
    
    with tab1:
        st.subheader("📊 First Response Time Analysis")
        
        # Task 1: Table
        if results['first_response_table']:
            st.markdown(f"### {results['first_response_table']['table_title']}")
            
            # Display metrics table            
            metrics_df = pd.DataFrame(results['first_response_table']['metrics'])
            def highlight_category_column(val):
                return 'background-color: lightblue'  # or any CSS color

            styled_df = metrics_df.style.applymap(
                highlight_category_column, subset=['Category']
            )

            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Display failed SLA incidents
            if results['first_response_table']['failed_sla']:
                st.markdown("#### Incidents Out of Response Time SLA:")
                failed_df = pd.DataFrame({
                    'Incident Key': results['first_response_table']['failed_sla']
                })
                st.dataframe(failed_df, use_container_width=True, hide_index=True)
        
        # Task 2: Column Chart
        if results['first_response_column']:
            st.markdown("#### First Response time")
            st.plotly_chart(results['first_response_column'], use_container_width=False, width=600, height=600)
        
        # Task 3: Scatter Chart All Months
        #if results['first_response_scatter_all']:
        #    st.plotly_chart(results['first_response_scatter_all'], use_container_width=True)
        
        # Task 4: Scatter Chart Last Month
        if results['first_response_scatter_month']:
            st.markdown(f'#### First Response Time (mins) - [{month_name}]')
            st.plotly_chart(results['first_response_scatter_month'], use_container_width=True)
    
    with tab2:
        st.subheader("📊 Resolution Time Analysis")
        
        # Task 5: Table
        if results['resolution_time_table']:
            st.markdown(f"### {results['resolution_time_table']['table_title']}")
            
            # Display metrics table
            metrics_df = pd.DataFrame(results['resolution_time_table']['metrics'])
            def highlight_category_column(val):
                return 'background-color: lightblue'  # or any CSS color

            styled_df = metrics_df.style.applymap(
                highlight_category_column, subset=['Category']
            )

            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Display failed SLA incidents
            if results['resolution_time_table']['failed_sla']:
                st.markdown("#### Incidents Out of Resolution Time SLA:")
                failed_df = pd.DataFrame({
                    'Incident Key': results['resolution_time_table']['failed_sla']
                })
                st.dataframe(failed_df, use_container_width=True, hide_index=True)
        
        # Task 6: Column Chart
        if results['resolution_time_column']:
            st.markdown("#### First Resolution time")
            st.plotly_chart(results['resolution_time_column'], use_container_width=False, height=600, width=600)
        
        # Task 7: Scatter Chart All Months
        #if results['resolution_time_scatter_all']:
        #    st.markdown(f'#### Resolution Time - YTD')
        #    st.plotly_chart(results['resolution_time_scatter_all'], use_container_width=True)
        
        # Task 8: Scatter Chart Last Month
        if results['resolution_time_scatter_month']:
            st.markdown(f'#### Resolution Time (hours) - {month_name}')
            st.plotly_chart(results['resolution_time_scatter_month'], use_container_width=True)
            
            
#### Cause Code Analysis     
def filter_hdeps_incidents_for_cause_analysis(df, start_date, end_date):
    """Filter HDEPS incidents for cause code analysis"""
    
    # Filter by HDEPS key
    filtered_df = df[df['key'].str.contains('HDEPS', na=False)].copy()
    
    # Convert resolution date
    filtered_df['resolutiondate'] = pd.to_datetime(filtered_df['resolutiondate'], errors='coerce', utc=True)
    
    # Filter by resolution date range
    filtered_df = filtered_df[
        (filtered_df['resolutiondate'] >= start_date) & 
        (filtered_df['resolutiondate'] <= end_date)
    ]
    
    # Filter by status: Done, Completed, Closed
    valid_statuses = ['Done', 'Completed', 'Closed']
    filtered_df = filtered_df[filtered_df['status'].isin(valid_statuses)]
    
    # Present only incidents, exclude post-incidents
    filtered_df = filtered_df[
        filtered_df['issuetype'].str.contains('Incident', case=False, na=False) & 
        ~filtered_df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    
    return filtered_df

def apply_cause_code_country_mapping(df):
    """Apply country mapping for cause code analysis"""
    
    # Issue key to country mapping
    issue_country_map = {
        'HDEPS-859': 'ARA', 'HDEPS-857': 'AIML', 'HDEPS-853': 'ARA',
        'HDEPS-848': 'AIML', 'HDEPS-845': 'AIML', 'HDEPS-837': 'AIML',
        'HDEPS-816': 'AIML', 'HDEPS-807': 'AIML', 'HDEPS-784': 'ARA',
        'HDEPS-783': 'ARA', 'HDEPS-781': 'ARA', 'HDEPS-780': 'ARA',
        'HDEPS-957': 'ARA', 'HDEPS-936': 'AIML', 'HDEPS-933': 'ARA',
        'HDEPS-918': 'ARA', 'HDEPS-917': 'AIML', 'HDEPS-874': 'ARA',
        'HDEPS-978': 'AIML', 'HDEPS-984': 'AIML'
    }
    
    # Country renaming map
    country_rename_map = {
        'DE (Germany)': 'EMR Germany',
        'FR (France)': 'LPD France',
        'IT (Italy)': 'EMR Italy',
        'Multi Country (CDD)': 'DataIQ',
        'UK (MDI)': 'EMR UK',
        'BE (Belgium)': 'LPD Belgium',
        'US (HCPA Lab)': 'US HCP',
        'US (Alert Engine)': 'US AE'
    }
    
    # Step 1: Fill empty Country (AE) with Reporting Country (AC)
    if 'country' in df.columns and 'reporting_country' in df.columns:
        df['country'] = df['country'].fillna(df['reporting_country'])
        df['country'] = df['country'].replace('None', 'ARA')

    
    # Step 2: Apply issue-specific country mapping
    for issue_key, country in issue_country_map.items():
        df.loc[df['key'] == issue_key, 'country'] = country
    
    # Step 3: Rename countries
    if 'country' in df.columns:
        df['country'] = df['country'].replace(country_rename_map)
    
    return df

def create_task1_cause_code_horizontal_chart(df, last_month_name):
    """Task 1: Create horizontal bar chart for incidents by cause code (last month only)"""
    
    # Filter for last month only
    last_month = pd.to_datetime(f"2025-{last_month_name}-01").month
    df_last_month = df[df['resolutiondate'].dt.month == last_month].copy()
    
    if len(df_last_month) == 0:
        return None, "No data available for the last month"
    
    # Count by cause code (Resolution column J) - assuming this maps to a specific field
    # You may need to adjust this field name based on your actual data structure
    cause_code_field = 'ticket_resolution'  # Adjust this field name as needed
    
    if cause_code_field not in df_last_month.columns:
        # If the field doesn't exist, create mock data or use available field
        st.warning(f"Field '{cause_code_field}' not found. Using 'priority' as substitute.")
        cause_code_field = 'priority'
    
    cause_code_counts = df_last_month[cause_code_field].value_counts().sort_values(ascending=True)
    
    # Create horizontal bar chart
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=cause_code_counts.index,
        x=cause_code_counts.values,
        orientation='h',
        text=cause_code_counts.values,
        textposition='outside',
        textfont=dict(size=14, family="Arial Black", color='black'),
        marker_color='steelblue'
    ))
    
    fig.update_layout(
        xaxis_title=dict(text="Count", font=dict(size=14, family="Arial Black")),
        yaxis=dict(tickfont=dict(size=14, family="Arial Black")),
        height=max(400, len(cause_code_counts) * 40),
        showlegend=False,
        plot_bgcolor='white',
        margin=dict(l=200, r=100, t=80, b=50)
    )
    
    return fig
    #f"Task 1 completed: {len(cause_code_counts)} cause code categories analyzed for {last_month_name}"

def create_task2_cause_code_column_chart(df, first_month_name, last_month_name):
    """Task 2: Create column chart grouped by cause code first, then countries within each cause code"""
    
    # Apply country mapping
    df = apply_cause_code_country_mapping(df)
    
    # Use Resolution column U - adjust field name as needed
    cause_code_field = 'ticket_resolution'  # Adjust this field name as needed
    
    if cause_code_field not in df.columns:
        # If the field doesn't exist, use available field
        st.warning(f"Field '{cause_code_field}' not found. Using 'priority' as substitute.")
        cause_code_field = 'priority'
    
    # Get TOP 2 cause codes by count
    top_cause_codes = df[cause_code_field].value_counts().head(2).index.tolist()
    df_top_causes = df[df[cause_code_field].isin(top_cause_codes)]
    
    if len(df_top_causes) == 0:
        return None, "No data available for top cause codes"
    
    # Create the restructured data for plotting
    fig = go.Figure()
    
    # Define colors for cause codes (matching the target image)
    cause_code_colors = {
        top_cause_codes[0]: '#1f4788',  # Dark blue for first cause code (Environment Issue)
        top_cause_codes[1]: '#5DADE2'   # Light blue for second cause code (Data Issue Fix)
    }
    
    # Create x-axis labels and positions
    x_labels = []
    x_positions = []
    current_position = 0
    
    # Process each cause code separately
    for i, cause_code in enumerate(top_cause_codes):
        # Filter data for this cause code
        cause_data = df_top_causes[df_top_causes[cause_code_field] == cause_code]
        country_counts = cause_data['country'].value_counts().sort_values(ascending=False)
        
        # Create positions for this cause code's countries
        cause_positions = list(range(current_position, current_position + len(country_counts)))
        
        # Add bars for this cause code
        fig.add_trace(go.Bar(
            name=cause_code,
            x=cause_positions,
            y=country_counts.values,
            text=country_counts.values,
            textposition='outside',
            textfont=dict(size=14, family="Arial Black", color='black'),
            marker_color=cause_code_colors[cause_code],
            showlegend=True
        ))
        
        # Update labels and positions
        x_labels.extend(country_counts.index.tolist())
        x_positions.extend(cause_positions)
        
        # Add gap between cause codes
        current_position += len(country_counts) + 1
    
    # Create custom x-axis with cause code sections
    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=x_positions,
            ticktext=x_labels,
            tickfont=dict(size=12, family="Arial Black"),
            tickangle=0,
            showgrid=False
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=12)
        ),
        height=600,
        barmode='group',
        plot_bgcolor='white',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.3,
            xanchor='center',
            x=0.5,
            font=dict(size=12)
        ),
        margin=dict(l=50, r=50, t=80, b=150)
    )
    
    # Add cause code section labels at the bottom
    section_starts = []
    section_centers = []
    current_pos = 0
    
    for cause_code in top_cause_codes:
        cause_data = df_top_causes[df_top_causes[cause_code_field] == cause_code]
        country_count = cause_data['country'].nunique()
        
        section_start = current_pos
        section_end = current_pos + country_count - 1
        section_center = (section_start + section_end) / 2
        
        section_centers.append(section_center)
        section_starts.append(section_start)
        
        # Add section label
        fig.add_annotation(
            x=section_center,
            y=-0.15,
            text=f"<b>{cause_code}</b>",
            showarrow=False,
            font=dict(size=14, family="Arial Black", color='gray'),
            yref='paper'
        )
        
        current_pos += country_count + 1
    
    return fig
  #, f"Task 2 completed: Top 2 cause codes ({', '.join(top_cause_codes)}) analyzed with cause-code-first grouping"


def display_cause_code_analysis():
    """Display cause code analysis with Task 1 and Task 2 visualizations"""
    
    st.header("🔍 Cause Code Analysis")
    
    if 'issues_df' not in st.session_state.jira_data:
        st.warning("Please fetch Jira data first to generate cause code analysis.")
        return
    
    df = st.session_state.jira_data['issues_df']
    start_date = pd.to_datetime(st.session_state.get('start_date', datetime.now()), utc=True)
    end_date = pd.to_datetime(st.session_state.get('end_date', datetime.now()), utc=True)
    last_month_name = end_date.strftime('%B')
    first_month_name = start_date.strftime('%B')
    
    # Check if HDEPS data exists
    hdeps_exists = df['key'].str.contains('HDEPS', na=False).any()
    if not hdeps_exists:
        st.warning("No HDEPS incidents found in the fetched data. Cause code analysis requires HDEPS project data.")
        return
    
    # Filter HDEPS incidents
    filtered_df = filter_hdeps_incidents_for_cause_analysis(df, start_date, end_date)
    
    if len(filtered_df) == 0:
        st.warning("No HDEPS incidents found matching the filter criteria (Done/Completed/Closed status, incidents only, within date range).")
        return
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total HDEPS Incidents", len(filtered_df))
    with col2:
        last_month_count = len(filtered_df[filtered_df['resolutiondate'].dt.strftime('%B') == last_month_name])
        st.metric(f"{last_month_name} Incidents", last_month_count)
    with col3:
        unique_countries = filtered_df['country'].nunique() if 'country' in filtered_df.columns else 0
        st.metric("Projects", unique_countries)
    with col4:
        date_range = f"{start_date.strftime('%B')} to {end_date.strftime('%B')}"
        st.metric("Analysis Period", date_range)
    
    st.markdown("---")
    
    # Task 1: Horizontal Bar Chart (Last Month Only)
    st.subheader(f"📊 Incidents by Cause Code - {last_month_name}")
    
    try:
        task1_fig = create_task1_cause_code_horizontal_chart(filtered_df, last_month_name)

        if task1_fig:
            st.plotly_chart(task1_fig, use_container_width=True)
        else:
            st.warning("No data available for Task 1 chart")
        
        #if task1_fig:
        #    st.plotly_chart(task1_fig, use_container_width=True)
        #    st.success(task1_message)
        #else:
        #    st.warning(task1_message)
            
    except Exception as e:
        st.error(f"Error creating Task 1 chart: {str(e)}")
    
    st.markdown("---")
    
    # Task 2: Column Chart by Country (All Months, Top 2 Causes)
    st.subheader(f"📊 Incidents by Country and Cause Code - Top 2 Categories")
    
    try:
        task2_fig = create_task2_cause_code_column_chart(filtered_df, last_month_name, first_month_name)
    
        if task2_fig:
            st.plotly_chart(task2_fig, use_container_width=True)
        else:
            st.warning("No data available for Task 2 chart")
            
        #if task2_fig:
        #    st.plotly_chart(task2_fig, use_container_width=True)
        #    st.success(task2_message)
        #else:
        #    st.warning(task2_message)
            
    except Exception as e:
        st.error(f"Error creating Task 2 chart: {str(e)}")
    
    # Display data summary
    st.markdown("---")
    st.subheader("📋 Data Summary")
    
    # Show country mapping results
    if 'country' in filtered_df.columns:
        with st.expander("View Country Distribution"):
            country_counts = filtered_df['country'].value_counts()
            country_df = pd.DataFrame({
                'Country': country_counts.index,
                'Incident Count': country_counts.values
            })
            st.dataframe(country_df, use_container_width=True)
    
    # Show filtered data
    with st.expander("View Filtered HDEPS Incidents"):
        display_cols = ['key', 'country', 'status', 'priority', 'resolutiondate', 'ticket_resolution']
        available_cols = [col for col in display_cols if col in filtered_df.columns]
        st.dataframe(filtered_df[available_cols], use_container_width=True)       

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        st.info("Please refresh the page and try again. If the issue persists, check your data and configuration.")