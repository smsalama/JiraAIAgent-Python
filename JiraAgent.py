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
from typing import Dict, List, Optional, Any, Tuple, Union
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
import re
import anthropic

# Suppress SSL warnings
warnings.filterwarnings('ignore', message='urllib3 v2 only supports OpenSSL')

# Handle Anthropic import with graceful fallback
try:
    ANTHROPIC_AVAILABLE = True
except ImportError as e:
    ANTHROPIC_AVAILABLE = False
    st.error(f"❌ Anthropic library not found. Please install it using: pip install anthropic")
    st.info("💡 The system will run in basic mode without AI integration.")


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
    layout="centered",
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

###AI Agent Class added here.####
class ClaudeJiraAI:
    """Claude AI-powered intelligent Jira data analysis agent"""
    
    def __init__(self, api_key: str):
        """Initialize Claude AI with API key"""
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-3-5-sonnet-20241022"  # Latest Claude model
        
        # System prompts for different analysis contexts
        self.system_prompts = {
            'data_analyst': """You are an expert Jira data analyst and software development consultant. 
            You analyze development team data to provide actionable insights about:
            - Team performance and productivity
            - Project health and progress
            - Workflow bottlenecks and inefficiencies
            - Quality metrics and trends
            - Resource allocation and workload distribution
            
            Always provide specific, data-driven recommendations that teams can implement immediately.
            Focus on practical solutions and highlight both positive patterns and areas for improvement.""",
            
            'visualization_expert': """You are a data visualization specialist who recommends optimal chart types for different data scenarios.
            Consider factors like:
            - Data type (categorical, numerical, temporal)
            - Number of data points and categories
            - The story the data should tell
            - User's analytical goals
            
            Recommend specific chart types and explain your reasoning.""",
            
            'insight_generator': """You are an AI that generates deep, actionable insights from software development data.
            Look for:
            - Performance patterns and anomalies
            - Productivity trends and correlations
            - Risk indicators and early warnings
            - Optimization opportunities
            - Team dynamics and collaboration patterns
            
            Provide concrete, implementable recommendations with clear business impact."""
        }
    
    def analyze_user_question(self, question: str, data_context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze user question using Claude AI to understand intent and requirements"""
        
        prompt = f"""
        Analyze this user question about Jira development data: "{question}"
        
        Available data context:
        - Total issues: {data_context.get('total_issues', 0)}
        - Available fields: {', '.join(data_context.get('fields', []))}
        - Date range: {data_context.get('date_range', 'Unknown')}
        - Projects: {', '.join(data_context.get('projects', []))}
        - Team size: {data_context.get('team_size', 0)} members
        - Status distribution: {data_context.get('status_summary', {})}
        
        Determine the user's intent and return a JSON response with:
        {{
            "analysis_type": "summary|comparison|trend|breakdown|performance|prediction|correlation|ranking",
            "visualization_type": "bar|pie|line|scatter|heatmap|table|sunburst|treemap|funnel|gauge",
            "primary_field": "main field to analyze",
            "secondary_field": "secondary field for comparison/grouping",
            "grouping_strategy": "how to group the data",
            "time_dimension": "if time-based analysis is needed",
            "filters": {{"field": "value"}},
            "metrics_to_calculate": ["list of metrics to compute"],
            "confidence": 0.0-1.0,
            "explanation": "brief explanation of analysis approach",
            "expected_insights": ["list of insights this analysis should reveal"]
        }}
        
        Focus on what would provide the most valuable insights for a software development team.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                system=self.system_prompts['data_analyst'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract JSON from Claude's response
            content = response.content[0].text
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            
            if json_match:
                return json.loads(json_match.group())
            else:
                # Fallback if JSON extraction fails
                return self._create_fallback_intent(question)
                
        except Exception as e:
            st.error(f"AI analysis error: {str(e)}")
            return self._create_fallback_intent(question)
    
    def generate_data_insights(self, analysis_results: Dict[str, Any], data_summary: Dict[str, Any]) -> str:
        """Generate intelligent insights using Claude AI"""
        
        prompt = f"""
        Analyze this Jira development team data and provide actionable insights:
        
        Data Summary:
        {json.dumps(data_summary, indent=2, default=str)}
        
        Analysis Results:
        {json.dumps(analysis_results, indent=2, default=str)}
        
        Provide a comprehensive analysis with:
        
        ## 🎯 Key Insights
        - 3-5 most important findings about team performance
        - Specific patterns or trends identified
        - Performance benchmarks and comparisons
        
        ## ⚠️ Risks & Concerns
        - Potential bottlenecks or issues to address
        - Quality or productivity concerns
        - Resource allocation problems
        
        ## 🚀 Recommendations
        - Specific, actionable steps to improve performance
        - Process optimizations
        - Tool or workflow suggestions
        
        ## 📈 Positive Patterns
        - What the team is doing well
        - Successful practices to continue
        - Strengths to leverage
        
        Make insights specific to software development teams and focus on practical, implementable recommendations.
        Use emojis and clear formatting for readability.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                system=self.system_prompts['insight_generator'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"**AI Insights:** Unable to generate detailed insights: {str(e)}"
    
    def create_intelligent_summary(self, df: pd.DataFrame, analysis_type: str) -> str:
        """Create an intelligent summary of the data analysis"""
        
        # Prepare data summary for Claude
        data_stats = {
            'total_records': len(df),
            'columns': list(df.columns),
            'date_range': self._get_date_range(df),
            'key_metrics': self._calculate_key_metrics(df)
        }
        
        prompt = f"""
        Create an intelligent summary for this {analysis_type} analysis of Jira data:
        
        Data Statistics:
        {json.dumps(data_stats, indent=2, default=str)}
        
        Provide a concise but insightful summary that:
        1. Explains what the data shows
        2. Highlights the most important findings
        3. Provides context for decision-making
        4. Suggests next steps or follow-up analyses
        
        Keep it focused on software development team insights.
        Format with clear sections and bullet points.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                system=self.system_prompts['data_analyst'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Summary: Analysis of {len(df)} records completed. See visualization for details."
    
    def suggest_follow_up_questions(self, current_question: str, analysis_results: Dict[str, Any], available_fields: List[str]) -> List[str]:
        """Generate intelligent follow-up questions using Claude"""
        
        prompt = f"""
        Based on this Jira data analysis:
        
        Original Question: "{current_question}"
        Analysis Type: {analysis_results.get('analysis_type', 'unknown')}
        Available Data Fields: {', '.join(available_fields)}
        
        Suggest 5 intelligent follow-up questions that would provide deeper insights into:
        - Team performance optimization
        - Process improvement opportunities
        - Risk identification and mitigation
        - Quality and productivity metrics
        - Resource allocation efficiency
        
        Make questions specific, actionable, and focused on software development team needs.
        Return as a simple numbered list.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=600,
                system=self.system_prompts['data_analyst'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            content = response.content[0].text
            # Extract questions from numbered list
            questions = []
            for line in content.split('\n'):
                if re.match(r'^\d+\.?\s+', line.strip()):
                    question = re.sub(r'^\d+\.?\s+', '', line.strip())
                    questions.append(question)
            
            return questions[:5]  # Return max 5 questions
            
        except Exception as e:
            return [
                "How can we improve our team's velocity?",
                "What are the main bottlenecks in our workflow?",
                "Which issues are taking longest to resolve?",
                "How is workload distributed across team members?",
                "What's our bug-to-feature ratio trend?"
            ]
    
    def explain_visualization_choice(self, question: str, chart_type: str, data_context: Dict[str, Any]) -> str:
        """Explain why a specific visualization was chosen"""
        
        prompt = f"""
        Explain why "{chart_type}" is the optimal visualization for this analysis:
        
        User Question: "{question}"
        Data Context: {json.dumps(data_context, default=str)}
        
        Provide a brief, educational explanation of:
        - Why this chart type is best for this data
        - What insights it helps reveal
        - How to interpret the visualization
        - Any limitations or considerations
        
        Keep it concise but informative.
        """
        
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=300,
                system=self.system_prompts['visualization_expert'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Selected {chart_type} chart as optimal for this data analysis."
    
    def _create_fallback_intent(self, question: str) -> Dict[str, Any]:
        """Create fallback analysis intent when Claude is unavailable"""
        question_lower = question.lower()
        
        # Simple keyword-based analysis
        if any(word in question_lower for word in ['trend', 'over time', 'timeline']):
            analysis_type = 'trend'
            viz_type = 'line'
        elif any(word in question_lower for word in ['compare', 'vs', 'versus']):
            analysis_type = 'comparison'
            viz_type = 'bar'
        elif any(word in question_lower for word in ['breakdown', 'distribution', 'pie']):
            analysis_type = 'breakdown'
            viz_type = 'pie'
        elif any(word in question_lower for word in ['performance', 'metrics', 'kpi']):
            analysis_type = 'performance'
            viz_type = 'gauge'
        else:
            analysis_type = 'summary'
            viz_type = 'bar'
        
        return {
            "analysis_type": analysis_type,
            "visualization_type": viz_type,
            "primary_field": "status",
            "secondary_field": None,
            "grouping_strategy": "by_category",
            "time_dimension": None,
            "filters": {},
            "metrics_to_calculate": ["count", "percentage"],
            "confidence": 0.6,
            "explanation": f"Basic {analysis_type} analysis",
            "expected_insights": ["Basic data overview"]
        }
    
    def _get_date_range(self, df: pd.DataFrame) -> str:
        """Extract date range from dataframe"""
        if 'created' in df.columns:
            try:
                dates = pd.to_datetime(df['created'], errors='coerce').dropna()
                if len(dates) > 0:
                    return f"{dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}"
            except:
                pass
        return "Unknown date range"
    
    def _calculate_key_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate key metrics from the dataframe"""
        metrics = {}
        
        try:
            # Basic metrics
            metrics['total_issues'] = len(df)
            
            # Status metrics
            if 'status' in df.columns:
                status_counts = df['status'].value_counts()
                completed_statuses = ['Done', 'Resolved', 'Closed', 'Complete']
                completed = sum(status_counts.get(status, 0) for status in completed_statuses)
                metrics['completion_rate'] = round((completed / len(df)) * 100, 1) if len(df) > 0 else 0
                metrics['status_distribution'] = status_counts.to_dict()
            
            # Team metrics
            if 'assignee' in df.columns:
                metrics['team_size'] = df['assignee'].nunique()
                metrics['avg_issues_per_person'] = round(len(df) / df['assignee'].nunique(), 1) if df['assignee'].nunique() > 0 else 0
            
            # Priority metrics
            if 'priority' in df.columns:
                priority_counts = df['priority'].value_counts()
                high_priority = sum(priority_counts.get(p, 0) for p in ['High', 'Critical', 'Urgent'])
                metrics['high_priority_percentage'] = round((high_priority / len(df)) * 100, 1) if len(df) > 0 else 0
            
            # Time-based metrics
            if 'created' in df.columns:
                df['created_dt'] = pd.to_datetime(df['created'], errors='coerce')
                valid_dates = df['created_dt'].dropna()
                if len(valid_dates) > 0:
                    metrics['date_span_days'] = (valid_dates.max() - valid_dates.min()).days
                    metrics['avg_issues_per_day'] = round(len(valid_dates) / max(metrics['date_span_days'], 1), 2)
            
        except Exception as e:
            metrics['calculation_error'] = str(e)
        
        return metrics

class ClaudeJiraAnalyzer:
    """Main analyzer class that combines Claude AI with data processing and visualization"""
    
    def __init__(self, claude_api_key: str = None):
        """Initialize with optional Claude API key"""
        self.claude_ai = ClaudeJiraAI(claude_api_key) if claude_api_key else None
        self.has_ai = claude_api_key is not None
        
    def analyze_question(self, question: str, df: pd.DataFrame, chart_preference: str = "Auto") -> Tuple[Any, str, Dict[str, Any]]:
        """Analyze question and generate visualization with intelligent insights"""
        
        # Prepare data context
        data_context = self._prepare_data_context(df)
        
        if self.has_ai:
            # Use Claude AI for intelligent analysis
            intent = self.claude_ai.analyze_user_question(question, data_context)
        else:
            # Fallback to basic analysis
            intent = self._basic_intent_analysis(question, chart_preference)
        
        # Override visualization type if user specified preference
        if chart_preference != "Auto":
            intent['visualization_type'] = chart_preference.lower().replace(' chart', '')
        
        # Generate visualization
        chart = self._create_visualization(df, intent)
        
        # Generate analysis summary
        if self.has_ai:
            # Get AI-powered insights
            analysis_results = {
                'intent': intent,
                'data_points': len(df),
                'chart_created': chart is not None
            }
            
            insights = self.claude_ai.generate_data_insights(analysis_results, data_context)
            summary = self.claude_ai.create_intelligent_summary(df, intent['analysis_type'])
            
            # Combine insights and summary
            response = f"{insights}\n\n**Technical Summary:**\n{summary}"
        else:
            # Basic summary
            response = self._create_basic_summary(df, intent)
        
        return chart, response, intent
    
    def get_follow_up_suggestions(self, question: str, intent: Dict[str, Any], df: pd.DataFrame) -> List[str]:
        """Get intelligent follow-up question suggestions"""
        if self.has_ai:
            return self.claude_ai.suggest_follow_up_questions(question, intent, list(df.columns))
        else:
            return self._basic_follow_up_questions(intent['analysis_type'])
    
    def explain_chart(self, question: str, chart_type: str, df: pd.DataFrame) -> str:
        """Explain why a specific chart type was chosen"""
        if self.has_ai:
            data_context = self._prepare_data_context(df)
            return self.claude_ai.explain_visualization_choice(question, chart_type, data_context)
        else:
            return f"Selected {chart_type} chart based on data characteristics and analysis requirements."
    
    def create_data_table(self, df: pd.DataFrame, intent: Dict[str, Any]) -> pd.DataFrame:
        """Create a summarized data table based on analysis intent"""
        
        try:
            primary_field = intent.get('primary_field', 'status')
            secondary_field = intent.get('secondary_field')
            
            if primary_field in df.columns:
                if secondary_field and secondary_field in df.columns:
                    # Two-dimensional analysis
                    summary_table = df.groupby([primary_field, secondary_field]).agg({
                        'key': 'count',
                        'priority': lambda x: (x.isin(['High', 'Critical'])).sum() if 'priority' in df.columns else 0
                    }).rename(columns={'key': 'Count', 'priority': 'High Priority'})
                    
                    summary_table = summary_table.reset_index()
                else:
                    # Single dimension analysis
                    summary_table = df[primary_field].value_counts().reset_index()
                    summary_table.columns = [primary_field.title(), 'Count']
                    
                    # Add percentage
                    summary_table['Percentage'] = (summary_table['Count'] / summary_table['Count'].sum() * 100).round(1)
                
                return summary_table
            else:
                # Fallback: basic summary
                return df.describe(include='all').reset_index()
                
        except Exception as e:
            st.error(f"Error creating data table: {str(e)}")
            return pd.DataFrame({'Error': [str(e)]})
    
    def _prepare_data_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Prepare data context for Claude AI analysis"""
        
        context = {
            'total_issues': len(df),
            'fields': list(df.columns),
            'date_range': self._get_date_range(df),
            'projects': df['project'].unique().tolist() if 'project' in df.columns else [],
            'team_size': df['assignee'].nunique() if 'assignee' in df.columns else 0
        }
        
        # Add status summary
        if 'status' in df.columns:
            context['status_summary'] = df['status'].value_counts().to_dict()
        
        # Add priority summary
        if 'priority' in df.columns:
            context['priority_summary'] = df['priority'].value_counts().to_dict()
        
        return context
    
    def _create_visualization(self, df: pd.DataFrame, intent: Dict[str, Any]) -> Any:
        """Create visualization based on analysis intent"""
        
        try:
            primary_field = intent.get('primary_field', 'status')
            viz_type = intent.get('visualization_type', 'bar')
            
            # Apply filters if specified
            filtered_df = df.copy()
            for field, value in intent.get('filters', {}).items():
                if field in filtered_df.columns:
                    if isinstance(value, list):
                        filtered_df = filtered_df[filtered_df[field].isin(value)]
                    else:
                        filtered_df = filtered_df[filtered_df[field] == value]
            
            if len(filtered_df) == 0:
                return None
            
            # Generate appropriate visualization
            if primary_field in filtered_df.columns:
                if viz_type == 'pie':
                    counts = filtered_df[primary_field].value_counts()
                    return px.pie(
                        values=counts.values,
                        names=counts.index,
                        title=f"{primary_field.title()} Distribution"
                    )
                
                elif viz_type == 'line' and intent.get('time_dimension'):
                    # Time-based line chart
                    if 'created' in filtered_df.columns:
                        filtered_df['created_dt'] = pd.to_datetime(filtered_df['created'], errors='coerce')
                        time_counts = filtered_df.groupby(filtered_df['created_dt'].dt.date).size()
                        return px.line(
                            x=time_counts.index,
                            y=time_counts.values,
                            title=f"{primary_field.title()} Trend Over Time",
                            markers=True
                        )
                
                elif viz_type == 'heatmap':
                    # Create correlation heatmap for numerical data
                    numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) > 1:
                        corr_matrix = filtered_df[numeric_cols].corr()
                        return px.imshow(
                            corr_matrix,
                            title="Data Correlation Heatmap",
                            color_continuous_scale='RdBu'
                        )
                
                elif viz_type == 'sunburst':
                    # Hierarchical sunburst chart
                    counts = filtered_df[primary_field].value_counts()
                    return px.sunburst(
                        names=list(counts.index) + ['Total'],
                        parents=['Total'] * len(counts) + [''],
                        values=list(counts.values) + [counts.sum()],
                        title=f"{primary_field.title()} Hierarchy"
                    )
                
                elif viz_type == 'treemap':
                    # Treemap visualization
                    counts = filtered_df[primary_field].value_counts()
                    return px.treemap(
                        names=counts.index,
                        values=counts.values,
                        title=f"{primary_field.title()} Treemap"
                    )
                
                elif viz_type == 'scatter':
                    # Scatter plot (requires numerical data)
                    numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) >= 2:
                        return px.scatter(
                            filtered_df,
                            x=numeric_cols[0],
                            y=numeric_cols[1],
                            color=primary_field if primary_field in filtered_df.columns else None,
                            title=f"Scatter Plot: {numeric_cols[0]} vs {numeric_cols[1]}"
                        )
                
                elif viz_type == 'table':
                    # Return None to indicate table should be shown instead of chart
                    return None
                
                else:
                    # Default bar chart
                    counts = filtered_df[primary_field].value_counts()
                    return px.bar(
                        x=counts.index,
                        y=counts.values,
                        title=f"{primary_field.title()} Analysis",
                        labels={'x': primary_field.title(), 'y': 'Count'}
                    )
            
            return None
            
        except Exception as e:
            st.error(f"Visualization error: {str(e)}")
            return None
    
    def _basic_intent_analysis(self, question: str, chart_preference: str) -> Dict[str, Any]:
        """Basic intent analysis when Claude AI is not available"""
        question_lower = question.lower()
        
        # Determine analysis type
        if 'trend' in question_lower or 'over time' in question_lower:
            analysis_type = 'trend'
            viz_type = 'line'
        elif 'compare' in question_lower:
            analysis_type = 'comparison'
            viz_type = 'bar'
        elif 'breakdown' in question_lower or 'distribution' in question_lower or 'pie' in question_lower:
            analysis_type = 'breakdown'
            viz_type = 'pie'
        else:
            analysis_type = 'summary'
            viz_type = 'bar'
        
        # Override with user preference
        if chart_preference != "Auto":
            viz_type = chart_preference.lower().replace(' chart', '')
        
        return {
            'analysis_type': analysis_type,
            'visualization_type': viz_type,
            'primary_field': 'status',
            'secondary_field': None,
            'grouping_strategy': 'by_category',
            'time_dimension': 'trend' in analysis_type,
            'filters': {},
            'metrics_to_calculate': ['count'],
            'confidence': 0.7,
            'explanation': f'Basic {analysis_type} analysis',
            'expected_insights': ['Data overview']
        }
    
    def _create_basic_summary(self, df: pd.DataFrame, intent: Dict[str, Any]) -> str:
        """Create basic summary when Claude AI is not available"""
        primary_field = intent.get('primary_field', 'status')
        analysis_type = intent.get('analysis_type', 'summary')
        
        summary = f"**{analysis_type.title()} Analysis:**\n\n"
        summary += f"• Total records analyzed: {len(df)}\n"
        
        if primary_field in df.columns:
            value_counts = df[primary_field].value_counts()
            summary += f"• {primary_field.title()} categories: {len(value_counts)}\n"
            summary += f"• Top category: {value_counts.index[0]} ({value_counts.iloc[0]} items)\n"
        
        return summary
    
    def _basic_follow_up_questions(self, analysis_type: str) -> List[str]:
        """Generate basic follow-up questions"""
        base_questions = {
            'summary': [
                "Show me team performance breakdown",
                "What's our completion rate?",
                "How are priorities distributed?",
                "Show me trends over time"
            ],
            'trend': [
                "What caused the peaks in activity?",
                "How does this compare to last month?",
                "Show me breakdown by team member",
                "What's the velocity trend?"
            ],
            'comparison': [
                "Which team member needs support?",
                "What's causing the differences?",
                "Show me performance over time",
                "How can we balance the workload?"
            ],
            'breakdown': [
                "What's driving the largest category?",
                "How has this changed over time?",
                "Show me by priority level",
                "What are the quality metrics?"
            ]
        }
        
        return base_questions.get(analysis_type, base_questions['summary'])
    
    def _get_date_range(self, df: pd.DataFrame) -> str:
        """Get date range from dataframe"""
        if 'created' in df.columns:
            try:
                dates = pd.to_datetime(df['created'], errors='coerce').dropna()
                if len(dates) > 0:
                    return f"{dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}"
            except:
                pass
        return "Unknown"

def enhance_dataframe_for_ai(df: pd.DataFrame) -> pd.DataFrame:
    """Enhance dataframe with calculated fields for better AI analysis"""
    
    try:
        # Handle datetime conversion robustly
        if 'created' in df.columns:
            df['created_dt'] = pd.to_datetime(df['created'], errors='coerce', utc=True)
            valid_dates = df['created_dt'].notna()
            if valid_dates.any():
                df.loc[valid_dates, 'created_date'] = df.loc[valid_dates, 'created_dt'].dt.date
                df.loc[valid_dates, 'created_month'] = df.loc[valid_dates, 'created_dt'].dt.to_period('M')
                df.loc[valid_dates, 'created_year'] = df.loc[valid_dates, 'created_dt'].dt.year
                df.loc[valid_dates, 'created_week'] = df.loc[valid_dates, 'created_dt'].dt.isocalendar().week
                df.loc[valid_dates, 'created_quarter'] = df.loc[valid_dates, 'created_dt'].dt.quarter
        
        # Handle resolution dates
        if 'resolutiondate' in df.columns:
            df['resolved_dt'] = pd.to_datetime(df['resolutiondate'], errors='coerce', utc=True)
            
            # Calculate resolution times
            if 'created_dt' in df.columns:
                valid_resolution = df['resolved_dt'].notna() & df['created_dt'].notna()
                if valid_resolution.any():
                    df.loc[valid_resolution, 'resolution_days'] = (
                        df.loc[valid_resolution, 'resolved_dt'] - df.loc[valid_resolution, 'created_dt']
                    ).dt.days
        
        # Age calculation
        if 'created_dt' in df.columns:
            valid_created = df['created_dt'].notna()
            if valid_created.any():
                df.loc[valid_created, 'age_days'] = (
                    pd.Timestamp.now(tz='UTC') - df.loc[valid_created, 'created_dt']
                ).dt.days
        
        # Clean up assignee field
        if 'assignee' in df.columns:
            df['assignee'] = df['assignee'].fillna('Unassigned')
        
        # Clean up priority field
        if 'priority' in df.columns:
            df['priority'] = df['priority'].fillna('None')
        
        return df
        
    except Exception as e:
        st.warning(f"Data enhancement warning: {str(e)}")
        return df

def setup_claude_integration() -> str:
    """Setup Claude API integration in Streamlit sidebar"""
    
    with st.sidebar:
        st.subheader("🧠 JirAI Integration")
        
        # API Key input
        claude_api_key = st.text_input(
            "AI API Key",
            type="password",
            help="Enter your AI API key for enhanced AI capabilities"
        )
        
        if claude_api_key:
            # Test the API key
            try:
                client = anthropic.Anthropic(api_key=claude_api_key)
                # Simple test call
                test_response = client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=10,
                    messages=[{"role": "user", "content": "Hi"}]
                )
                st.success("✅ AI connected!")
                st.session_state.claude_api_key = claude_api_key
                return claude_api_key
            except Exception as e:
                st.error(f"❌ AI API error: {str(e)}")
                return None

def process_claude_question(question: str, chart_preference: str = "Auto"):
    """Process question using Claude AI-enhanced analysis"""
    
    if 'issues_df' not in st.session_state.jira_data:
        st.error("Please fetch Jira data first to use the AI assistant.")
        return
    
    df = st.session_state.jira_data['issues_df'].copy()
    
    # Get Claude API key
    claude_api_key = st.session_state.get('claude_api_key', None)
    
    # Initialize Claude analyzer
    analyzer = ClaudeJiraAnalyzer(claude_api_key)
    
    # Enhance dataframe
    df = enhance_dataframe_for_ai(df)
    
    try:
        with st.spinner("🧠 JirAI is analyzing your data..."):
            # Analyze question and generate visualization
            chart, response, intent = analyzer.analyze_question(question, df, chart_preference)
            
            # Create data table if requested or if no chart was generated
            if intent.get('visualization_type') == 'table' or chart is None:
                data_table = analyzer.create_data_table(df, intent)
                st.session_state.last_data_table = data_table
            
            # Get follow-up suggestions
            followup_questions = analyzer.get_follow_up_suggestions(question, intent, df)
            
            # Get chart explanation
            if chart is not None:
                chart_explanation = analyzer.explain_chart(question, intent.get('visualization_type', 'chart'), df)
                response += f"\n\n**📊 Chart Selection:**\n{chart_explanation}"
        
        # Add confidence indicator
        confidence = intent.get('confidence', 0.5)
        if analyzer.has_ai:
            if confidence > 0.8:
                confidence_indicator = "🎯 High Confidence (AI Enhanced)"
            elif confidence > 0.6:
                confidence_indicator = "🤖 Medium Confidence (AI Enhanced)"
            else:
                confidence_indicator = "🔄 Learning (AI Active)"
        else:
            confidence_indicator = "🔧 Basic Analysis Mode"
        
        enhanced_response = f"{confidence_indicator}\n\n{response}"
        
        # Add follow-up suggestions
        if followup_questions:
            enhanced_response += f"\n\n**💡 Suggested Follow-up Questions:**\n"
            for i, q in enumerate(followup_questions[:3], 1):
                enhanced_response += f"{i}. {q}\n"
        
        # Store results
        st.session_state.chat_history.append((question, enhanced_response, chart))
        st.session_state.followup_questions = followup_questions
        st.session_state.last_intent = intent
        
        st.rerun()
        
    except Exception as e:
        error_response = f"Analysis error: {str(e)}\n\nPlease try rephrasing your question or check your data."
        st.session_state.chat_history.append((question, error_response, None))
        st.rerun()

def display_claude_enhanced_chat():
    """Display Claude AI-enhanced chat interface"""
    
    st.header("🤖 JirAI Assistant")
    st.write("Ask intelligent questions about your Jira data and get AI-powered insights with visualizations")
    
    # Setup Claude integration
    claude_api_key = setup_claude_integration()
    
    # Initialize session state
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'followup_questions' not in st.session_state:
        st.session_state.followup_questions = []
    if 'last_data_table' not in st.session_state:
        st.session_state.last_data_table = None
    
    # Check if data is available
    if 'issues_df' in st.session_state.jira_data:
        data_info = st.session_state.jira_data['issues_df']
        
        # Display AI capabilities
        if claude_api_key:
            st.success("🧠 **AI Enhanced Mode** - Advanced natural language understanding active!")
            with st.expander("🚀 AI Capabilities"):
                st.markdown("""
                **🎯 Advanced Analysis:**
                - Natural language question understanding
                - Intelligent insight generation
                - Context-aware recommendations
                - Automated pattern detection
                
                **📊 Smart Visualizations:**
                - Optimal chart type selection
                - Multi-dimensional analysis
                - Interactive data exploration
                - Explanation of chart choices
                
                **💡 Proactive Insights:**
                - Performance optimization suggestions
                - Risk identification
                - Process improvement recommendations
                - Follow-up question generation
                """)
        else:
            st.info("🔧 **Basic Mode** - Add Claude API key for enhanced intelligence")
        
        # Quick data overview
        with st.expander("📊 Data Overview"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Issues", len(data_info))
            with col2:
                projects = data_info['project'].nunique() if 'project' in data_info.columns else 0
                st.metric("Projects", projects)
            with col3:
                assignees = data_info['assignee'].nunique() if 'assignee' in data_info.columns else 0
                st.metric("Team Members", assignees)
            with col4:
                if 'status' in data_info.columns:
                    completed = len(data_info[data_info['status'].isin(['Done', 'Resolved', 'Closed'])])
                    completion_rate = (completed / len(data_info) * 100) if len(data_info) > 0 else 0
                    st.metric("Completion Rate", f"{completion_rate:.1f}%")
        
        # Chat interface
        col1, col2 = st.columns([3, 1])
        
        with col1:
            user_question = st.text_input(
                "Ask JirAI about your Jira data:",
                placeholder="e.g., What insights can you provide about our team's performance and what should we improve?",
                key="claude_question_input"
            )
        
        with col2:
            chart_preference = st.selectbox(
                "Visualization",
                ["Auto", "Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot", 
                 "Heatmap", "Sunburst", "Treemap", "Table", "Funnel"],
                help="Claude will intelligently choose the best visualization"
            )
        
        # Action buttons
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("🚀 Ask JirAI", type="primary"):
                if user_question:
                    process_claude_question(user_question, chart_preference)
                else:
                    st.warning("Please enter a question!")
        
        with col2:
            if st.button("🗑️ Clear Chat"):
                st.session_state.chat_history = []
                st.session_state.followup_questions = []
                st.session_state.last_data_table = None
                st.rerun()
        
        # Follow-up questions
        if st.session_state.followup_questions:
            st.subheader("💡 Suggested Next Questions")
            cols = st.columns(min(3, len(st.session_state.followup_questions)))
            for i, question in enumerate(st.session_state.followup_questions[:3]):
                with cols[i]:
                    if st.button(f"💭 {question[:40]}...", key=f"followup_{i}", help=question):
                        process_claude_question(question, "Auto")
        
        # Display data table if available
        if st.session_state.last_data_table is not None:
            st.subheader("📊 Data Summary Table")
            st.dataframe(st.session_state.last_data_table, use_container_width=True, hide_index=True)
        
        # Chat history
        if st.session_state.chat_history:
            st.markdown("---")
            st.subheader("💬 Conversation History")
            
            for i, (question, response, chart) in enumerate(reversed(st.session_state.chat_history)):
                with st.container():
                    # Question
                    st.markdown(f"**🧑 You:** {question}")
                    
                    # Response
                    st.markdown(f"**🤖 JirAI:**")
                    st.markdown(response)
                    
                    # Chart if available
                    if chart is not None:
                        try:
                            st.plotly_chart(chart, use_container_width=True, key=f"JiraAI_chart_{i}")
                        except Exception as e:
                            st.error(f"Visualization error: {str(e)}")
                    
                    if i < len(st.session_state.chat_history) - 1:
                        st.markdown("---")
        
        # Quick actions
        st.markdown("---")
        st.subheader("⚡ Quick Analysis")
        
        quick_actions = [
            ("📊 Overall Health", "Analyze our project's overall health and provide insights on what we're doing well and what needs improvement"),
            ("👥 Team Performance", "Compare team member performance and suggest ways to optimize workload distribution"),
            ("🔍 Bottleneck Analysis", "Identify bottlenecks in our workflow and suggest process improvements"),
            ("📈 Trend Analysis", "Show me trends over time and predict future patterns"),
            ("🎯 Priority Focus", "Analyze our priority distribution and suggest focus areas for maximum impact")
        ]
        
        cols = st.columns(len(quick_actions))
        for i, (label, question) in enumerate(quick_actions):
            with cols[i]:
                if st.button(label, key=f"quick_claude_{i}", help=question):
                    process_claude_question(question, "Auto")
    
    else:
        st.warning("Please fetch Jira data first to start using AI assistant.")
        st.info("Configure your Jira connection in the sidebar to get started.")

# INTEGRATION FUNCTIONS FOR MAIN APPLICATION

def display_enhanced_ai_chat_tab():
    """Main function to display the enhanced AI chat tab - Use this in your main tabs"""
    display_claude_enhanced_chat()
    

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
                        
                        #Fetch worklogs for ALL issues (with reasonable limit)
                        #st.info(f"Fetching worklogs for {len(all_issue_keys)} issues...")
                        
                        # Limit worklog fetching to prevent timeout (adjust as needed)
                        #worklog_limit = min(len(all_issue_keys), 200)
                        #limited_issue_keys = all_issue_keys[:worklog_limit]
                        
                        #worklogs = jira_api.get_worklogs(limited_issue_keys)
                        
                        #if worklogs:
                        #    worklogs_df = pd.DataFrame(worklogs)
                        #    st.session_state.jira_data['worklogs_df'] = worklogs_df
                        #    st.success(f"✅ Fetched {len(issues)} issues and {len(worklogs)} worklogs from {len(config.projects)} projects")
                        #else:
                        #    st.warning(f"✅ Fetched {len(issues)} issues but no worklogs found")
                        
                        #st.rerun()
                    #else:
                        #st.warning("No issues found in the specified date range")
            else:
                st.warning("Please configure Jira settings and select projects first.")
                
    # Main content area - Updated tabs
    tab1,tab2,tab3,tab4,tab5 = st.tabs(["✔️ Sanity Check", "📈 Operations Report", "🛠️ Support Report", "🔍Cause Code Analysis", "🧑‍💻 ProdOps Report"])
    #"💬 AI Chat"
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
    #with tab6:
        #display_enhanced_ai_chat_tab()

        
def display_project_dashboard(df, project_name):
    """Display dashboard for a specific project"""
    try:
        # Enhanced AI Agent for insights
        ai_agent = ClaudeJiraAI()
        
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
        st.dataframe(df[available_columns], use_container_width=True, hide_index= True)
        
    except Exception as e:
        st.error(f"Error displaying dashboard: {str(e)}")

def process_ai_chat_question(question: str, chart_preference: str = "Auto"):
    """Advanced AI chat processing with sophisticated analysis"""
    if 'issues_df' not in st.session_state.jira_data:
        return
    
    df = st.session_state.jira_data['issues_df'].copy()
    ai_agent = ClaudeJiraAI()
    
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
    
def smart_general_analysis(df, question, chart_preference="Auto", entities=None):
    """Replacement for the problematic smart_general_analysis function"""
    try:
        # Simple fallback analysis
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            
            if chart_preference.lower() == 'pie':
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Data Overview"
                )
            else:
                fig = px.bar(
                    x=status_counts.index,
                    y=status_counts.values,
                    title="Data Overview",
                    labels={'x': 'Status', 'y': 'Count'}
                )
            
            response = f"Analysis: {len(df)} total issues across {len(status_counts)} status categories."
            return fig, response
        
        # Fallback for data without status
        return None, "Data overview: Please use the enhanced AI features for better analysis."
        
    except Exception as e:
        return None, f"Analysis unavailable: {str(e)}"
    
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
            # FIX    the issue with any delivery the includes the work upstream
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
    st.subheader("1️⃣ Resolved Without Due Date Check")
    with st.spinner("Checking resolved dates..."):
        success, result = check_missing_due_date(df, end_date)
        if success:
            st.success(result)
        else:
            st.error("❌ Issues found with missing due dates")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index=True)
            else:
                st.write(result)
    
    st.subheader("2️⃣ Missing Closed Date Check")
    with st.spinner("Checking for missing closed dates..."):
        success, result = check_missing_closed_date(df, end_date)
        if success:
            st.success(result)
        else:
            st.error("❌ Issues found with missing closed dates")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index= True)
            else:
                st.write(result)
    
    st.subheader("3️⃣ Future Closed Date Check")
    with st.spinner("Checking for future closed dates..."):
        success, result = check_future_closed_date(df, end_date)
        if success:
            st.success(result)
        else:
            st.error("❌ Issues found with future closed dates")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index= True)
            else:
                st.write(result)
                
    st.subheader("4️⃣ Open Deliveries Check")
    with st.spinner("Checking Open Deliveries..."):
        success, result = check_open_deliveries(df)
        if success:
            st.success(result)
        else:
            st.error("❌ Open Deliveries Found")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index= True)
            else:
                st.write(result)
                
    st.subheader("5️⃣ In Progress Deliveries Check")
    with st.spinner("Checking for In Progress Deliveries..."):
        success, result = check_inprogress_deliveries(df)
        if success:
            st.success(result)
        else:
            st.error("❌ In Progress Deliveries Found")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index=True)
            else:
                st.write(result)
                
    st.subheader("6️⃣ Cancelled Deliveries Check")
    with st.spinner("Checking for Cancelled Deliveries..."):
        success, result = check_cancelled_deliveries(df)
        if success:
            st.success(result)
        else:
            st.error("❌ Cancelled Deliveries Found")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index= True)
            else:
                st.write(result)            
    
    st.subheader("7️⃣ Delayed Deliveries Check")
    with st.spinner("Checking Delayed Deliveries..."):
        success, result = check_delayed_deliveries(df, end_date)
        if success:
            st.success(result)
        else:
            st.error("❌ Delayed Deliveries Found")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result, use_container_width=True, hide_index=True)
            else:
                st.write(result)
    
    # Summary section
    st.markdown('---')
    st.subheader("🎟️ All DTE Delivery Calendar Ticket Summary")
    dte_deliveries = filter_dte_delivery_stories(df)
    display_cols = ['issuetype', 'key', 'summary','priority', 'status', 'assignee',  'due_date', 'closed_date'] 
    available_columns = [col for col in display_cols if col in dte_deliveries.columns]
    st.dataframe(dte_deliveries[available_columns], use_container_width=True, hide_index=True)

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
            st.dataframe(monthly_df, use_container_width=True, hide_index=True)
        
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

        st.subheader('Delivery Breakdown by Parent')
        parent_df['Parent'] = parent_df['Parent'].replace('CDD', 'DataIQ')
        st.dataframe(parent_df, use_container_width=True, hide_index=True)
    
        
        st.subheader("Operations Delivery Breakdown")
                    #fig = px.pie(grouped_df, values='Total Count', names='Parent')
            # Create bar chart
            
        fig = px.bar(
        grouped_df,
        x='Parent',
        y='Total Deliveries',
        text='Total Deliveries',
        color_discrete_sequence=['#0033A0']  
        )

        # Calculate max value for proper Y-axis range
        max_value = grouped_df['Total Deliveries'].max()

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
            
            # Y-axis styling with proper range for text labels
            yaxis=dict(
                tickfont=dict(size=14, color='black', family='Arial Black'),
                title=dict(
                    text='Total Deliveries',
                    font=dict(size=16, color='black', family='Arial Black')
                ),
                showgrid=True,
                gridcolor='lightgray',
                range=[0, max_value * 1.3]  # Add 30% padding above highest bar for text labels
            ),
            
            # Layout properties
            plot_bgcolor='white',
            margin=dict(l=80, r=60, t=100, b=120),  # Increased top margin from 80 to 100
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

        st.plotly_chart(fig, use_container_width=True)
        
            
    # Define the data
    data = {
        "Acronym": ["ARA", "AIML", "DataIQ", "LPD", "EMR", "Alert Engine"],
        "Definition": [
            "Onboarding, either from DataIQ or a data owner. The data is checked, prepared, and the ETL process is run, followed by automated tests to ensure completion. After integrating and performing QA in DEV, DEMO, and LIVE environments, a 'Go Live' notification is sent to stakeholders once everything is verified.",
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
    st.dataframe(df, use_container_width=True, hide_index=True)
    
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
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
        
#ProdOps Section   
def filter_dev_star_stories(df):
    """Apply standard Dev Star Calendar filters"""
    # Filter by project name
    filtered_df = df[df['project_name'] == 'The Dev Star'].copy() if 'project_name' in df.columns else df.copy()
    
    # Filter by issue type
    if 'issuetype' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['issuetype'].isin(['Story', 'Research', 'Bug'])]
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
                    font=dict(size=18, color='black')  # Removed invalid 'weight' property
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
            f"{results['metrics']['completion_count']['count']} stories"
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

        st.dataframe(display_df, use_container_width=True,hide_index=True)
        
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
    
    # Define color palette
    Template_Colors = {
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
    task1_fig = create_resolved_items_chart(df, start_date, end_date, Template_Colors)
    
    # Task 2: Incidents per Project
    task2_fig = create_incidents_per_project_chart(df, start_date, end_date, Template_Colors)
    
    # Task 3: Incidents by Priority
    task3_fig = create_incidents_by_priority_chart(df, start_date, end_date, Template_Colors)
    
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
    """Task 2: Create incidents per project bar chart with tight grouping"""
    
    # Filter incidents only (exclude post-incidents)
    df_incidents = df[
        df['issuetype'].str.contains('Incident', case=False, na=False) & 
        ~df['issuetype'].str.contains('Post', case=False, na=False)
    ]
    
    # Get months
    months = pd.date_range(start=start_date, end=end_date, freq='ME').strftime('%B').tolist()
    
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
    
    # Collect all data and identify projects with incidents
    all_data = {}
    project_totals = {}
    
    for month in months:
        all_data[month] = {}
        if month == 'March':
            for country, count in march_data.items():
                all_data[month][country] = count
                project_totals[country] = project_totals.get(country, 0) + count
        else:
            month_data = df_incidents[df_incidents['resolution_month'] == month]
            if 'country' in month_data.columns:
                country_counts = month_data['country'].value_counts()
                for country, count in country_counts.items():
                    all_data[month][country] = count
                    project_totals[country] = project_totals.get(country, 0) + count
    
    # Filter to only projects with incidents
    active_projects = [proj for proj, total in project_totals.items() if total > 0]
    
    # Sort projects by total incidents (descending)
    active_projects.sort(key=lambda x: project_totals[x], reverse=True)
    
    # Create figure
    fig = go.Figure()
    
    # Color mapping - ensure we have colors for all months by cycling
    month_colors = [colors['blue'], colors['light_blue'], colors['green'], colors['grey']]
    month_color_map = {}
    for i, month in enumerate(months):
        month_color_map[month] = month_colors[i % len(month_colors)]  # Cycle through colors
    
    # Calculate custom positions for tight grouping
    bar_width = 0.15
    group_spacing = 1.0  # Space between project groups
    
    # Build data structure for positioning with error handling
    project_bar_data = {}
    for project in active_projects:
        project_bar_data[project] = []
        for month in months:
            count = all_data[month].get(project, 0)
            if count > 0:  # Only include months with actual data
                # Ensure month exists in color map
                if month not in month_color_map:
                    print(f"Warning: No color found for month {month}")
                    continue
                    
                project_bar_data[project].append({
                    'month': month,
                    'count': count,
                    'color': month_color_map[month]
                })
    
    # Calculate positions and create bars
    current_x = 0
    x_tick_positions = []
    x_tick_labels = []
    
    for project in active_projects:
        bars_data = project_bar_data[project]
        if not bars_data:
            continue
            
        # Calculate center position for this project
        num_bars = len(bars_data)
        group_width = num_bars * bar_width
        group_start = current_x - group_width / 2
        
        # Add each bar for this project
        for i, bar_data in enumerate(bars_data):
            x_pos = group_start + (i + 0.5) * bar_width
            
            fig.add_trace(go.Bar(
                x=[x_pos],
                y=[bar_data['count']],
                width=bar_width,
                marker_color=bar_data['color'],
                text=[str(bar_data['count'])],
                textposition='outside',
                textfont=dict(size=16, color='black', family='Arial Black'),
                showlegend=False,  # We'll add custom legend
                hoverinfo='none'
            ))
        
        # Store position for x-axis labels
        x_tick_positions.append(current_x)
        x_tick_labels.append(project)
        current_x += group_spacing
    
    # Add custom legend
    for month, color in month_color_map.items():
        fig.add_trace(go.Bar(
            x=[None], y=[None],
            marker_color=color,
            name=month,
            showlegend=True
        ))
    
    # Calculate max value for y-axis
    max_month_value = 0
    for month_data in all_data.values():
        for count in month_data.values():
            if count > max_month_value:
                max_month_value = count
    
    if max_month_value == 0:
        max_month_value = 15
    
    # Update layout
    fig.update_layout(
        xaxis=dict(
            tickvals=x_tick_positions,
            ticktext=x_tick_labels,
            tickfont=dict(size=11, color='black', family='Arial Black'),
            title=dict(
                text='Projects',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            tickangle=-45,
            range=[-0.5, current_x - 0.5]
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=12, color='black', family='Arial'),
            title=dict(
                text='Incident Count',
                font=dict(size=14, color='black', family='Arial Black')
            ),
            range=[0, max_month_value * 1.3]
        ),
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(size=11, color='black', family='Arial')
        ),
        width=600,
        height=550,
        autosize=False,
        margin=dict(l=80, r=40, t=90, b=120),
        dragmode=False
    )
    
    return fig

def create_incidents_by_priority_chart(df, start_date, end_date, colors):
    """Task 3: Create incidents by priority vertical bar chart"""
    
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
    
    # Calculate gap settings based on number of months
    num_months = len(months)
    
    # Adjust gap settings based on number of months to prevent overlapping
    if num_months == 1:
        bargap = 0.6  # More space around single month group
        bargroupgap = 0.1  # Tight spacing between priorities
    elif num_months == 2:
        bargap = 0.4  # Good spacing between month groups
        bargroupgap = 0.15  # Moderate spacing between priorities
    elif num_months == 3:
        bargap = 0.5  # More space between month groups
        bargroupgap = 0.25  # More space between priorities to prevent overlap
    elif num_months == 4:
        bargap = 0.6  # Even more space between month groups
        bargroupgap = 0.3  # Maximum space between priorities
    else:
        bargap = 0.7  # Maximum space for 5+ months
        bargroupgap = 0.35
    
    # Collect all data and max value
    all_values = []
    
    # Create one trace per priority - this ensures proper grouping
    for priority in priority_order:
        y_values = []  # counts for this priority across all months
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
            
            y_values.append(count)
            text_values.append(str(count) if count > 0 else '')
            all_values.append(count)
        
        # Add one trace for this priority across all months
        fig.add_trace(go.Bar(
            name=priority,
            x=months,  # All months on x-axis
            y=y_values,  # Values for this priority
            marker_color=priority_colors[priority],
            text=text_values,
            textposition='outside',
            textfont=dict(size=16, color='black', family='Arial Black'),
            offsetgroup=priority  # This ensures proper grouping
        ))
    
    # Calculate maximum value for y-axis range
    max_value = max(all_values) if all_values else 16
    
    # Calculate dynamic height
    chart_height = min(600, max(400, max_value * 20 + 200))
    
    fig.update_layout(
        xaxis=dict(
            tickfont=dict(size=14, color='black', family='Arial Black'),
            title=dict(
                text='Months',
                font=dict(size=16, color='black', family='Arial Black')
            )
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=14, color='black', family='Arial'),
            title=dict(
                text='Incident Count',
                font=dict(size=16, color='black', family='Arial Black')
            ),
            range=[0, max_value * 1.25]  # Space for text labels
        ),
        barmode='group',  # Critical: groups bars side by side
        plot_bgcolor='white',
        paper_bgcolor='white',
        width=600,
        height=chart_height,
        autosize=False,
        bargap=bargap,  # Dynamic space between month groups
        bargroupgap=bargroupgap,  # Dynamic space between priority bars within each month
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
            font=dict(size=12, color='black', family='Arial')
        ),
        margin=dict(l=80, r=40, t=100, b=80),
        showlegend=True,
        dragmode=False
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
    st.plotly_chart(results['resolved_items_chart'], use_container_width=True)
    
    
    # Display Task 2
    st.subheader("🌍 Incidents per Project")
    st.plotly_chart(results['incidents_per_project_chart'], use_container_width=True)
    
    # Display Task 3
    st.subheader("🎯 Incidents by Priority")
    st.plotly_chart(results['incidents_by_priority_chart'], use_container_width=True)
    
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
    
    #Task 3: First Response Time Scatter Chart - All Months
    first_response_scatter_all = create_first_response_scatter_all(df_prepared, start_date, end_date)
    
    # Task 4: First Response Time Scatter Chart - Last Month
    first_response_scatter_month = create_first_response_scatter_month(df_prepared, last_month)
    
    # Task 5: Resolution Time Overall Result - Last Month
    resolution_time_table = create_resolution_time_table(df_prepared, last_month)
    
    # Task 6: Resolution Time Column Chart - All Months
    resolution_time_column = create_resolution_time_column_chart(df_prepared, start_date, end_date)
    
    # Task 7: Resolution Time Scatter Chart - All Months
    resolution_time_scatter_all = create_resolution_time_scatter_all(df_prepared, start_date, end_date)
    
    # Task 8: Resolution Time Scatter Chart - Last Month
    resolution_time_scatter_month = create_resolution_time_scatter_month(df_prepared, last_month)
    
    return {
        'first_response_table': first_response_table,
        'first_response_column': first_response_column,
        'first_response_scatter_all': first_response_scatter_all,
        'first_response_scatter_month': first_response_scatter_month,
        'resolution_time_table': resolution_time_table,
        'resolution_time_column': resolution_time_column,
        'resolution_time_scatter_all': resolution_time_scatter_all,
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
        df_month['first_response_time'] = df_month['first_response_time'].fillna(0)
        df_month['first_response_goal'] = df_month['first_response_goal'].fillna(0)
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
                'Average_Minutes': 35.78,  # 35m 47s in decimal
                'Label': '35m 47s'
            })
        else:
            month_df = df[df['resolution_month'] == month]
            if len(month_df) > 0 and 'first_response_seconds' in month_df.columns:
                avg_seconds = month_df['first_response_seconds'].mean()
                avg_minutes = avg_seconds / 60  # Convert to minutes for consistency
                minutes = int(avg_seconds // 60)
                seconds = int(avg_seconds % 60)
                label = f"{minutes}m {seconds:02d}s"
                monthly_data.append({
                    'Month': month,
                    'Average_Minutes': avg_minutes,
                    'Label': label
                })
    
    # Create DataFrame
    plot_df = pd.DataFrame(monthly_data)
    
    # Calculate dynamic properties based on data - same logic as resolution time
    num_months = len(plot_df)
    max_value = plot_df['Average_Minutes'].max() if len(plot_df) > 0 else 40
    
    # Dynamic bar width - consistent across both methods
    bar_width = max(0.3, min(0.8, 0.8 / max(1, num_months * 0.3)))
    
    # Dynamic Y-axis range - ensure text labels are fully visible
    y_range_max = max_value * 1.3  # 30% padding for text labels
    
    # Dynamic text size based on available space and number of months
    base_text_size = 20
    text_size = max(16, min(base_text_size, base_text_size - (num_months - 2) * 1))
    
    # Create figure
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=plot_df['Month'],
        y=plot_df['Average_Minutes'],
        text=plot_df['Label'],
        textposition='outside',
        textfont=dict(size=text_size, color='black', family='Arial Black'),
        marker_color='darkblue',
        name='Average Response Time',
        width=bar_width
    ))
    
    # Dynamic margin adjustments - same logic as resolution time
    top_margin = max(100, 80 + (text_size - 16) * 2)  # More space for larger text
    bottom_margin = max(80, 60 + num_months * 2)  # More space for month labels if many months
    
    # Update layout with dynamic properties
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
            gridcolor='lightgray',
            range=[0, y_range_max]  # Dynamic range to accommodate text labels
        ),
        plot_bgcolor='white',
        showlegend=False,
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,
        margin=dict(
            l=80,
            r=60,
            t=top_margin,  # Dynamic top margin for text labels
            b=bottom_margin  # Dynamic bottom margin for month labels
        ),
        dragmode=False
    )
    
    return fig

def create_first_response_scatter_all(df, start_date, end_date):
    """Task 3: Create first response time scatter chart for all months"""
    
    # Sort by resolution date
    df_sorted = df.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_sorted['seq_num'] = range(1, len(df_sorted) + 1)
    
    # Get month boundaries and calculate mid-points for x-axis labels
    month_boundaries = []
    tick_positions = []
    tick_labels = []
    
    # Get unique months in chronological order (preserve order from sorted dataframe)
    seen_months = set()
    ordered_months = []
    for month in df_sorted['resolution_month']:
        if month not in seen_months:
            ordered_months.append(month)
            seen_months.add(month)
    
    for month in ordered_months:
        month_df = df_sorted[df_sorted['resolution_month'] == month]
        if len(month_df) > 0:
            start_pos = month_df['seq_num'].min()
            end_pos = month_df['seq_num'].max()
            mid_point = (start_pos + end_pos) / 2
            
            month_boundaries.append({
                'month': month,
                'start': start_pos,
                'end': end_pos,
                'mid_point': mid_point
            })
            
            tick_positions.append(mid_point)
            tick_labels.append(month)
    
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
    
    # Update layout with custom x-axis labels
    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=tick_positions,
            ticktext=tick_labels,
            showticklabels=True,
            showgrid=False,
            zeroline=False,
            title='',
            tickfont=dict(size=11)
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
        df_month['first_resolution_time'] = df_month['first_resolution_time'].fillna(0)
        df_month['first_resolution_goal'] = df_month['first_resolution_goal'].fillna(0)
        
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
                'Average_Hours': 8.92, # 8:55 in decimal
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
    
    # Calculate dynamic properties based on data
    num_months = len(plot_df)
    max_value = plot_df['Average_Hours'].max() if len(plot_df) > 0 else 20
    
    # Dynamic bar width - consistent across both methods
    bar_width = max(0.3, min(0.8, 0.8 / max(1, num_months * 0.3)))
    
    # Dynamic Y-axis range - ensure text labels are fully visible
    y_range_max = max_value * 1.3  # 30% padding for text labels
    
    # Dynamic text size based on available space and number of months
    base_text_size = 20
    text_size = max(16, min(base_text_size, base_text_size - (num_months - 2) * 1))
    
    # Create figure
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=plot_df['Month'],
        y=plot_df['Average_Hours'],
        text=plot_df['Label'],
        textposition='outside',
        textfont=dict(size=text_size, color='black', family='Arial Black'),
        marker_color='darkblue',
        name='Average Resolution Time',
        width=bar_width
    ))
    
    # Dynamic margin adjustments
    top_margin = max(100, 80 + (text_size - 16) * 2)  # More space for larger text
    bottom_margin = max(80, 60 + num_months * 2)  # More space for month labels if many months
    
    # Update layout with dynamic properties
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
            gridcolor='lightgray',
            range=[0, y_range_max]  # Dynamic range to accommodate text labels
        ),
        plot_bgcolor='white',
        showlegend=False,
        # Fixed dimensions
        width=600,
        height=600,
        autosize=False,
        margin=dict(
            l=80,
            r=60,
            t=top_margin,  # Dynamic top margin for text labels
            b=bottom_margin  # Dynamic bottom margin for month labels
        ),
        dragmode=False
    )
    
    return fig

def create_resolution_time_scatter_all(df, start_date, end_date):
    """Task 7: Create resolution time scatter chart for all months"""
    
    # Sort by resolution date
    df_sorted = df.sort_values('resolutiondate').copy()
    
    # Add sequential numbers
    df_sorted['seq_num'] = range(1, len(df_sorted) + 1)
    
    # Get month boundaries and calculate mid-points for x-axis labels
    month_boundaries = []
    tick_positions = []
    tick_labels = []
    
    # Get unique months in chronological order (preserve order from sorted dataframe)
    seen_months = set()
    ordered_months = []
    for month in df_sorted['resolution_month']:
        if month not in seen_months:
            ordered_months.append(month)
            seen_months.add(month)
    
    for month in ordered_months:
        month_df = df_sorted[df_sorted['resolution_month'] == month]
        if len(month_df) > 0:
            start_pos = month_df['seq_num'].min()
            end_pos = month_df['seq_num'].max()
            mid_point = (start_pos + end_pos) / 2
            
            month_boundaries.append({
                'month': month,
                'start': start_pos,
                'end': end_pos,
                'mid_point': mid_point
            })
            
            tick_positions.append(mid_point)
            tick_labels.append(month)
    
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
    
    # Update layout with custom x-axis labels
    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=tick_positions,
            ticktext=tick_labels,
            showticklabels=True,
            showgrid=False,
            zeroline=False,
            title='',
            tickfont=dict(size=11)
        ),
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
        if results['first_response_scatter_all']:
            st.markdown(f'#### Response Time - YTD')
            st.plotly_chart(results['first_response_scatter_all'], use_container_width=True)
        
        # Task 4: Scatter Chart Last Month
        if results['first_response_scatter_month']:
            st.markdown(f'#### First Response Time (mins) - [{month_name}]')
            st.plotly_chart(results['first_response_scatter_month'], use_container_width=False)
    
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
        if results['resolution_time_scatter_all']:
            st.markdown(f'#### Resolution Time - YTD')
            st.plotly_chart(results['resolution_time_scatter_all'], use_container_width=True)
        
        # Task 8: Scatter Chart Last Month
        if results['resolution_time_scatter_month']:
            st.markdown(f'#### Resolution Time (hours) - {month_name}')
            st.plotly_chart(results['resolution_time_scatter_month'], use_container_width=False)
            
            
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
    
    # Create x-axis labels and positions with shortened names to prevent overlap
    x_labels = []
    x_positions = []
    current_position = 0
    
    # Function to shorten country names for display
    def shorten_country_name(name):
        if len(name) <= 8:
            return name
        # Create abbreviations for long names
        abbreviations = {
            'EMR Germany': 'EMR DE',
            'EMR Italy': 'EMR IT',
            'LPD Belgium': 'LPD BE',
            'LPD France': 'LPD FR',
            'US AE': 'US AE',
            'US HCP': 'US HCP',
            'DataIQ': 'DataIQ'
        }
        return abbreviations.get(name, name[:8])  # Fallback to first 8 chars
    
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
        
        # Update labels and positions with shortened names
        shortened_names = [shorten_country_name(name) for name in country_counts.index.tolist()]
        x_labels.extend(shortened_names)
        x_positions.extend(cause_positions)
        
        # Add gap between cause codes
        current_position += len(country_counts) + 1
    
    # Calculate max value for y-axis range
    max_value = 0
    for cause_code in top_cause_codes:
        cause_data = df_top_causes[df_top_causes[cause_code_field] == cause_code]
        country_counts = cause_data['country'].value_counts()
        if len(country_counts) > 0:
            max_value = max(max_value, country_counts.max())
    
    # Create custom x-axis with cause code sections
    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=x_positions,
            ticktext=x_labels,
            tickfont=dict(size=12, family="Arial Black"),  # Back to normal font size with shorter labels
            tickangle=-90,  # Keep at -90 degrees as requested
            showgrid=False
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='lightgray',
            tickfont=dict(size=12),
            range=[0, max_value * 1.3]  # Add 30% padding for text labels above bars
        ),
        height=600,  # Back to normal height
        barmode='group',
        plot_bgcolor='white',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,  # Move legend to top
            xanchor='center',
            x=0.5,
            font=dict(size=12)
        ),
        margin=dict(l=50, r=50, t=120, b=180)  # Reasonable bottom margin for shortened labels
    )
    
    # Add cause code section labels at the bottom - positioned lower to avoid overlap
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
        
        # Add section label - positioned appropriately below shortened country labels
        fig.add_annotation(
            x=section_center,
            y=-0.3,  # Adjusted back up since country names are now shorter
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
        date_range = f"{start_date.strftime('%b')} - {end_date.strftime('%b')}"

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
            st.dataframe(country_df, use_container_width=True, hide_index= True)
    
    # Show filtered data
    with st.expander("View Filtered HDEPS Incidents"):
        display_cols = ['key', 'country', 'status', 'priority', 'resolutiondate', 'ticket_resolution']
        available_cols = [col for col in display_cols if col in filtered_df.columns]
        st.dataframe(filtered_df[available_cols], use_container_width=True, hide_index= True)       

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        st.info("Please refresh the page and try again. If the issue persists, check your data and configuration.")