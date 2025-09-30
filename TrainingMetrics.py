import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import io
import openpyxl
import traceback
from typing import Dict, List, Tuple, Optional
import warnings
import re
warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="DTE Training Plan Metrics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 15px;
        color: white;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
        transition: transform 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
    }
    .status-complete {
        color: #28a745;
        font-weight: bold;
        background: #d4edda;
        padding: 4px 8px;
        border-radius: 15px;
        border: 1px solid #28a745;
    }
    .status-due {
        color: #ffc107;
        font-weight: bold;
        background: #fff3cd;
        padding: 4px 8px;
        border-radius: 15px;
        border: 1px solid #ffc107;
    }
    .status-overdue {
        color: #dc3545;
        font-weight: bold;
        background: #f8d7da;
        padding: 4px 8px;
        border-radius: 15px;
        border: 1px solid #dc3545;
    }
    .status-progress {
        color: #17a2b8;
        font-weight: bold;
        background: #d1ecf1;
        padding: 4px 8px;
        border-radius: 15px;
        border: 1px solid #17a2b8;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px 10px 0 0;
        color: white;
        font-weight: bold;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
</style>
""", unsafe_allow_html=True)

class DTEMetricsProcessor:
    def __init__(self):
        # Core data containers
        self.project_tasks = None
        self.training_dump = None
        self.effort_data = None
        self.operations_sheet = None
        self.support_sheet = None
        self.prodops_sheet = None
        self.operations_jira = None
        self.support_jira = None
        self.training_metrics = None
        self.competency_metrics = None
        self.distinct_names = None
        
        # Processed data containers
        self.operations_data = None
        self.support_data = None
        self.prodops_data = None
        self.competency_data = None
        
    def identify_team_from_name(self, employee_name: str) -> str:
        """Identify team based on employee name with enhanced matching"""
        if pd.isna(employee_name) or not employee_name:
            return 'Unknown'
        
        employee_name = str(employee_name).strip()
        name_lower = employee_name.lower()
        
        # ProdOps team (exact names and variations)
        prodops_patterns = [
            ['iestyn', 'pettigrew'],
            ['fernando', 'macena'],
            ['jonathan', 'funnel'],  # All variations of Jonathan
            ['jonathan', 'funnell']
        ]
        
        for pattern in prodops_patterns:
            if all(part in name_lower for part in pattern):
                return 'ProdOps'
        
        # Single name checks for unique names
        if 'iestyn' in name_lower or 'fernando' in name_lower or 'jonathan' in name_lower:
            return 'ProdOps'
        
        # Operations team keywords (enhanced from Excel analysis)
        operations_keywords = [
            'garchitorena', 'dianne marie', 'landingin', 'jhustine', 'palattao', 'cedric',
            'escrupulo', 'reuben john', 'salido', 'darren patrick jan', 'manalang', 'lemson',
            'sudhir', 'dhanush kumar l', 'kammala', 'rajashekhar', 'rachel banham', 'banham-rayward',
            'gitanjali', 'munda', 'will', 'noonan', 'ben'
        ]
        
        # Support team keywords (enhanced from Excel analysis)
        support_keywords = [
            'bharadwaj', 'achal n', 'garczyńska', 'kamila ex1', 'kamila', 'lal', 'rahul',
            'mallapanahalli doreswamy', 'niranja', 'pradeep', 'v pradeep', 'reddy', 'subba',
            'singh', 'avinash', 'venkateshwaralu', 'ashok', 'chandan kumar', 'chandan'
        ]
        
        # Check Operations (longer patterns first for more accurate matching)
        for keyword in sorted(operations_keywords, key=len, reverse=True):
            if keyword in name_lower:
                return 'Operations'
        
        # Check Support (longer patterns first for more accurate matching)  
        for keyword in sorted(support_keywords, key=len, reverse=True):
            if keyword in name_lower:
                return 'Support'
        
        return 'Unknown'
    
    def load_excel_file(self, file) -> bool:
        """Load and process the DTE Training Plan Metrics Excel file"""
        try:
            file.seek(0)
            excel_data = pd.ExcelFile(file)
            available_sheets = excel_data.sheet_names
            st.info(f"Available sheets: {available_sheets}")
            
            # Load Project tasks sheet (header at row 8)
            if 'Project tasks' in available_sheets:
                try:
                    project_df = pd.read_excel(file, sheet_name='Project tasks', header=None)
                    if len(project_df) > 8:
                        headers = project_df.iloc[8].values
                        self.project_tasks = project_df.iloc[9:].copy()
                        self.project_tasks.columns = headers
                        self.project_tasks = self.project_tasks.dropna(subset=['Task number']).reset_index(drop=True)
                        st.success(f"✅ Project tasks loaded: {len(self.project_tasks)} records")
                except Exception as e:
                    st.warning(f"Error loading Project tasks: {str(e)}")
            
            # Load Operations sheet with enhanced processing
            if 'Operations' in available_sheets:
                try:
                    self.operations_sheet = pd.read_excel(file, sheet_name='Operations')
                    st.success(f"✅ Operations sheet loaded: {len(self.operations_sheet)} rows, {len(self.operations_sheet.columns)} columns")
                    
                    # Process operations data with enhanced employee detection
                    self.operations_data = self._process_operations_data()
                    if self.operations_data:
                        st.success(f"✅ Operations data processed: {len(self.operations_data)} employee records")
                    
                except Exception as e:
                    st.error(f"Error loading Operations: {str(e)}")
                    st.code(traceback.format_exc())
            
            # Load Support sheet (employee names as column headers starting from column 5)
            if 'Support' in available_sheets:
                try:
                    self.support_sheet = pd.read_excel(file, sheet_name='Support')
                    st.success(f"✅ Support sheet loaded: {len(self.support_sheet)} records, {len(self.support_sheet.columns)} columns")
                    
                    # Process support data
                    self.support_data = self._process_support_data()
                    if self.support_data:
                        st.success(f"✅ Support data processed: {len(self.support_data)} employee records")
                    
                except Exception as e:
                    st.error(f"Error loading Support: {str(e)}")
                    st.code(traceback.format_exc())
            
            # Load ProdOps sheet (3 specific employee columns)
            if 'ProdOps' in available_sheets:
                try:
                    self.prodops_sheet = pd.read_excel(file, sheet_name='ProdOps')
                    st.success(f"✅ ProdOps sheet loaded: {len(self.prodops_sheet)} records, {len(self.prodops_sheet.columns)} columns")
                    
                    # Process ProdOps data
                    self.prodops_data = self._process_prodops_data()
                    if self.prodops_data:
                        st.success(f"✅ ProdOps data processed: {len(self.prodops_data)} employee records")
                    
                except Exception as e:
                    st.error(f"Error loading ProdOps: {str(e)}")
                    st.code(traceback.format_exc())
            
            # Load JIRA dumps
            if 'Jira Dumps Operations' in available_sheets:
                try:
                    self.operations_jira = pd.read_excel(file, sheet_name='Jira Dumps Operations')
                    st.success(f"✅ Operations JIRA loaded: {len(self.operations_jira)} records")
                except Exception as e:
                    st.warning(f"Operations JIRA error: {str(e)}")
            
            if 'Jira Dumps Support' in available_sheets:
                try:
                    self.support_jira = pd.read_excel(file, sheet_name='Jira Dumps Support')
                    st.success(f"✅ Support JIRA loaded: {len(self.support_jira)} records")
                except Exception as e:
                    st.warning(f"Support JIRA error: {str(e)}")
            
            # Load Competency Metrics sheet
            if 'Competency Metrics' in available_sheets:
                try:
                    self.competency_metrics = pd.read_excel(file, sheet_name='Competency Metrics')
                    st.success(f"✅ Competency metrics sheet loaded: {len(self.competency_metrics)} records")
                    
                    # Process competency data
                    self.competency_data = self._process_competency_data()
                    if self.competency_data:
                        st.success(f"✅ Competency data processed: {len(self.competency_data)} records")
                    
                except Exception as e:
                    st.error(f"Competency Metrics error: {str(e)}")
                    st.code(traceback.format_exc())
            
            # Load other sheets
            for sheet_name, attribute in [
                ('Training Dump', 'training_dump'),
                ('Effort', 'effort_data'),
                ('Training Metrics', 'training_metrics'),
                ('Get Distinct Names', 'distinct_names')
            ]:
                if sheet_name in available_sheets:
                    try:
                        setattr(self, attribute, pd.read_excel(file, sheet_name=sheet_name))
                        st.success(f"✅ {sheet_name} loaded")
                    except Exception as e:
                        st.warning(f"{sheet_name} error: {str(e)}")
            
            st.success("🎉 Excel file processed successfully!")
            return True
            
        except Exception as e:
            st.error(f"❌ Error loading Excel file: {str(e)}")
            st.code(traceback.format_exc())
            return False
    
    def _process_operations_data(self) -> List[Dict]:
        """Enhanced Operations sheet processing to properly identify employee columns"""
        if self.operations_sheet is None or self.operations_sheet.empty:
            return []
        
        try:
            operations_data = []
            
            # Debug: Show the structure of the Operations sheet
            st.info(f"Operations sheet shape: {self.operations_sheet.shape}")
            st.info(f"First few column names: {list(self.operations_sheet.columns[:10])}")
            
            # Find the "Status" column to determine where employee columns start
            status_col_idx = None
            for i, col in enumerate(self.operations_sheet.columns):
                col_str = str(col).strip().lower()
                if 'status' in col_str:
                    status_col_idx = i
                    st.info(f"Found Status column at index {i}: '{col}'")
                    break
            
            if status_col_idx is None:
                st.warning("Could not find 'Status' column in Operations sheet")
                return []
            
            # Employee columns start after the Status column
            employee_start_col = status_col_idx + 1
            employee_columns = []
            
            # Extract employee names from columns after Status
            for col_idx in range(employee_start_col, len(self.operations_sheet.columns)):
                col_name = self.operations_sheet.columns[col_idx]
                col_str = str(col_name).strip()
                
                # Filter out non-employee columns (Unnamed, NaN, empty, or status-like values)
                if (col_str and 
                    col_str != 'nan' and 
                    not pd.isna(col_name) and 
                    not col_str.startswith('Unnamed') and
                    len(col_str) > 3 and  # Employee names should be longer than 3 chars
                    not any(status_word in col_str.lower() for status_word in ['assigned', 'done', 'progress', 'due', 'overdue', 'not'])):
                    
                    # Additional check: see if this column contains actual employee data
                    sample_data = self.operations_sheet[col_name].dropna()
                    if len(sample_data) > 0:
                        # Check if the data looks like status values rather than employee names
                        sample_str = str(sample_data.iloc[0]).strip() if len(sample_data) > 0 else ""
                        if not any(status_word in sample_str.lower() for status_word in ['assigned', 'not assigned', 'done', 'progress', 'due']):
                            employee_columns.append(col_str)
            
            st.info(f"Found {len(employee_columns)} potential employee columns: {employee_columns[:5]}...")
            
            # If we couldn't find employee columns this way, try alternative approach
            if len(employee_columns) == 0:
                st.warning("Could not find employee columns after Status. Trying alternative approach...")
                
                # Look for columns that might contain actual employee names based on content
                for col_idx in range(employee_start_col, len(self.operations_sheet.columns)):
                    col_name = self.operations_sheet.columns[col_idx]
                    col_str = str(col_name).strip()
                    
                    if col_str and col_str != 'nan' and not pd.isna(col_name):
                        # Check if column header looks like a person's name (contains letters and spaces)
                        if re.match(r'^[A-Za-z\s\-\.]+$', col_str) and len(col_str) > 5:
                            employee_columns.append(col_str)
                
                st.info(f"Alternative approach found {len(employee_columns)} employee columns: {employee_columns[:5]}...")
            
            if len(employee_columns) == 0:
                st.error("No employee columns found in Operations sheet")
                return []
            
            # Get total training count (number of rows with training data)
            training_rows = self.operations_sheet.dropna(subset=['Status'] if 'Status' in self.operations_sheet.columns else [self.operations_sheet.columns[0]])
            total_trainings = len(training_rows)
            
            # Process each employee's data
            for employee_name in employee_columns:
                if employee_name in self.operations_sheet.columns:
                    employee_data = self.operations_sheet[employee_name].astype(str).str.strip()
                    
                    # Count specific status types as requested
                    assigned_done = sum(employee_data.str.contains('Assigned - Done', case=False, na=False))
                    assigned_progress = sum(employee_data.str.contains('Assigned - In Progress', case=False, na=False))  
                    assigned_due = sum(employee_data.str.contains('Assigned - Due', case=False, na=False))
                    
                    # Also count variations without "Assigned -" prefix
                    done_alt = sum(employee_data.str.contains('^Done$', case=False, na=False, regex=True))
                    progress_alt = sum(employee_data.str.contains('^In Progress$', case=False, na=False, regex=True))
                    due_alt = sum(employee_data.str.contains('^Due$', case=False, na=False, regex=True))
                    
                    # Combine counts
                    complete = assigned_done + done_alt
                    in_progress = assigned_progress + progress_alt  
                    due = assigned_due + due_alt
                    
                    # Count overdue and not assigned
                    overdue = sum(employee_data.str.contains('Overdue', case=False, na=False))
                    not_assigned = sum(employee_data.str.contains('Not Assigned|Not Required', case=False, na=False))
                    
                    # Only include employees with actual assignments
                    total_assigned = complete + in_progress + due + overdue
                    
                    if total_assigned > 0:
                        operations_data.append({
                            'Employee': employee_name,
                            'Team': 'Operations',
                            'Total_Trainings': total_trainings,
                            'Complete': complete,
                            'Due': due,
                            'OverDue': overdue,
                            'In_Progress': in_progress,
                            'Not_Assigned': not_assigned
                        })
                        
                        st.info(f"Employee: {employee_name} - Complete: {complete}, Due: {due}, In Progress: {in_progress}, Overdue: {overdue}")
            
            return operations_data
            
        except Exception as e:
            st.error(f"Error processing operations data: {str(e)}")
            st.code(traceback.format_exc())
            return []
    
    def _process_support_data(self) -> List[Dict]:
        """Process Support sheet data where employee names are column headers"""
        if self.support_sheet is None or self.support_sheet.empty:
            return []
        
        try:
            support_data = []
            
            # Get employee column names - they should be in row 0 (header row) starting from column 5
            employee_columns = []
            
            # Check the actual column headers
            for col_idx, col_name in enumerate(self.support_sheet.columns):
                if col_idx >= 5:  # Employee columns start from column 5
                    col_str = str(col_name).strip()
                    if (col_str and col_str != 'nan' and not pd.isna(col_name) and 
                        len(col_str) > 5 and not col_str.startswith('Unnamed')):
                        employee_columns.append(col_str)
            
            st.info(f"Found {len(employee_columns)} employee columns in Support: {employee_columns[:5]}...")
            
            # Get the total number of distinct training modules
            distinct_trainings = len(self.support_sheet)
            
            # Process each employee's data
            for employee in employee_columns:
                if employee in self.support_sheet.columns:
                    employee_data = self.support_sheet[employee].dropna()
                    
                    # Count statuses
                    complete = sum(employee_data.astype(str).str.contains('Complete', case=False, na=False))
                    due = sum(employee_data.astype(str).str.contains('Due', case=False, na=False))
                    in_progress = sum(employee_data.astype(str).str.contains('Progress', case=False, na=False))
                    
                    if len(employee_data) > 0:
                        support_data.append({
                            'Employee': employee,
                            'Team': 'Support',
                            'Total_Trainings': distinct_trainings,  # Same for all employees
                            'Complete': complete,
                            'Due': due,
                            'OverDue': 0,  # Support doesn't typically use overdue
                            'In_Progress': in_progress
                        })
            
            return support_data
            
        except Exception as e:
            st.error(f"Error processing support data: {str(e)}")
            st.code(traceback.format_exc())
            return []
    
    def _process_prodops_data(self) -> List[Dict]:
        """Process ProdOps sheet data with specific employee columns"""
        if self.prodops_sheet is None or self.prodops_sheet.empty:
            return []
        
        try:
            prodops_data = []
            
            # Expected ProdOps employee names (with variations)
            expected_employees = [
                'Iestyn Pettigrew', 
                'Fernando Macena', 
                'Jonathan Funnel',  # Without space
                'Jonathan Funnell', # With double 'l'
                'Jonathan Funnel ', # With space
            ]
            
            # Find the actual employee columns in the sheet
            employee_columns = []
            for col_name in self.prodops_sheet.columns:
                col_str = str(col_name).strip()
                if col_str and col_str != 'nan' and not pd.isna(col_name):
                    # Check for exact match first
                    if col_str in expected_employees:
                        employee_columns.append(col_str)
                    else:
                        # Check for partial matches
                        col_lower = col_str.lower()
                        if ('iestyn' in col_lower or 'pettigrew' in col_lower or
                            'fernando' in col_lower or 'macena' in col_lower or
                            'jonathan' in col_lower or 'funnel' in col_lower):
                            employee_columns.append(col_str)
            
            st.info(f"Found {len(employee_columns)} ProdOps employee columns: {employee_columns}")
            
            # Get the total number of distinct training modules
            distinct_trainings = len(self.prodops_sheet)
            
            # Process each employee's data
            for employee in employee_columns:
                if employee in self.prodops_sheet.columns:
                    employee_data = self.prodops_sheet[employee].dropna()
                    
                    # Count 1s (completed) and 0s (not completed)
                    complete = sum(employee_data == 1)
                    pending = sum(employee_data == 0)
                    
                    if len(employee_data) > 0:
                        prodops_data.append({
                            'Employee': employee,
                            'Team': 'ProdOps',
                            'Total_Trainings': distinct_trainings,  # Same for all employees
                            'Complete': complete,
                            'Due': pending,
                            'OverDue': 0,
                            'In_Progress': 0
                        })
            
            return prodops_data
            
        except Exception as e:
            st.error(f"Error processing ProdOps data: {str(e)}")
            st.code(traceback.format_exc())
            return []
    
    def _process_competency_data(self) -> List[Dict]:
        """Process Competency Metrics sheet data"""
        if self.competency_metrics is None or self.competency_metrics.empty:
            return []
        
        try:
            # Find the correct data starting row
            data_start_row = None
            headers = None
            
            for i, row in self.competency_metrics.iterrows():
                row_values = [str(val).strip() for val in row.values if pd.notna(val)]
                if len(row_values) >= 4:
                    if ('Employee' in row_values[0] and 'Competency' in row_values[1] and 
                        'Level' in row_values[2] and 'NumberOfStories' in row_values[3]):
                        headers = row_values[:4]
                        data_start_row = i + 1
                        break
                    elif (any('Employee' in str(val) for val in row_values) and 
                          any('Competency' in str(val) for val in row_values)):
                        # Try to find the correct columns
                        for j, val in enumerate(row_values):
                            if 'Employee' in str(val):
                                headers = row_values[j:j+4] if j+4 <= len(row_values) else row_values[j:]
                                data_start_row = i + 1
                                break
                        if headers:
                            break
            
            if data_start_row is None:
                # Try the second row as headers (common Excel format)
                if len(self.competency_metrics) > 1:
                    headers = ['Employee', 'Competency', 'Level', 'NumberOfStories']
                    data_start_row = 1
                else:
                    st.error("Could not find competency data headers")
                    return []
            
            st.info(f"Found competency headers at row {data_start_row-1}: {headers}")
            
            # Extract competency data
            comp_df = self.competency_metrics.iloc[data_start_row:].copy()
            
            # Set proper column names
            if len(comp_df.columns) >= 4:
                comp_df = comp_df.iloc[:, :4].copy()  # Take only first 4 columns
                comp_df.columns = ['Employee', 'Competency', 'Level', 'NumberOfStories']
                
                # Clean the data
                comp_df = comp_df.dropna(subset=['Employee']).reset_index(drop=True)
                comp_df = comp_df[comp_df['Employee'].astype(str).str.strip() != ''].reset_index(drop=True)
                
                # Convert NumberOfStories to numeric
                comp_df['NumberOfStories'] = pd.to_numeric(comp_df['NumberOfStories'], errors='coerce').fillna(0)
                
                # Add Team identification
                comp_df['Team'] = comp_df['Employee'].apply(self.identify_team_from_name)
                
                st.info(f"Processed competency data: {len(comp_df)} records")
                
                # Show team distribution
                team_dist = comp_df['Team'].value_counts()
                st.info(f"Team distribution: {team_dist.to_dict()}")
                
                return comp_df.to_dict('records')
            else:
                st.error("Competency sheet doesn't have enough columns")
                return []
            
        except Exception as e:
            st.error(f"Error processing competency data: {str(e)}")
            st.code(traceback.format_exc())
            return []
    
    def create_training_dashboard(self):
        """Create training metrics dashboard"""
        st.markdown('<h2 style="color: #1f77b4;">📊 Training Metrics Dashboard</h2>', unsafe_allow_html=True)
        
        # Check if we have any training data
        has_operations = self.operations_data and len(self.operations_data) > 0
        has_support = self.support_data and len(self.support_data) > 0  
        has_prodops = self.prodops_data and len(self.prodops_data) > 0
        
        if not (has_operations or has_support or has_prodops):
            st.warning("No training data available. Please load the Excel file with Operations, Support, or ProdOps sheets.")
            return
        
        # Calculate overall metrics properly
        all_training_data = []
        if has_operations:
            all_training_data.extend(self.operations_data)
        if has_support:
            all_training_data.extend(self.support_data)
        if has_prodops:
            all_training_data.extend(self.prodops_data)
        
        if all_training_data:
            # Calculate totals
            total_employees = len(all_training_data)
            total_complete_instances = sum(emp['Complete'] for emp in all_training_data)
            total_due_instances = sum(emp['Due'] for emp in all_training_data)
            total_overdue_instances = sum(emp['OverDue'] for emp in all_training_data)
            total_in_progress_instances = sum(emp['In_Progress'] for emp in all_training_data)
            
            # Overall metrics cards
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Employees", total_employees)
            with col2:
                st.metric("Completed Instances", total_complete_instances)
            with col3:
                st.metric("Due Instances", total_due_instances)
            with col4:
                st.metric("Overdue Instances", total_overdue_instances)
            with col5:
                st.metric("In Progress Instances", total_in_progress_instances)
            
            # Team summary
            st.markdown("### Team Training Overview")
            team_summary_data = []
            
            for team_name, team_data in [('Operations', self.operations_data), ('Support', self.support_data), ('ProdOps', self.prodops_data)]:
                if team_data and len(team_data) > 0:
                    distinct_trainings = team_data[0]['Total_Trainings']
                    team_employees = len(team_data)
                    team_complete_instances = sum(emp['Complete'] for emp in team_data)
                    avg_completion_rate = (team_complete_instances / (distinct_trainings * team_employees)) if distinct_trainings > 0 and team_employees > 0 else 0
                    
                    team_summary_data.append({
                        'Team': team_name,
                        'Employees': team_employees,
                        'Training_Modules': distinct_trainings,
                        'Completed_Instances': team_complete_instances,
                        'Avg_Completion_Rate': avg_completion_rate
                    })
            
            if team_summary_data:
                # Team comparison chart
                team_df = pd.DataFrame(team_summary_data)
                
                fig_team_comparison = px.bar(
                    team_df, x='Team', y='Avg_Completion_Rate',
                    title="Average Completion Rate by Team",
                    color='Avg_Completion_Rate',
                    color_continuous_scale='RdYlGn',
                    text='Completed_Instances'
                )
                fig_team_comparison.update_layout(yaxis_tickformat='.1%')
                fig_team_comparison.update_traces(texttemplate='%{text} instances', textposition='outside')
                st.plotly_chart(fig_team_comparison, use_container_width=True)
                
                # Team summary table
                display_df = team_df.copy()
                display_df['Avg_Completion_Rate'] = display_df['Avg_Completion_Rate'].apply(lambda x: f"{x:.1%}")
                st.dataframe(display_df, use_container_width=True)
            
            # Overall pie chart
            st.markdown("### Overall Training Status Distribution")
            fig_overall = px.pie(
                values=[total_complete_instances, total_in_progress_instances, total_due_instances, total_overdue_instances],
                names=['Complete', 'In Progress', 'Due', 'Overdue'],
                title="Training Status Distribution (All Instances)",
                color_discrete_map={
                    'Complete': '#28a745',
                    'In Progress': '#17a2b8', 
                    'Due': '#ffc107',
                    'Overdue': '#dc3545'
                }
            )
            st.plotly_chart(fig_overall, use_container_width=True)
        
        # Team-wise breakdown - replicated sections
        st.markdown("### Team Performance Breakdown")
        
        # Operations Section
        if has_operations:
            st.markdown("## 🔧 Operations Team")
            self._create_team_dashboard(self.operations_data, "Operations")
            st.divider()
        
        # Support Section
        if has_support:
            st.markdown("## 🎧 Support Team")
            self._create_team_dashboard(self.support_data, "Support")
            st.divider()
        
        # ProdOps Section
        if has_prodops:
            st.markdown("## ⚡ ProdOps Team")
            self._create_team_dashboard(self.prodops_data, "ProdOps")
            st.divider()
    
    def _create_team_dashboard(self, team_data: List[Dict], team_name: str):
        """Create individual team dashboard"""
        if not team_data:
            return
        
        team_df = pd.DataFrame(team_data)
        
        # Get distinct training count (should be same for all employees in the team)
        distinct_trainings = team_data[0]['Total_Trainings'] if team_data else 0
        
        # Team summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Team Members", len(team_data))
        with col2:
            st.metric("Total Training Modules", distinct_trainings)  # Distinct count
        with col3:
            total_complete = sum(emp['Complete'] for emp in team_data)
            # Calculate completion rate as average across all employees
            avg_completion_rate = sum(emp['Complete'] / distinct_trainings for emp in team_data) / len(team_data) if len(team_data) > 0 and distinct_trainings > 0 else 0
            st.metric("Avg Completion Rate", f"{avg_completion_rate:.1%}")
        
        # Individual performance stacked bar chart
        fig_individual = go.Figure()
        
        fig_individual.add_trace(go.Bar(
            name='Complete',
            x=team_df['Employee'],
            y=team_df['Complete'],
            marker_color='#28a745',
            text=team_df['Complete'],
            textposition='inside'
        ))
        
        fig_individual.add_trace(go.Bar(
            name='In Progress', 
            x=team_df['Employee'],
            y=team_df['In_Progress'],
            marker_color='#17a2b8',
            text=team_df['In_Progress'],
            textposition='inside'
        ))
        
        fig_individual.add_trace(go.Bar(
            name='Due',
            x=team_df['Employee'],
            y=team_df['Due'],
            marker_color='#ffc107',
            text=team_df['Due'],
            textposition='inside'
        ))
        
        if team_name == 'Operations':  # Only Operations has overdue
            fig_individual.add_trace(go.Bar(
                name='Overdue',
                x=team_df['Employee'],
                y=team_df['OverDue'],
                marker_color='#dc3545',
                text=team_df['OverDue'],
                textposition='inside'
            ))
        
        fig_individual.update_layout(
            barmode='stack',
            title=f'{team_name} Team - Individual Training Progress',
            xaxis_title='Employee',
            yaxis_title='Number of Trainings',
            height=500,
            xaxis_tickangle=45
        )
        
        st.plotly_chart(fig_individual, use_container_width=True)
        
        # Individual completion rates
        st.markdown(f"### {team_name} Team Individual Completion Rates")
        
        # Calculate individual completion rates
        individual_rates = []
        for emp in team_data:
            completion_rate = emp['Complete'] / distinct_trainings if distinct_trainings > 0 else 0
            individual_rates.append({
                'Employee': emp['Employee'],
                'Completed': emp['Complete'],
                'Total_Modules': distinct_trainings,
                'Completion_Rate': f"{completion_rate:.1%}",
                'Due': emp['Due'],
                'In_Progress': emp['In_Progress'],
                'Overdue': emp.get('OverDue', 0)
            })
        
        rates_df = pd.DataFrame(individual_rates)
        st.dataframe(rates_df, use_container_width=True)
        
        # Data table
        st.markdown(f"### {team_name} Team Training Details")
        st.dataframe(team_df, use_container_width=True)
    
    def create_competency_dashboard(self):
        """Create comprehensive competency analysis with Main vs New stacked charts"""
        st.markdown('<h2 style="color: #1f77b4;">🎯 Competency Analysis: Main vs New</h2>', unsafe_allow_html=True)
        
        if not self.competency_data or len(self.competency_data) == 0:
            st.warning("No competency data available. Please ensure the Excel file contains the Competency Metrics sheet with Employee, Competency, Level, and NumberOfStories columns.")
            return
        
        try:
            # Convert to DataFrame
            comp_df = pd.DataFrame(self.competency_data)
            
            # Filter out unknown teams
            comp_df_filtered = comp_df[comp_df['Team'] != 'Unknown']
            
            if comp_df_filtered.empty:
                st.warning("No team members could be identified in competency data.")
                st.info("Available employees in competency data:")
                unique_employees = comp_df['Employee'].unique()[:10]
                for emp in unique_employees:
                    st.write(f"• {emp}")
                return
            
            # Overall summary
            col1, col2 = st.columns(2)
            
            with col1:
                # Overall Main vs New distribution
                level_counts = comp_df_filtered['Level'].value_counts()
                fig_level = px.pie(
                    values=level_counts.values,
                    names=level_counts.index,
                    title="Overall Competency Level Distribution",
                    color_discrete_map={'Main': '#1f77b4', 'New': '#ff7f0e'}
                )
                st.plotly_chart(fig_level, use_container_width=True)
            
            with col2:
                # Team summary
                team_summary = comp_df_filtered.groupby(['Team', 'Level']).size().reset_index(name='Count')
                fig_team_summary = px.bar(
                    team_summary, x='Team', y='Count', color='Level',
                    title="Competency Levels by Team",
                    barmode='group',
                    color_discrete_map={'Main': '#1f77b4', 'New': '#ff7f0e'}
                )
                st.plotly_chart(fig_team_summary, use_container_width=True)
            
            # Operations Section
            st.markdown("## 🔧 Operations Team Competency Analysis")
            operations_data = comp_df_filtered[comp_df_filtered['Team'] == 'Operations']
            if not operations_data.empty:
                self._create_competency_team_section(operations_data, 'Operations')
            else:
                st.info("No Operations team members found in competency data")
            st.divider()
            
            # Support Section
            st.markdown("## 🎧 Support Team Competency Analysis")
            support_data = comp_df_filtered[comp_df_filtered['Team'] == 'Support']
            if not support_data.empty:
                self._create_competency_team_section(support_data, 'Support')
            else:
                st.info("No Support team members found in competency data")
            st.divider()
            
            # ProdOps Section (if data exists)
            prodops_data = comp_df_filtered[comp_df_filtered['Team'] == 'ProdOps']
            if not prodops_data.empty:
                st.markdown("## ⚡ ProdOps Team Competency Analysis")
                self._create_competency_team_section(prodops_data, 'ProdOps')
                st.divider()
            
            # Competency type heatmaps
            st.markdown("### 🔥 Competency Type Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Story count heatmap by competency type and level
                comp_type_stories = comp_df_filtered.groupby(['Competency', 'Level'])['NumberOfStories'].sum().reset_index()
                story_heatmap = comp_type_stories.pivot(
                    index='Competency', columns='Level', values='NumberOfStories'
                ).fillna(0)
                
                if not story_heatmap.empty:
                    fig_story_heatmap = px.imshow(
                        story_heatmap.values,
                        x=story_heatmap.columns,
                        y=story_heatmap.index,
                        title="Story Count Heatmap by Competency",
                        color_continuous_scale='RdYlGn',
                        aspect="auto",
                        text_auto=True
                    )
                    fig_story_heatmap.update_layout(height=400)
                    st.plotly_chart(fig_story_heatmap, use_container_width=True)
            
            with col2:
                # Employee count heatmap
                emp_count_comp = comp_df_filtered.groupby(['Competency', 'Level']).size().reset_index(name='Count')
                emp_heatmap = emp_count_comp.pivot(
                    index='Competency', columns='Level', values='Count'
                ).fillna(0)
                
                if not emp_heatmap.empty:
                    fig_emp_heatmap = px.imshow(
                        emp_heatmap.values,
                        x=emp_heatmap.columns,
                        y=emp_heatmap.index,
                        title="Employee Count Heatmap by Competency",
                        color_continuous_scale='viridis',
                        aspect="auto",
                        text_auto=True
                    )
                    fig_emp_heatmap.update_layout(height=400)
                    st.plotly_chart(fig_emp_heatmap, use_container_width=True)
            
            # Summary table
            st.markdown("### 📋 Competency Summary Table")
            
            # Create summary by employee
            emp_summary = comp_df_filtered.groupby(['Employee', 'Team']).agg({
                'Level': lambda x: f"Main: {sum(x=='Main')}, New: {sum(x=='New')}",
                'NumberOfStories': 'sum'
            }).reset_index()
            emp_summary.columns = ['Employee', 'Team', 'Competency_Breakdown', 'Total_Stories']
            emp_summary = emp_summary.sort_values(['Team', 'Total_Stories'], ascending=[True, False])
            
            # Team filter
            team_filter = st.selectbox("Filter by Team:", ['All'] + ['Operations', 'Support', 'ProdOps'])
            
            if team_filter != 'All':
                filtered_summary = emp_summary[emp_summary['Team'] == team_filter]
            else:
                filtered_summary = emp_summary
            
            st.dataframe(filtered_summary, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error creating competency dashboard: {str(e)}")
            st.code(traceback.format_exc())
    
    def _create_competency_team_section(self, team_data: pd.DataFrame, team_name: str):
        """Create competency analysis section for a specific team"""
        
        # Calculate competency counts per employee
        emp_comp_counts = team_data.groupby(['Employee', 'Level']).size().reset_index(name='Count')
        emp_comp_pivot = emp_comp_counts.pivot_table(
            index='Employee', columns='Level', values='Count', fill_value=0
        ).reset_index()
        
        # Ensure both Main and New columns exist
        if 'Main' not in emp_comp_pivot.columns:
            emp_comp_pivot['Main'] = 0
        if 'New' not in emp_comp_pivot.columns:
            emp_comp_pivot['New'] = 0
        
        if not emp_comp_pivot.empty:
            # Create stacked bar chart for competency counts
            fig_comp = go.Figure()
            
            fig_comp.add_trace(go.Bar(
                name='Main Competencies',
                x=emp_comp_pivot['Employee'],
                y=emp_comp_pivot['Main'],
                marker_color='#1f77b4',
                text=emp_comp_pivot['Main'],
                textposition='inside',
                texttemplate='%{text}',
                hovertemplate='<b>%{x}</b><br>Main: %{y}<extra></extra>'
            ))
            
            fig_comp.add_trace(go.Bar(
                name='New Competencies',
                x=emp_comp_pivot['Employee'],
                y=emp_comp_pivot['New'],
                marker_color='#ff7f0e',
                text=emp_comp_pivot['New'],
                textposition='inside',
                texttemplate='%{text}',
                hovertemplate='<b>%{x}</b><br>New: %{y}<extra></extra>'
            ))
            
            fig_comp.update_layout(
                barmode='stack',
                title=f'{team_name} Team: Main vs New Competency Count per Employee',
                xaxis_title='Employee',
                yaxis_title='Number of Competencies',
                height=500,
                xaxis_tickangle=45,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            st.plotly_chart(fig_comp, use_container_width=True)
        
        # Story count analysis
        story_analysis = team_data.groupby(['Employee', 'Level'])['NumberOfStories'].sum().reset_index()
        story_pivot = story_analysis.pivot_table(
            index='Employee', columns='Level', values='NumberOfStories', fill_value=0
        ).reset_index()
        
        # Ensure both columns exist
        if 'Main' not in story_pivot.columns:
            story_pivot['Main'] = 0
        if 'New' not in story_pivot.columns:
            story_pivot['New'] = 0
        
        if not story_pivot.empty:
            # Create stacked bar chart for story counts
            fig_stories = go.Figure()
            
            fig_stories.add_trace(go.Bar(
                name='Main Stories',
                x=story_pivot['Employee'],
                y=story_pivot['Main'],
                marker_color='#28a745',
                text=story_pivot['Main'].astype(int),
                textposition='inside',
                texttemplate='%{text}',
                hovertemplate='<b>%{x}</b><br>Main Stories: %{y}<extra></extra>'
            ))
            
            fig_stories.add_trace(go.Bar(
                name='New Stories',
                x=story_pivot['Employee'],
                y=story_pivot['New'],
                marker_color='#ffc107',
                text=story_pivot['New'].astype(int),
                textposition='inside',
                texttemplate='%{text}',
                hovertemplate='<b>%{x}</b><br>New Stories: %{y}<extra></extra>'
            ))
            
            fig_stories.update_layout(
                barmode='stack',
                title=f'{team_name} Team: Main vs New Story Count per Employee',
                xaxis_title='Employee',
                yaxis_title='Number of Stories',
                height=500,
                xaxis_tickangle=45,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom", 
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            st.plotly_chart(fig_stories, use_container_width=True)
        
        # Team summary metrics
        col_a, col_b, col_c, col_d = st.columns(4)
        
        main_count = len(team_data[team_data['Level'] == 'Main'])
        new_count = len(team_data[team_data['Level'] == 'New'])
        total_count = main_count + new_count
        
        main_stories = int(team_data[team_data['Level'] == 'Main']['NumberOfStories'].sum())
        new_stories = int(team_data[team_data['Level'] == 'New']['NumberOfStories'].sum())
        
        with col_a:
            st.metric("Main Competencies", main_count,
                     delta=f"{main_count/total_count:.1%}" if total_count > 0 else "0%")
        with col_b:
            st.metric("New Competencies", new_count,
                     delta=f"{new_count/total_count:.1%}" if total_count > 0 else "0%")
        with col_c:
            st.metric("Main Stories", main_stories)
        with col_d:
            st.metric("New Stories", new_stories)
    
    def create_jira_dashboard(self):
        """Create JIRA metrics dashboard"""
        st.markdown('<h2 style="color: #1f77b4;">📋 JIRA Metrics Dashboard</h2>', unsafe_allow_html=True)
        
        has_ops_jira = self.operations_jira is not None and not self.operations_jira.empty
        has_support_jira = self.support_jira is not None and not self.support_jira.empty
        
        if not (has_ops_jira or has_support_jira):
            st.info("No JIRA data available. Please load the Excel file with JIRA dump sheets.")
            return
        
        col1, col2 = st.columns(2)
        
        # Operations JIRA
        with col1:
            st.markdown("### Operations JIRA Metrics")
            
            if has_ops_jira:
                ops_total = len(self.operations_jira)
                ops_done = len(self.operations_jira[self.operations_jira['Status'] == 'Done'])
                ops_completion_rate = ops_done / ops_total if ops_total > 0 else 0
                
                # Metrics
                col1a, col1b, col1c = st.columns(3)
                with col1a:
                    st.metric("Total Tasks", ops_total)
                with col1b:
                    st.metric("Completed", ops_done)
                with col1c:
                    st.metric("Completion Rate", f"{ops_completion_rate:.1%}")
                
                # Status distribution
                status_counts = self.operations_jira['Status'].value_counts()
                fig_ops_status = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Operations JIRA Status"
                )
                st.plotly_chart(fig_ops_status, use_container_width=True)
                
                # Top assignees
                if 'Assignee' in self.operations_jira.columns:
                    assignee_counts = self.operations_jira['Assignee'].value_counts().head(10)
                    fig_assignees = px.bar(
                        x=assignee_counts.values,
                        y=assignee_counts.index,
                        orientation='h',
                        title="Top 10 Operations Assignees"
                    )
                    st.plotly_chart(fig_assignees, use_container_width=True)
            else:
                st.info("No Operations JIRA data")
        
        # Support JIRA
        with col2:
            st.markdown("### Support JIRA Metrics")
            
            if has_support_jira:
                support_total = len(self.support_jira)
                support_closed = len(self.support_jira[self.support_jira['Status'].isin(['Closed', 'Completed'])])
                support_closure_rate = support_closed / support_total if support_total > 0 else 0
                
                # Metrics
                col2a, col2b, col2c = st.columns(3)
                with col2a:
                    st.metric("Total Tickets", support_total)
                with col2b:
                    st.metric("Closed", support_closed)
                with col2c:
                    st.metric("Closure Rate", f"{support_closure_rate:.1%}")
                
                # Status distribution
                status_counts = self.support_jira['Status'].value_counts()
                fig_support_status = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="Support JIRA Status"
                )
                st.plotly_chart(fig_support_status, use_container_width=True)
                
                # Top assignees
                if 'Assignee' in self.support_jira.columns:
                    assignee_counts = self.support_jira['Assignee'].value_counts().head(10)
                    fig_assignees = px.bar(
                        x=assignee_counts.values,
                        y=assignee_counts.index,
                        orientation='h',
                        title="Top 10 Support Assignees"
                    )
                    st.plotly_chart(fig_assignees, use_container_width=True)
            else:
                st.info("No Support JIRA data")
    
    def create_data_overview(self):
        """Create data overview and export functionality"""
        st.markdown('<h2 style="color: #1f77b4;">📋 Data Overview & Export</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Loaded Data Summary")
            
            data_summary = []
            sheets_info = [
                ('Project Tasks', self.project_tasks),
                ('Operations Sheet', self.operations_sheet),
                ('Support Sheet', self.support_sheet),
                ('ProdOps Sheet', self.prodops_sheet),
                ('Operations JIRA', self.operations_jira),
                ('Support JIRA', self.support_jira),
                ('Competency Metrics', self.competency_metrics),
            ]
            
            # Processed data
            processed_info = [
                ('Operations Training Data', self.operations_data),
                ('Support Training Data', self.support_data),
                ('ProdOps Training Data', self.prodops_data),
                ('Competency Analysis Data', self.competency_data),
            ]
            
            for name, data in sheets_info + processed_info:
                if data is not None and len(data) > 0:
                    record_count = len(data) if hasattr(data, '__len__') else 'Available'
                    data_summary.append({
                        'Data Source': name,
                        'Records': record_count,
                        'Status': '✅ Loaded'
                    })
                else:
                    data_summary.append({
                        'Data Source': name,
                        'Records': 0,
                        'Status': '❌ Not Available'
                    })
            
            summary_df = pd.DataFrame(data_summary)
            st.dataframe(summary_df, use_container_width=True)
            
            # Debug information for troubleshooting
            with st.expander("🔍 Debug Information"):
                if self.operations_sheet is not None:
                    st.write("**Operations Sheet Column Headers:**")
                    ops_cols = list(self.operations_sheet.columns)
                    for i, col in enumerate(ops_cols):
                        st.write(f"  Column {i}: '{col}'")
                        if i > 15:  # Limit output
                            st.write(f"  ... and {len(ops_cols) - i - 1} more columns")
                            break
                
                if self.support_sheet is not None:
                    st.write("**Support Sheet Column Headers:**")
                    support_cols = list(self.support_sheet.columns)
                    for i, col in enumerate(support_cols):
                        if i >= 5:  # Show employee columns
                            st.write(f"  Column {i}: '{col}'")
                        if i > 15:  # Limit output
                            st.write(f"  ... and {len(support_cols) - i - 1} more columns")
                            break
                
                if self.prodops_sheet is not None:
                    st.write("**ProdOps Sheet Column Headers:**")
                    for i, col in enumerate(self.prodops_sheet.columns):
                        st.write(f"  Column {i}: '{col}'")
        
        with col2:
            st.markdown("### Export Options")
            
            if st.button("📥 Export Processed Data to Excel", type="primary"):
                try:
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        
                        # Export processed training data
                        if self.operations_data:
                            pd.DataFrame(self.operations_data).to_excel(writer, sheet_name='Operations_Training', index=False)
                        
                        if self.support_data:
                            pd.DataFrame(self.support_data).to_excel(writer, sheet_name='Support_Training', index=False)
                        
                        if self.prodops_data:
                            pd.DataFrame(self.prodops_data).to_excel(writer, sheet_name='ProdOps_Training', index=False)
                        
                        if self.competency_data:
                            pd.DataFrame(self.competency_data).to_excel(writer, sheet_name='Competency_Analysis', index=False)
                        
                        # Export raw sheets if available
                        raw_sheets = [
                            ('Raw_Operations', self.operations_sheet),
                            ('Raw_Support', self.support_sheet),
                            ('Raw_ProdOps', self.prodops_sheet),
                            ('Raw_Operations_JIRA', self.operations_jira),
                            ('Raw_Support_JIRA', self.support_jira),
                        ]
                        
                        for sheet_name, data in raw_sheets:
                            if data is not None and not data.empty:
                                data.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    st.download_button(
                        label="📄 Download Excel File",
                        data=output.getvalue(),
                        file_name=f"DTE_Processed_Metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    st.success("✅ Excel file prepared for download!")
                    
                except Exception as e:
                    st.error(f"❌ Error creating Excel file: {str(e)}")

def main():
    st.markdown('<h1 class="main-header">🚀 DTE Training Plan Metrics Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if 'processor' not in st.session_state:
        st.session_state.processor = DTEMetricsProcessor()
    
    processor = st.session_state.processor
    
    # Sidebar
    with st.sidebar:
        st.header("📁 Data Management")
        
        # File upload
        st.subheader("Upload Excel File")
        excel_file = st.file_uploader(
            "DTE Training Plan Metrics (Excel)", 
            type=['xlsx', 'xls'], 
            key="excel_file",
            help="Upload the complete DTE Training Plan Metrics Excel file"
        )
        
        # Load button
        if st.button("📊 Load Excel File", type="primary"):
            if excel_file:
                with st.spinner("Loading and processing Excel file..."):
                    success = processor.load_excel_file(excel_file)
                    if success:
                        st.success("🎉 Excel file loaded successfully!")
                        st.rerun()
            else:
                st.warning("Please upload the Excel file")
        
        st.divider()
        
        # Data status
        st.subheader("📈 Data Status")
        
        # Training data status
        if processor.operations_data:
            st.success(f"✅ Operations Training: {len(processor.operations_data)} employees")
        else:
            st.info("⏳ Operations Training: Not processed")
        
        if processor.support_data:
            st.success(f"✅ Support Training: {len(processor.support_data)} employees")
        else:
            st.info("⏳ Support Training: Not processed")
        
        if processor.prodops_data:
            st.success(f"✅ ProdOps Training: {len(processor.prodops_data)} employees")
        else:
            st.info("⏳ ProdOps Training: Not processed")
        
        if processor.competency_data:
            st.success(f"✅ Competency Data: {len(processor.competency_data)} records")
        else:
            st.info("⏳ Competency Data: Not processed")
        
        # JIRA data status
        if processor.operations_jira is not None:
            st.success(f"✅ Operations JIRA: {len(processor.operations_jira)} records")
        else:
            st.info("⏳ Operations JIRA: Not loaded")
        
        if processor.support_jira is not None:
            st.success(f"✅ Support JIRA: {len(processor.support_jira)} records")
        else:
            st.info("⏳ Support JIRA: Not loaded")
    
    # Main content
    has_training_data = any([
        processor.operations_data,
        processor.support_data,
        processor.prodops_data
    ])
    
    has_competency_data = processor.competency_data is not None
    has_jira_data = any([
        processor.operations_jira is not None,
        processor.support_jira is not None
    ])
    
    if has_training_data or has_competency_data or has_jira_data:
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Training Metrics", "🎯 Competency Analysis", "📋 JIRA Metrics", "📁 Data Overview"
        ])
        
        with tab1:
            try:
                processor.create_training_dashboard()
            except Exception as e:
                st.error(f"Error in training dashboard: {str(e)}")
                st.code(traceback.format_exc())
        
        with tab2:
            try:
                processor.create_competency_dashboard()
            except Exception as e:
                st.error(f"Error in competency dashboard: {str(e)}")
                st.code(traceback.format_exc())
        
        with tab3:
            try:
                processor.create_jira_dashboard()
            except Exception as e:
                st.error(f"Error in JIRA dashboard: {str(e)}")
                st.code(traceback.format_exc())
        
        with tab4:
            try:
                processor.create_data_overview()
            except Exception as e:
                st.error(f"Error in data overview: {str(e)}")
                st.code(traceback.format_exc())
    
    else:
        # Welcome screen
        st.markdown("""
        ## Welcome to the DTE Training Plan Metrics Dashboard! 🎉
        
        ### 📊 **Training Metrics Analysis**
        - **Operations Team Training** - Individual employee progress tracking
        - **Support Team Training** - Completion status by employee  
        - **ProdOps Team Training** - Specialized team metrics
        - **Stacked Bar Charts** - Visual progress representation
        
        ### 🎯 **Competency Analysis**
        - **Main vs New Competencies** - Individual employee breakdown
        - **Story Count Analysis** - Performance metrics by competency level
        - **Team-based Stacked Charts** - Separate visualizations for Operations, Support, and ProdOps
        - **Interactive Heatmaps** - Competency distribution analysis
        
        ### 📋 **JIRA Integration**
        - **Operations JIRA Tasks** - Task completion tracking
        - **Support JIRA Tickets** - Ticket closure analysis
        - **Assignee Performance** - Individual workload metrics
        
        ### 🚀 **Key Features:**
        
        #### **Enhanced Data Processing:**
        - Reads employee names from Excel column headers
        - Processes Operations, Support, and ProdOps sheets accurately
        - Handles competency data with proper team identification
        - Robust error handling and data validation
        
        #### **Visual Analytics:**
        - **Stacked bar charts** showing Main vs New competencies per employee
        - **Team-separated analysis** for Operations, Support, and ProdOps
        - **Interactive dashboards** with filtering capabilities
        - **Professional styling** with modern UI components
        
        ### 📁 **How to Use:**
        
        1. **Upload your DTE Training Plan Metrics.xlsx file**
        2. **Click "Load Excel File"** to process all sheets automatically
        3. **Navigate through tabs** to explore different analytics
        4. **Use filters and interactive charts** to drill down into specific data
        5. **Export processed data** for reporting and sharing
        
        ---
        
        **Ready to load your Excel file and explore comprehensive training and competency analytics!** 📊✨
        """)

if __name__ == "__main__":
    main()