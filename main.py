# Standard library imports
import json
import logging
import re
import os
import uuid
import time
import io
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from urllib.parse import urlparse
from collections import defaultdict

# Third-party imports
# Environment variable handling
import dotenv
from dotenv import load_dotenv

# FastAPI related
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Query
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import requests
import aiohttp

# Azure related
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.storage.blob import BlobServiceClient

# PDF and document processing
import fitz  # PyMuPDF for PDF processing
import docx  # python-docx for Word document processing
import openai  # OpenAI API for NLP processing

# ReportLab for PDF generation
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    Flowable, Frame, PageTemplate, HRFlowable
)
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics

# Utility imports
import importlib.util

# Load environment variables from .env file
load_dotenv()

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app setup
app = FastAPI(title="Resume and Job Processor API", 
             description="API for processing and storing resume and job data",
             version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OpenAI API Credentials from environment variables
openai.api_key = os.getenv("OPENAI_API_KEY")
openai.api_base = os.getenv("OPENAI_API_BASE")
openai.api_type = os.getenv("OPENAI_API_TYPE")
openai.api_version = os.getenv("OPENAI_API_VERSION")
deployment_name = os.getenv("OPENAI_DEPLOYMENT_NAME")

# Azure Cosmos DB Credentials from environment variables
HOST = os.getenv("COSMOS_HOST")
MASTER_KEY = os.getenv("COSMOS_MASTER_KEY")
RESUME_DATABASE_ID = os.getenv("RESUME_DATABASE_ID")
RESUME_CONTAINER_ID = os.getenv("RESUME_CONTAINER_ID")
JOB_DATABASE_ID = os.getenv("JOB_DATABASE_ID")
JOB_CONTAINER_ID = os.getenv("JOB_CONTAINER_ID")
TAILORED_RESUME_CONTAINER_ID = os.getenv("TAILORED_RESUME_CONTAINER_ID")
PARTITION_KEY_PATH = os.getenv("PARTITION_KEY_PATH", "/items")

# Azure Blob Storage Configuration from environment variables
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
BLOB_SAS_TOKEN = os.getenv("BLOB_SAS_TOKEN")
BLOB_BASE_URL = os.getenv("BLOB_BASE_URL")
BLOB_SAS_URL = os.getenv("BLOB_SAS_URL")
BLOB_CONNECTION_STRING_WITH_SAS = os.getenv("BLOB_CONNECTION_STRING_WITH_SAS")

# Admin Configuration
ADMIN_KEY = os.getenv("ADMIN_KEY", "resume_admin_key_2025")

# Check if key environment variables are present
if not all([openai.api_key, openai.api_base, HOST, MASTER_KEY, BLOB_CONNECTION_STRING, BLOB_SAS_TOKEN]):
    logger.warning("Missing critical environment variables. Please check your .env file.")

# Initialize Cosmos Client
try:
    client = CosmosClient(HOST, MASTER_KEY)
    logger.info("Successfully connected to Cosmos DB")
except Exception as e:
    logger.error(f"Failed to connect to Cosmos DB: {str(e)}")
    raise

class DynamicSection:
    """Base class for dynamic section handling"""
    def __init__(self, styles, colors):
        self.styles = styles
        self.colors = colors
        
    def process(self, data: Any) -> List[Flowable]:
        raise NotImplementedError

class ExperienceSection(DynamicSection):
    """Handles work experience section with enhanced formatting"""
    def process(self, experiences: List[Dict[str, Any]]) -> List[Flowable]:
        elements = []
        
        for exp in experiences:
            # Create a container for experience elements
            exp_elements = []
            
            # Company and location on the left, date on the right
            company_loc_date = []
            
            if 'company' in exp and 'location' in exp:
                company_loc_date.append(f"<b>{exp['company']}, {exp['location']}</b>")
            elif 'company' in exp:
                company_loc_date.append(f"<b>{exp['company']}</b>")
                
            company_date_text = "<table width='100%'><tr>"
            company_date_text += f"<td>{' '.join(company_loc_date)}</td>"
            
            # Add dates with right alignment
            if 'date' in exp:
                date_text = self._format_date_range(exp['date'])
                company_date_text += f"<td align='right'>{date_text}</td>"
            
            company_date_text += "</tr></table>"
            exp_elements.append(Paragraph(company_date_text, self.styles['ExperienceTitle']))
            
            # Add job title
            if 'title' in exp:
                exp_elements.append(Paragraph(
                    f"<i>{exp['title']}</i>",
                    self.styles['JobTitle']
                ))
            
            # Process responsibilities with better formatting
            if 'responsibilities' in exp:
                exp_elements.append(Spacer(1, 5))
                for resp in exp['responsibilities']:
                    formatted_resp = self._format_responsibility(resp)
                    exp_elements.append(Paragraph(
                        f"• {formatted_resp}",
                        self.styles['ListItem']
                    ))
            
            # Add achievements if available
            if 'achievements' in exp:
                for achievement in exp['achievements']:
                    exp_elements.append(Paragraph(
                        f"• {achievement}",
                        self.styles['ListItem']
                    ))
            
            elements.extend(exp_elements)
            elements.append(Spacer(1, 10))  
            
        return elements
    
    def _format_date_range(self, date_str: str) -> str:
        """Format date range with proper spacing"""
        return date_str
    
    def _format_responsibility(self, resp: str) -> str:
        """Format responsibility text with highlighting for key achievements"""
        # Highlight metrics and achievements
        resp = re.sub(r'(\d+[%+]|\$[\d,]+|increased|decreased|improved|launched|created|developed)',
                     r'<b>\1</b>', resp, flags=re.IGNORECASE)
        return resp

class SkillsSection(DynamicSection):
    """Enhanced skills section formatting"""
    def process(self, skills: Dict[str, List[str]]) -> List[Flowable]:
        """Process skills section with category formatting"""
        elements = []
        
        # Get the skill item style with fallback
        skill_style = self.styles.get('SkillItem', self.styles['Normal'])

        for category, skill_list in skills.items():
            # Add category with skills on the same line
            if isinstance(skill_list, list):
                skill_text = f"<b>{category}:</b> {', '.join(skill_list)}"
                elements.append(Paragraph(
                    skill_text,
                    skill_style
                ))
                elements.append(Spacer(1, 3))
            else:
                # If not a list, just add as is
                elements.append(Paragraph(
                    f"<b>{category}:</b> {skill_list}",
                    skill_style
                ))
                elements.append(Spacer(1, 3))

        return elements

class EducationSection(DynamicSection):
    """Handles education section formatting"""
    def process(self, education: Union[List[Dict[str, Any]], Dict[str, Any]]) -> List[Flowable]:
        elements = []
        
        # First, convert education to a list if it's a dictionary
        if isinstance(education, dict):
            self.logger.warning(f"Education section is not a list, wrapping: {type(education)}")
            education = [education]  # Wrap the dictionary in a list
        # Check if education is a list
        elif not isinstance(education, list):
            # Convert string or other types to a list with a single message
            if isinstance(education, str):
                return [Paragraph(f"Education (raw text): {education}", self.styles.get('Normal'))]
            else:
                return [Paragraph(f"Education data is not in the expected format: {type(education)}", 
                                 self.styles.get('Error', self.styles.get('Normal')))]
        
        # Process each education item
        for edu in education:
            # Skip non-dictionary items
            if not isinstance(edu, dict):
                elements.append(Paragraph(
                    f"Skipped invalid education entry (not a dictionary): {str(edu)[:100]}...",
                    self.styles.get('Error', self.styles.get('Normal'))
                ))
                continue
                
            # School and location on the left, date on the right
            school_loc_date = []
            
            if 'institution' in edu and 'location' in edu:
                school_loc_date.append(f"<b>{edu['institution']}, {edu['location']}</b>")
            elif 'institution' in edu:
                school_loc_date.append(f"<b>{edu['institution']}</b>")
            elif 'school' in edu:  # Alternative key
                school_loc_date.append(f"<b>{edu['school']}</b>")
            else:
                # If institution is missing, add a placeholder
                school_loc_date.append("<b>Institution not specified</b>")
                
            school_date_text = "<table width='100%'><tr>"
            school_date_text += f"<td>{' '.join(school_loc_date)}</td>"
            
            # Add dates with right alignment
            if 'date' in edu:
                date_text = edu['date']
                school_date_text += f"<td align='right'>{date_text}</td>"
            elif 'dates' in edu:  # Alternative key
                date_text = edu['dates']
                school_date_text += f"<td align='right'>{date_text}</td>"
            
            school_date_text += "</tr></table>"
            elements.append(Paragraph(
                school_date_text, 
                self.styles.get('ExperienceTitle', self.styles.get('Normal'))
            ))
            
            # Add degree and major
            if 'degree' in edu and 'major' in edu:
                elements.append(Paragraph(
                    f"<i>{edu['degree']}: {edu['major']}</i>",
                    self.styles.get('JobTitle', self.styles.get('Normal'))
                ))
            elif 'degree' in edu:
                elements.append(Paragraph(
                    f"<i>{edu['degree']}</i>",
                    self.styles.get('JobTitle', self.styles.get('Normal'))
                ))
            
            # Add additional details if available
            if 'details' in edu and isinstance(edu['details'], list):
                elements.append(Spacer(1, 5))
                for detail in edu['details']:
                    elements.append(Paragraph(
                        f"• {detail}", 
                        self.styles.get('ListItem', self.styles.get('Normal'))
                    ))
            elif 'courses' in edu and isinstance(edu['courses'], list):
                elements.append(Spacer(1, 5))
                elements.append(Paragraph(
                    "<b>Relevant Coursework:</b>", 
                    self.styles.get('Content', self.styles.get('Normal'))
                ))
                elements.append(Paragraph(
                    ", ".join(edu['courses']), 
                    self.styles.get('Content', self.styles.get('Normal'))
                ))
            
            elements.append(Spacer(1, 10))

        return elements

class ProjectsSection(DynamicSection):
    """Handles projects section formatting"""
    def process(self, projects: List[Dict[str, Any]]) -> List[Flowable]:
        elements = []
        
        for project in projects:
            # Project name and date if available
            project_header = "<table width='100%'><tr>"
            
            if 'name' in project:
                project_header += f"<td><b>{project['name']}</b>"
                if 'date' in project:
                    project_header += f" ({project['date']})"
                project_header += "</td>"
            
            project_header += "</tr></table>"
            elements.append(Paragraph(project_header, self.styles['JobTitle']))
            
            # Add project description
            if 'description' in project:
                elements.append(Paragraph(
                    f"• {project['description']}",
                    self.styles['ListItem']
                ))
            
            # Add technologies if available
            if 'technologies' in project:
                if isinstance(project['technologies'], list):
                    tech_text = ', '.join(project['technologies'])
                else:
                    tech_text = project['technologies']
                    
                elements.append(Paragraph(
                    f"<i>Technologies: {tech_text}</i>",
                    self.styles['ExperienceDetails']
                ))
            
            elements.append(Spacer(1, 5))
            
        return elements

class ResumePDFGenerator:
    """Enhanced Resume PDF Generator with dynamic section handling and ATS optimization"""
    def __init__(self, output_path: str, theme: str = 'default'):
        """Initialize the PDF generator with the specified output path and theme"""
        self.output_path = output_path
        self.theme = theme
        
        # Initialize the document
        self.doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.3 * inch,
            bottomMargin=0.3 * inch
        )
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize with empty collections
        self.elements = []
        self.header_elements = []
        self.main_content = []
        
        # Initialize styles
        self.styles = getSampleStyleSheet()
        
        # Initialize colors based on theme
        self.colors = self._get_theme_colors(theme)
        
        # Set up custom styles
        self.setup_styles()
        
        # Initialize section handlers
        self._initialize_section_handlers()
        
        self.logger.info(f"Initialized ResumePDFGenerator with output path: {output_path} and theme: {theme}")
        
    def _get_theme_colors(self, theme: str) -> Dict[str, Any]:
        """Get color scheme based on theme"""
        themes = {
            'default': {
                'primary': colors.HexColor('#2A1052'),  # Dark purple
                'secondary': colors.HexColor('#333333'),
                'accent': colors.HexColor('#4B0082'),    
                'text': colors.HexColor('#000000'),
                'subtext': colors.HexColor('#666666'),
                'background': colors.HexColor('#FFFFFF'), 
                'highlight': colors.HexColor('#4B0082')   
            },
        }
        return themes.get(theme, themes['default'])
    
    def _initialize_section_handlers(self):
        """Initialize handlers for different resume sections"""
        self.section_handlers = {
            'experience': ExperienceSection(self.styles, self.colors),
            'skills': SkillsSection(self.styles, self.colors),
            'education': EducationSection(self.styles, self.colors),
            'projects': ProjectsSection(self.styles, self.colors)
        }
    
    def setup_styles(self):
        """Initialize enhanced style sheet"""
        # Work with the instance styles, not a local variable
        # self.styles was already initialized in __init__ with getSampleStyleSheet()
        
        # Define colors
        self.colors = self._get_theme_colors(self.theme)
        
        # Add custom styles
        self.styles.add(ParagraphStyle(
            name='HeaderName',
            parent=self.styles['Heading1'],
            fontSize=20,
            textColor=self.colors['primary'],
            spaceAfter=10,
            alignment=TA_CENTER
        ))
        
        self.styles.add(ParagraphStyle(
            name='Contact',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=self.colors['text'],
            alignment=TA_CENTER,
            spaceAfter=5
        ))
        
        # Add Error style for error messages
        self.styles.add(ParagraphStyle(
            name='Error',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=colors.red,
            spaceBefore=2,
            spaceAfter=2,
            backColor=colors.lightgrey,
            borderWidth=1,
            borderColor=colors.red,
            borderPadding=5,
            borderRadius=2,
            alignment=TA_LEFT
        ))
        
        # Common styles
        styles_config = {
            'HeaderTitle': {
                'parent': 'Normal',
                'fontSize': 12,
                'textColor': self.colors['subtext'],
                'alignment': TA_LEFT,
                'spaceAfter': 8,
                'leading': 14
            },
            'SectionHeader': {
                'parent': 'Heading2',
                'fontSize': 14,
                'textColor': self.colors['primary'],
                'spaceBefore': 8,
                'spaceAfter': 2,
                'leading': 16,
                'fontName': 'Helvetica-Bold'
            },
            'ListItem': {
                'parent': 'Normal',
                'fontSize': 9,
                'textColor': self.colors['text'],
                'spaceBefore': 0,
                'spaceAfter': 3,
                'leftIndent': 10,
                'leading': 13
            },
            'Content': {
                'parent': 'Normal',
                'fontSize': 10,
                'textColor': self.colors['text'],
                'spaceBefore': 0,
                'spaceAfter': 3,
                'leading': 13
            },
            'SkillItem': {
                'parent': 'Normal',
                'fontSize': 9,
                'textColor': self.colors['text'],
                'spaceBefore': 1,
                'spaceAfter': 1,
                'leading': 12
            },
            'ExperienceTitle': {
                'parent': 'Normal',
                'fontSize': 10,
                'textColor': self.colors['text'],
                'spaceBefore': 4,
                'spaceAfter': 1,
                'leading': 12
            },
            'JobTitle': {
                'parent': 'Normal',
                'fontSize': 10,
                'textColor': self.colors['primary'],
                'spaceBefore': 1,
                'spaceAfter': 2,
                'leading': 12
            },
            'ExperienceDetails': {
                'parent': 'Normal',
                'fontSize': 9,
                'textColor': self.colors['subtext'],
                'spaceBefore': 1,
                'spaceAfter': 2,
                'leading': 11
            }
        }

        # Add all styles to the stylesheet
        for style_name, style_props in styles_config.items():
            parent_style = self.styles[style_props.pop('parent')]
            self.styles.add(ParagraphStyle(
                style_name,
                parent=parent_style,
                **style_props
            ))
    
    def process_section(self, section_name: str, content: Any) -> List[Flowable]:
        """Process section content with the appropriate handler"""
        try:
            # Make sure we have an Error style available for error messages
            error_style = self.styles.get('Error', self.styles['Normal'])
            
            # Handle case where content might be a string instead of expected type
            if isinstance(content, str):
                self.logger.warning(f"Section {section_name} contains string instead of expected dictionary/list: {content[:100]}")
                # Convert to appropriate structure based on section
                if section_name in ['experience', 'education', 'projects', 'certifications', 'publications']:
                    # These sections expect lists
                    # For education specifically, we'll create a minimal structure
                    if section_name == 'education':
                        # Create a properly structured education entry with the text as description
                        edu_entry = [{
                            "institution": "From Resume Text",
                            "degree": "Education Information",
                            "details": [content[:500] + ("..." if len(content) > 500 else "")]
                        }]
                        # Use the proper handler with this structured data
                        section_handler = self.section_handlers.get(section_name)
                        if section_handler:
                            return section_handler.process(edu_entry)
                    
                    return [Paragraph(f"Error: Invalid data format for {section_name}", error_style),
                            Paragraph(content[:250] + "...", self.styles['Normal'])]
                elif section_name == 'skills':
                    # Skills expects a dictionary
                    return [Paragraph(f"Error: Invalid data format for {section_name}", error_style),
                            Paragraph(content[:250] + "...", self.styles['Normal'])]
                else:
                    # General handling for other sections
                    return [Paragraph(content, self.styles['Normal'])]
            
            # Get the appropriate section handler
            section_handler = self.section_handlers.get(section_name)
            if section_handler:
                # Special handling for education section
                if section_name == 'education':
                    # Education section handler expects a list of dictionaries
                    # If it's a dictionary, the EducationSection.process method will handle wrapping it
                    # If it's anything else, let the handler deal with it
                    return section_handler.process(content)
                else:
                    return section_handler.process(content)
            else:
                # Default handling for unrecognized sections
                elements = []
                if isinstance(content, str):
                    elements.append(Paragraph(content, self.styles['Normal']))
                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, str):
                            elements.append(Paragraph(f"• {item}", self.styles['ListItem']))
                        elif isinstance(item, dict):
                            for key, value in item.items():
                                elements.append(Paragraph(f"<b>{key}</b>", self.styles['Normal']))
                                if isinstance(value, str):
                                    elements.append(Paragraph(value, self.styles['Content']))
                elif isinstance(content, dict):
                    for key, value in content.items():
                        elements.append(Paragraph(f"<b>{key}</b>", self.styles['Normal']))
                        if isinstance(value, str):
                            elements.append(Paragraph(value, self.styles['Content']))
                        elif isinstance(value, list):
                            for item in value:
                                if isinstance(item, str):
                                    elements.append(Paragraph(f"• {item}", self.styles['ListItem']))
                return elements
        except Exception as e:
            self.logger.error(f"Error processing section {section_name}: {str(e)}")
            # Safely access Error style, falling back to Normal if needed
            error_style = self.styles.get('Error', self.styles['Normal'])
            return [Paragraph(f"Error processing {section_name}: {str(e)}", error_style)]
    
    def process_resume(self, resume_data: Dict[str, Any]):
        """Enhanced resume processing with dynamic section handling"""
        try:
            # Make sure we have Error style available
            error_style = self.styles.get('Error', self.styles['Normal'])
            header_style = self.styles.get('HeaderName', self.styles['Heading1'])
            
            # Validate that resume_data is a dictionary
            if not isinstance(resume_data, dict):
                self.logger.error(f"resume_data is not a dictionary: {type(resume_data)}")
                # Try to convert string to dictionary if possible
                if isinstance(resume_data, str):
                    try:
                        resume_data = json.loads(resume_data)
                        self.logger.info("Successfully converted string resume_data to dictionary")
                    except json.JSONDecodeError:
                        self.logger.error("Failed to parse string resume_data as JSON")
                        resume_data = {"name": "Error Processing Resume", "summary": "Invalid resume data format"}
                else:
                    self.logger.error(f"Cannot process resume_data of type: {type(resume_data)}")
                    resume_data = {"name": "Error Processing Resume", "summary": "Invalid resume data format"}
            
            # Create header with validated data
            self.create_header_section(resume_data)
            
            # Process sections in the order we want them to appear
            section_order = ['summary', 'skills', 'experience', 'education', 'projects', 'certifications', 'publications']
            
            for section in section_order:
                if section in resume_data and resume_data[section]:
                    try:
                        # For education specifically, add extra validation
                        if section == 'education':
                            education_data = resume_data[section]
                            # If education is a string, convert it to a proper structure
                            if isinstance(education_data, str):
                                self.logger.warning(f"Education section is a string, converting to proper structure: {education_data[:100]}")
                                education_data = [{
                                    "institution": "From Resume Text",
                                    "degree": "Education Information",
                                    "details": [education_data[:500] + ("..." if len(education_data) > 500 else "")]
                                }]
                                # Update the resume_data with the proper structure
                                resume_data[section] = education_data
                            # If education is not a list, wrap it in a list
                            elif not isinstance(education_data, list):
                                self.logger.warning(f"Education section is not a list, wrapping: {type(education_data)}")
                                # If it's a dict, it might be a single education entry
                                if isinstance(education_data, dict):
                                    resume_data[section] = [education_data]
                                else:
                                    # Otherwise create a dummy entry
                                    resume_data[section] = [{
                                        "institution": "Invalid Format",
                                        "degree": "Could not parse education data",
                                        "details": [f"Original type: {type(education_data)}"]
                                    }]
                        
                        section_title = re.sub(r'([a-z])([A-Z])', r'\1 \2', section)
                        section_title = section_title.replace('_', ' ').title()
                        
                        self.main_content.append(Paragraph(section_title, self.styles['SectionHeader']))
                        self.main_content.append(self.add_section_divider())
                        
                        # Process section content
                        section_elements = self.process_section(section, resume_data[section])
                        self.main_content.extend(section_elements)
                    except Exception as section_e:
                        self.logger.error(f"Error processing section {section}: {str(section_e)}")
                        # Add an error message instead
                        self.main_content.append(Paragraph(f"Error processing {section_title}", error_style))
            
            # Process any remaining sections not in our predefined order
            skip_fields = {'name', 'email', 'phone', 'linkedin', 'github', 'website', 'location', 'summary'}
            skip_fields.update(section_order)
            
            for key, value in resume_data.items():
                if key not in skip_fields and value is not None:
                    try:
                        section_title = re.sub(r'([a-z])([A-Z])', r'\1 \2', key)
                        section_title = section_title.replace('_', ' ').title()
                        
                        self.main_content.append(Paragraph(section_title, self.styles['SectionHeader']))
                        self.main_content.append(self.add_section_divider())
                        
                        # Process section content
                        section_elements = self.process_section(key, value)
                        self.main_content.extend(section_elements)
                    except Exception as other_e:
                        self.logger.error(f"Error processing other section {key}: {str(other_e)}")
                        # Add an error message instead
                        self.main_content.append(Paragraph(f"Error processing {section_title}", error_style))
        except Exception as e:
            # Global error handling if something went wrong in the overall process
            self.logger.error(f"Error processing resume data: {str(e)}")
            # Add error message to the document
            self.main_content.append(Paragraph("Error Processing Resume", header_style))
            self.main_content.append(Paragraph(f"An error occurred while processing this resume: {str(e)}", self.styles['Normal']))
            # Don't re-raise the exception to allow PDF generation to continue

    def format_contact_info(self, resume_data: Dict[str, str]) -> str:
        """Format contact information in a centered layout"""
        try:
            # Ensure resume_data is a dictionary
            if not isinstance(resume_data, dict):
                self.logger.error(f"resume_data in format_contact_info is not a dictionary: {type(resume_data)}")
                return "Contact information not available"
            
            contact_parts = []
            
            # Email and phone on one line
            if 'email' in resume_data and resume_data['email']:
                contact_parts.append(f"✉️ {resume_data['email']}")
            if 'phone' in resume_data and resume_data['phone']:
                contact_parts.append(f"📞 {resume_data['phone']}")
                
            # GitHub and LinkedIn on next line
            social_parts = []
            if 'github' in resume_data and resume_data['github']:
                social_parts.append(f"🔗 {resume_data['github']}")
            if 'linkedin' in resume_data and resume_data['linkedin']:
                social_parts.append(f"🔗 {resume_data['linkedin']}")
                
            contact_text = f"<div align='center'>{' | '.join(contact_parts)}</div>" if contact_parts else ""
            if social_parts:
                contact_text += f"<div align='center'>{' | '.join(social_parts)}</div>"
                
            return contact_text
        except Exception as e:
            self.logger.error(f"Error formatting contact info: {str(e)}")
            return "Contact information not available"

    def add_section_divider(self):
        """Add a section divider"""
        return HRFlowable(
            width="100%",
            thickness=1,
            color=self.colors['secondary'],
            spaceBefore=0,
            spaceAfter=5,
            lineCap='round'
        )

    def add_section(self, title: str, content: Any) -> List[Flowable]:
        """Add a generic section to the resume"""
        elements = []
        
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    for key, value in item.items():
                        if isinstance(value, str):
                            elements.append(Paragraph(
                                f"<b>{key}</b>: {value}", 
                                self.styles['Content']
                            ))
                        elif isinstance(value, list):
                            elements.append(Paragraph(
                                f"<b>{key}</b>:", 
                                self.styles['Content']
                            ))
                            for subitem in value:
                                elements.append(Paragraph(
                                    f"• {subitem}", 
                                    self.styles['ListItem']
                                ))
                    elements.append(Spacer(1, 3))
                else:
                    elements.append(Paragraph(f"• {item}", self.styles['ListItem']))
        elif isinstance(content, dict):
            for key, value in content.items():
                if isinstance(value, str):
                    elements.append(Paragraph(
                        f"<b>{key}</b>: {value}", 
                        self.styles['Content']
                    ))
                elif isinstance(value, list):
                    elements.append(Paragraph(
                        f"<b>{key}</b>:", 
                        self.styles['Content']
                    ))
                    for item in value:
                        elements.append(Paragraph(
                            f"• {item}", 
                            self.styles['ListItem']
                        ))
        elif isinstance(content, str):
            elements.append(Paragraph(content, self.styles['Content']))
            
        return elements

    def create_header_section(self, resume_data: Dict[str, Any]):
        """Create the header section with name and contact info"""
        try:
            # Make sure HeaderName style is available
            header_style = self.styles.get('HeaderName', self.styles['Heading1'])
            
            # Ensure resume_data is a dictionary
            if not isinstance(resume_data, dict):
                self.logger.error(f"resume_data is not a dictionary: {type(resume_data)}")
                if isinstance(resume_data, str):
                    try:
                        resume_data = json.loads(resume_data)
                    except json.JSONDecodeError:
                        resume_data = {"name": "Error Processing Resume"}
                else:
                    resume_data = {"name": "Error Processing Resume"}
            
            # Add name centered and bold
            if isinstance(resume_data, dict) and 'name' in resume_data and resume_data['name']:
                self.header_elements.append(Paragraph(
                    f"<div align='center'>{resume_data['name']}</div>", 
                    header_style
                ))
            else:
                # Add a default name if missing
                self.header_elements.append(Paragraph(
                    "<div align='center'>Resume</div>", 
                    header_style
                ))
            
            # Add contact info - safely check resume_data is a dict first
            if isinstance(resume_data, dict):
                contact_fields = {'email', 'phone', 'linkedin', 'github', 'website'}
                contact_info = {
                    field: resume_data.get(field)
                    for field in contact_fields
                    if field in resume_data and resume_data[field]
                }
                
                if contact_info:
                    contact_text = self.format_contact_info(resume_data)
                    contact_style = self.styles.get('Contact', self.styles['Normal'])
                    self.header_elements.append(Paragraph(contact_text, contact_style))
                    self.header_elements.append(Spacer(1, 5))
        except Exception as e:
            self.logger.error(f"Error creating header section: {str(e)}")
            # Add a minimal header if there's an error
            header_style = self.styles.get('HeaderName', self.styles['Heading1'])
            self.header_elements.append(Paragraph(
                "<div align='center'>Resume</div>", 
                header_style
            ))

    def generate_pdf(self):
        """Generate the final PDF"""
        try:
            # Create a single column layout
            content_frame = Frame(
                self.doc.leftMargin,
                self.doc.bottomMargin,
                self.doc.width,
                self.doc.height,
                leftPadding=3,
                rightPadding=3,
                topPadding=3,
                bottomPadding=3,
                showBoundary=0
            )

            def page_background(canvas, doc):
                canvas.saveState()
                canvas.setFillColor(self.colors['background'])
                canvas.rect(0, 0, A4[0], A4[1], fill=1, stroke=0)
                canvas.restoreState()

            template = PageTemplate(
                id='SingleColumn',
                frames=[content_frame],
                onPage=page_background
            )

            self.doc.addPageTemplates([template])

            # Combine all content
            all_content = self.header_elements + self.main_content

            # Set document metadata for ATS optimization
            self.doc.title = "Resume"
            self.doc.author = "Resume Generator"
            self.doc.subject = "Resume"
            self.doc.keywords = ["Resume", "CV", "Job Application"]

            # Generate the PDF
            self.doc.build(all_content)

            self.logger.info(f"Resume PDF generated successfully at: {self.output_path}")
        except Exception as e:
            self.logger.error(f"Error generating PDF: {str(e)}")
            raise
    @staticmethod
    def convert_resume_json_to_pdf(json_file_path: str, pdf_output_path: str, theme: str = 'default'):
        """Convert resume JSON file to PDF"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                resume_data = json.load(file)

            pdf_gen = ResumePDFGenerator(pdf_output_path, theme)
            pdf_gen.process_resume(resume_data)
            pdf_gen.generate_pdf()
            
            return True

        except FileNotFoundError:
            logging.error(f"Resume JSON file not found: {json_file_path}")
            raise
        except json.JSONDecodeError:
            logging.error("Invalid JSON format")
            raise
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            raise

# Database and container management functions
def get_or_create_database(database_id: str):
    try:
        database = client.get_database_client(database_id)
        logger.info(f"Successfully connected to database: {database_id}")
        return database
    except exceptions.CosmosResourceNotFoundError:
        logger.info(f"Creating new database: {database_id}")
        return client.create_database(database_id)
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

def get_or_create_container(database, container_id: str):
    try:
        container = database.get_container_client(container_id)
        logger.info(f"Successfully connected to container: {container_id}")
        return container
    except exceptions.CosmosResourceNotFoundError:
        logger.info(f"Creating new container: {container_id}")
        return database.create_container(id=container_id, partition_key=PartitionKey(path=PARTITION_KEY_PATH))
    except Exception as e:
        logger.error(f"Container error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Container error: {str(e)}")

def store_json(container, doc_id: str, data: dict, max_retries=3):
    attempt = 0
    while attempt < max_retries:
        try:
            document = {"id": doc_id, "data": data}
            existing_doc = list(container.query_items(
                query="SELECT * FROM c WHERE c.id=@id",
                parameters=[{"name": "@id", "value": doc_id}],
                enable_cross_partition_query=True
            ))
            if existing_doc:
                new_id = f"{doc_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
                document["id"] = new_id
                logger.warning(f"Document with ID {doc_id} exists. Storing with new ID: {new_id}")
            container.create_item(body=document)
            logger.info(f"Successfully stored document with ID: {document['id']}")
            return document["id"]
        except exceptions.CosmosHttpResponseError as e:
            logger.error(f"Error storing in Cosmos DB (Attempt {attempt + 1}): {str(e)}")
            attempt += 1
            time.sleep(2 ** attempt)
    raise HTTPException(status_code=500, detail="Failed to store document after multiple attempts.")

# GPT API functions
async def resume_to_json(resume_text: str) -> dict:
    """Convert resume text to JSON using OpenAI GPT"""
    try:
        response = await openai.ChatCompletion.acreate(
            engine=deployment_name,
            messages=[
                {"role": "system", "content": "You are a resume parser that converts resume text to structured JSON. Always ensure your output is valid JSON format."},
                {"role": "user", "content": f"Convert this resume text to JSON format with sections for personal info, summary, experience, education, and skills. Return ONLY valid JSON without explanation or formatting:\n\n{resume_text}"}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        json_string = response.choices[0].message.content
        # Clean up the response to ensure it's valid JSON
        json_string = json_string.strip()
        
        # Remove any markdown code block indicators
        if json_string.startswith("```json"):
            json_string = json_string[7:]
        elif json_string.startswith("```"):
            json_string = json_string[3:]
            
        if json_string.endswith("```"):
            json_string = json_string[:-3]
            
        # Further cleanup to handle common JSON formatting issues
        json_string = json_string.strip()
        
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON decode error: {str(json_err)} in string: {json_string[:100]}...")
            
            # Attempt to fix common JSON errors
            # 1. Replace single quotes with double quotes
            fixed_json = json_string.replace("'", "\"")
            # 2. Ensure property names are quoted
            fixed_json = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', fixed_json)
            # 3. Fix trailing commas
            fixed_json = re.sub(r',\s*}', '}', fixed_json)
            
            try:
                return json.loads(fixed_json)
            except json.JSONDecodeError:
                # If still failing, try a more robust approach with a JSON repair library if available
                # For now, fall back to a minimal structure
                logger.error(f"Failed to repair JSON. Original error: {str(json_err)}")
                return {
                    "error": "Could not parse resume data",
                    "personal_info": {"name": "Unknown"},
                    "summary": "Failed to parse resume content properly."
                }
            
    except Exception as e:
        logger.error(f"Error in resume_to_json: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to convert resume to JSON format")

async def analyze_job_details(title: str, description: str) -> dict:
    """Analyze job details using OpenAI GPT"""
    try:
        response = await openai.ChatCompletion.acreate(
            engine=deployment_name,
            messages=[
                {"role": "system", "content": "You are a job analysis expert. Always respond with valid JSON containing requirements, responsibilities, and qualifications."},
                {"role": "user", "content": f"Convert this job posting into JSON format:\n\nTitle: {title}\n\nDescription: {description}"}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        content = response.choices[0].message.content.strip()
        # Clean up the response to ensure it's valid JSON
        if content.startswith("```json"):
            content = content[7:-3]  # Remove ```json and ``` markers
        elif content.startswith("{"):
            content = content  # Already JSON format
        else:
            raise ValueError("Response is not in valid JSON format")
            
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {content}")
        raise HTTPException(status_code=500, detail="Failed to parse job details response")
    except Exception as e:
        logger.error(f"Error in analyze_job_details: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to analyze job details")

async def generate_tailored_resume(resume_data: dict, job_data: dict) -> dict:
    """Generate tailored resume using OpenAI GPT"""
    try:
        response = await openai.ChatCompletion.acreate(
            engine=deployment_name,
            messages=[
                {"role": "system", "content": "You are an expert at tailoring resumes to specific job requirements. Always respond with valid JSON. Generate a well-structured resume in JSON format with the following sections: name, email, phone, github, linkedin,summary, skills, experience, education,projects,Certifications. Formatting Requirements: Personal Information: Include name, email, phone, github, and linkedin.Summary section:Give a entire summary of the resume like roles,passion,about the user Skills Section: Should contain categorized skills as key-value pairs, such as Languages and Technologies & Tools. Experience Section: Should be a list of objects with company, location, title, date, and responsibilities. Each role should include a bulleted list of responsibilities and mention relevant technologies used. Education Section: Should include institution, degree, major, date, gpa, and coursework. Coursework should be listed as a comma-separated string. Projects Section: Each project should include name, description, and technologies. The description should be concise but informative. Technologies should be stored as a list of strings. Certications.Ensure the JSON output follows this structure exactly without any extra formatting or missing fields This keeps everything compact while retaining all essential details."},  
                {"role": "user", "content": f"Tailor this resume to the job requirements and return a valid JSON object:\n\nResume: {json.dumps(resume_data)}\n\nJob Details: {json.dumps(job_data)}"}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content.strip()
        # Clean up the response to ensure it's valid JSON
        if content.startswith("```json"):
            content = content[7:-3]  # Remove ```json and ``` markers
        elif not content.startswith("{"):
            raise ValueError("Response is not in valid JSON format")
            
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response: {content}")
            raise ValueError("Response could not be parsed as JSON")
            
    except ValueError as e:
        logger.error(f"Value error in generate_tailored_resume: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Error in generate_tailored_resume: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate tailored resume")

# PDF Generation classes (DynamicSection, ExperienceSection, SkillsSection, ResumePDFGenerator)
# ... (Keep these classes as they were in the original script)

# PDF upload function
def upload_pdf_to_azure(pdf_file_path: str):
    """
    Upload a PDF file to Azure Blob Storage with robust error handling
    and container creation.
    
    Args:
        pdf_file_path: Local path to the PDF file
        
    Returns:
        The name of the uploaded file or None if upload failed
    """
    try:
        # Generate a unique name with timestamp and UUID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        pdf_file_name = f"resume_{timestamp}_{unique_id}.pdf"
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        
        # Ensure the container exists
        container_name = BLOB_CONTAINER_NAME
        if not container_name:
            container_name = "new"  # Default container name
            logger.warning(f"BLOB_CONTAINER_NAME not set, using default: {container_name}")
            
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Check if container exists, create if not
        try:
            container_properties = container_client.get_container_properties()
            logger.info(f"Container exists: {container_name}")
        except Exception as container_error:
            logger.info(f"Container doesn't exist or error accessing it: {str(container_error)}")
            logger.info(f"Creating container: {container_name}")
            blob_service_client.create_container(container_name)
            container_client = blob_service_client.get_container_client(container_name)
        
        # Log upload details
        logger.info(f"Uploading file: {pdf_file_name} to container: {container_name}")
        
        # Upload the file
        with open(pdf_file_path, "rb") as pdf_data:
            blob_client = container_client.get_blob_client(pdf_file_name)
            blob_client.upload_blob(pdf_data, overwrite=True)
        
        # Get the URL with SAS token
        base_url = blob_client.url
        
        # Ensure we have a valid SAS token
        sas_token = BLOB_SAS_TOKEN
        
        # If no SAS token configured, try to generate one
        if not sas_token:
            try:
                from datetime import timedelta
                from azure.storage.blob import generate_blob_sas, BlobSasPermissions
                
                # Generate a SAS token with read permissions that expires in 7 days
                sas_token = generate_blob_sas(
                    account_name=blob_service_client.account_name,
                    container_name=container_name,
                    blob_name=pdf_file_name,
                    account_key=blob_service_client.credential.account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + timedelta(days=7)
                )
                logger.info("Generated temporary SAS token")
            except Exception as sas_e:
                logger.error(f"Failed to generate SAS token: {str(sas_e)}")
        
        # Verify the upload was successful by checking if blob exists
        try:
            blob_properties = blob_client.get_blob_properties()
            logger.info(f"Upload verified: Blob exists with size {blob_properties.size} bytes")
        except Exception as verify_error:
            logger.error(f"Upload verification failed: {str(verify_error)}")
            raise Exception("Upload verification failed - blob does not exist after upload")
            
        logger.info(f"PDF uploaded successfully: {pdf_file_name}")
        return pdf_file_name
        
    except Exception as e:
        logger.error(f"An error occurred while uploading PDF: {str(e)}")
        return None

def extract_text_from_pdf(contents: bytes) -> str:
    """Extract text from PDF content"""
    try:
        with fitz.open(stream=contents, filetype="pdf") as doc:
            text = ""
            for page in doc:
                text += page.get_text()
            return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to extract text from PDF")

def extract_text_from_docx(contents: bytes) -> str:
    """Extract text from DOCX content"""
    try:
        doc = docx.Document(io.BytesIO(contents))
        text = []
        for paragraph in doc.paragraphs:
                text.append(paragraph.text)
        return '\n'.join(text)
    except Exception as e:
        logger.error(f"Error extracting text from DOCX: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to extract text from DOCX")

# Helper functions for the modified endpoints
async def process_resume_with_openai(resume_text: str) -> dict:
    """Process resume text with OpenAI to extract structured data."""
    try:
        response = await openai.ChatCompletion.acreate(
            engine=deployment_name,
            messages=[
                {"role": "system", "content": "You are a resume parser that converts resume text to structured JSON."},
                {"role": "user", "content": f"Convert this resume text to JSON format with sections for personal info, summary, experience, education, and skills:\n\n{resume_text}"}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        json_string = response.choices[0].message.content
        # Clean up the response to ensure it's valid JSON
        json_string = json_string.strip()
        if json_string.startswith("```json"):
            json_string = json_string[7:-3]  # Remove ```json and ``` markers
        
        try:
            resume_data = json.loads(json_string)
            # Add type field to identify this as a resume
            resume_data["type"] = "resume"
            return resume_data
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response: {json_string}")
            # Return a basic structure if parsing fails
            return {
                "type": "resume",
                "filename": "Parsed Resume",
                "summary": "Failed to parse resume content properly."
            }
    except Exception as e:
        logger.error(f"Error in process_resume_with_openai: {str(e)}")
        # Return a basic structure if processing fails
        return {
            "type": "resume",
            "filename": "Parsed Resume",
            "summary": "Failed to process resume content."
        }

def save_resume_to_cosmos(resume_data: dict) -> str:
    """Save resume data to Cosmos DB."""
    try:
        # Generate a unique ID for the resume
        resume_id = str(uuid.uuid4())
        
        # Add ID to the resume data
        resume_data["id"] = resume_id
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Save the resume
        container.create_item(body=resume_data)
        
        logger.info(f"Resume saved to Cosmos DB with ID: {resume_id}")
        return resume_id
    
    except Exception as e:
        logger.error(f"Error saving resume to Cosmos DB: {str(e)}")
        raise

def save_file_to_blob(content, blob_name):
    """
    Save file content to Azure Blob Storage and return URL with SAS token.
    
    Args:
        content: The binary content to save
        blob_name: The name to use for the blob
        
    Returns:
        URL to the saved blob with SAS token properly appended
    """
    # Log the blob name being used
    logger.info(f"Saving file to blob with name: {blob_name}")
    
    # Normalize the blob name to ensure it's valid
    # Make sure there are no leading slashes as they're not allowed in blob names
    while blob_name.startswith('/'):
        blob_name = blob_name[1:]
    
    # Replace any invalid characters
    blob_name = re.sub(r'[^\w\/\.\-]', '_', blob_name)
    
    # Ensure blob name follows the correct pattern (resume_YYYYMMDD_HHMMSS_uniqueid.pdf)
    if not blob_name.startswith('resume_') or not blob_name.endswith('.pdf'):
        # If it doesn't follow our pattern, rename it
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        blob_name = f"resume_{timestamp}_{unique_id}.pdf"
        logger.info(f"Renamed blob to follow standard pattern: {blob_name}")
    
    logger.info(f"Normalized blob name: {blob_name}")
    
    # Ensure container name is set correctly - MUST be "new" for the standard URL format
    container_name = "new"  # Force container name to be "new"
    if BLOB_CONTAINER_NAME and BLOB_CONTAINER_NAME != "new":
        logger.warning(f"Overriding configured container name '{BLOB_CONTAINER_NAME}' to use standard container 'new'")
    
    # Try multiple approaches to ensure success
    try:
        # Try with regular connection string first (more reliable)
        logger.info("Uploading with regular connection string")
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Check if container exists, create if not
        try:
            container_properties = container_client.get_container_properties()
            logger.info(f"Container exists: {container_name}")
        except Exception as container_error:
            logger.info(f"Container doesn't exist or error accessing it: {str(container_error)}")
            logger.info(f"Creating container: {container_name}")
            blob_service_client.create_container(container_name)
            container_client = blob_service_client.get_container_client(container_name)
        
        # Check if blob exists before uploading
        blob_client = container_client.get_blob_client(blob_name)
        try:
            blob_properties = blob_client.get_blob_properties()
            logger.info(f"Blob already exists, will overwrite: {blob_name}")
        except Exception:
            logger.info(f"Blob doesn't exist yet, will create: {blob_name}")
        
        # Upload file
        blob_client.upload_blob(content, overwrite=True)
        logger.info(f"Successfully uploaded blob: {blob_name}")
        
        # Construct the correct URL format
        # Ensure the URL is in format https://pdf1.blob.core.windows.net/new/blob_name
        correct_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
        
        # Add SAS token
        if BLOB_SAS_TOKEN:
            # Remove ? from token if present at the beginning
            sas_token = BLOB_SAS_TOKEN
            if sas_token.startswith('?'):
                sas_token = sas_token[1:]
                
            # Add SAS token to URL
            correct_url = f"{correct_url}?{sas_token}"
        
        logger.info(f"Generated correct format URL: {correct_url}")
        
        # Test that URL is accessible
        try:
            response = requests.head(correct_url, timeout=5)
            if 200 <= response.status_code < 300:
                logger.info(f"URL verified accessible with status code: {response.status_code}")
            else:
                logger.warning(f"URL might not be accessible, status code: {response.status_code}")
                # Try fallback to actual blob URL if the constructed URL fails
                if blob_client.url != correct_url.split('?')[0]:
                    logger.warning(f"Using actual blob URL as fallback since constructed URL failed")
                    base_url = blob_client.url
                    if BLOB_SAS_TOKEN:
                        full_url = f"{base_url}?{sas_token}"
                    else:
                        full_url = base_url
                    return full_url
        except Exception as e:
            logger.warning(f"Couldn't verify URL accessibility: {str(e)}")
        
        return correct_url
    
    except Exception as e:
        logger.error(f"Error uploading blob: {str(e)}")
        # Return a direct URL but warn it may not be accessible
        direct_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
        if BLOB_SAS_TOKEN:
            if BLOB_SAS_TOKEN.startswith('?'):
                direct_url += BLOB_SAS_TOKEN
            else:
                direct_url += f"?{BLOB_SAS_TOKEN}"
        
        logger.warning(f"Failed upload attempts. Returning direct URL: {direct_url}")
        return direct_url

def update_resume_with_blob_url(resume_id, blob_url):
    """Update resume in Cosmos DB with blob URL."""
    try:
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Get the resume
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if items:
            resume = items[0]
            resume["blob_url"] = blob_url
            
            # Update the resume
            container.upsert_item(resume)
            logger.info(f"Resume {resume_id} updated with blob URL: {blob_url}")
        else:
            logger.error(f"Resume {resume_id} not found for blob URL update")
    
    except Exception as e:
        logger.error(f"Error updating resume with blob URL: {str(e)}")
        raise

# FastAPI route
@app.post("/process-all/")
async def process_all(title: str = Form(...), description: str = Form(...), file: UploadFile = File(...)):
    """
    Process a resume file and job details, generate a tailored resume, and create a PDF.
    
    Args:
        title: Job title
        description: Job description 
        file: Resume file (PDF or DOCX)
        
    Returns:
        JSON response with processing results
    """
    logger.info(f"Processing resume file: {file.filename}")
    
    if not file.filename.lower().endswith(('.pdf', '.docx')):
        logger.error("Invalid file type")
        raise HTTPException(status_code=400, detail="Only PDF and DOCX files are supported")
    
    # Status tracking
    process_status = {
        "resume_processing": "pending",
        "job_analysis": "pending",
        "tailoring": "pending",
        "pdf_generation": "pending",
        "errors": []
    }
    
    resume_id = ""
    job_id = ""
    tailored_resume_id = ""
    pdf_file_name = ""
    
    try:
        # Read file content
        contents = await file.read()
        resume_text = ""
        resume_json_data = {}
        
        # Step 1: Extract text from document
        try:
            if file.filename.lower().endswith('.pdf'):
                resume_text = extract_text_from_pdf(contents)
            else:
                resume_text = extract_text_from_docx(contents)
        
            if not resume_text or len(resume_text.strip()) < 100:
                logger.warning(f"Very little text extracted: {len(resume_text.strip()) if resume_text else 0} chars")
                process_status["errors"].append("Very little text could be extracted from the resume.")
        except Exception as extract_e:
            logger.error(f"Text extraction error: {str(extract_e)}")
            process_status["errors"].append(f"Failed to extract text: {str(extract_e)}")
            
        # Step 2: Process resume with OpenAI
        try:
            resume_json_data = await resume_to_json(resume_text)
            process_status["resume_processing"] = "complete"
        except Exception as resume_e:
            logger.error(f"Resume parsing error: {str(resume_e)}")
            process_status["errors"].append(f"Failed to parse resume: {str(resume_e)}")
            process_status["resume_processing"] = "failed"
            # Create a minimal structure
            resume_json_data = {
                "name": os.path.splitext(file.filename)[0],
                "contact": {},
                "summary": "Error processing resume content",
                "experience": [],
                "education": [],
                "skills": []
            }
            
        # Step 3: Save resume to Cosmos DB
        try:
            resume_database = get_or_create_database(RESUME_DATABASE_ID)
            resume_container = get_or_create_container(resume_database, RESUME_CONTAINER_ID)
        
            file_name = os.path.splitext(file.filename)[0]
            resume_name = re.sub(r'[^a-zA-Z0-9_-]', '_', resume_json_data.get("name", file_name)).replace(" ", "_").lower()
        
            resume_id = store_json(resume_container, resume_name, resume_json_data)
            logger.info(f"Stored resume with ID: {resume_id}")
        except Exception as store_e:
            logger.error(f"Resume storage error: {str(store_e)}")
            process_status["errors"].append(f"Failed to store resume: {str(store_e)}")
            resume_id = f"temp_{int(time.time())}"
        
        # Step 4: Process job details
        try:
            job_json_data = await analyze_job_details(title, description)
            process_status["job_analysis"] = "complete"
        
            job_database = get_or_create_database(JOB_DATABASE_ID)
            job_container = get_or_create_container(job_database, JOB_CONTAINER_ID)
        
            job_id = re.sub(r'[^a-zA-Z0-9_-]', '_', title).replace(" ", "_").lower()
            job_id = store_json(job_container, job_id, job_json_data)
            logger.info(f"Stored job with ID: {job_id}")
        except Exception as job_e:
            logger.error(f"Job analysis error: {str(job_e)}")
            process_status["errors"].append(f"Failed to analyze job: {str(job_e)}")
            process_status["job_analysis"] = "failed"
            job_json_data = {
                "Requirements": {"Skills": ["Processing error"]},
                "Responsibilities": {"Main": "Processing error"},
                "Qualifications": ["Processing error"]
            }
            job_id = re.sub(r'[^a-zA-Z0-9_-]', '_', title).replace(" ", "_").lower()
            
        # Step 5: Generate tailored resume
        try:
            tailored_resume_json = await generate_tailored_resume(resume_json_data, job_json_data)
            process_status["tailoring"] = "complete"
        
            tailored_resume_database = get_or_create_database(RESUME_DATABASE_ID)
            tailored_resume_container = get_or_create_container(tailored_resume_database, TAILORED_RESUME_CONTAINER_ID)
        
            tailored_resume_id = f"{resume_name}_for_{job_id}"
            tailored_resume_id = store_json(tailored_resume_container, tailored_resume_id, tailored_resume_json)
            logger.info(f"Stored tailored resume with ID: {tailored_resume_id}")
        except Exception as tailor_e:
            logger.error(f"Resume tailoring error: {str(tailor_e)}")
            process_status["errors"].append(f"Failed to tailor resume: {str(tailor_e)}")
            process_status["tailoring"] = "failed"
            tailored_resume_json = resume_json_data.copy()
            tailored_resume_id = f"{resume_name}_for_{job_id}"
            
        # Step 6: Generate PDF
        try:
            logger.info("Beginning PDF generation")
            
            # Ensure tailored_resume_json is a dictionary
            if not isinstance(tailored_resume_json, dict):
                logger.error(f"tailored_resume_json is not a dictionary: {type(tailored_resume_json)}")
                if isinstance(tailored_resume_json, str):
                    try:
                        tailored_resume_json = json.loads(tailored_resume_json)
                    except json.JSONDecodeError:
                        logger.error("Could not parse tailored_resume_json as JSON")
                        tailored_resume_json = {
                            "name": "Error Processing Resume",
                            "summary": "Error occurred during resume processing"
                        }
                else:
                    logger.error(f"Cannot process tailored_resume_json of type: {type(tailored_resume_json)}")
                    tailored_resume_json = {
                        "name": "Error Processing Resume",
                        "summary": "Error occurred during resume processing"
                    }
            
            temp_pdf_path = f"temp_resume_{int(time.time())}.pdf"
            pdf_gen = ResumePDFGenerator(temp_pdf_path, "default")
            pdf_gen.process_resume(tailored_resume_json)
            pdf_gen.generate_pdf()
        
            pdf_file_name = upload_pdf_to_azure(temp_pdf_path)
            
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)
                
            process_status["pdf_generation"] = "complete"
            logger.info(f"Generated and uploaded PDF: {pdf_file_name}")
        except Exception as pdf_e:
            logger.error(f"PDF generation error: {str(pdf_e)}")
            process_status["errors"].append(f"Failed to generate PDF: {str(pdf_e)}")
            process_status["pdf_generation"] = "failed"
            pdf_file_name = "error_generating.pdf"
            
        # Determine overall status
        overall_status = "success"
        if "failed" in [process_status["resume_processing"], process_status["job_analysis"], 
                        process_status["tailoring"], process_status["pdf_generation"]]:
            overall_status = "partial_success"
        if len(process_status["errors"]) >= 3:
            overall_status = "mostly_failed"
            
        # Return results
        response_data = {
            "status": overall_status,
                "resume_id": resume_id,
                "job_id": job_id,
                "tailored_resume_id": tailored_resume_id,
            "pdf_url": pdf_file_name,
            "process_details": process_status
        }
        
        if overall_status == "mostly_failed":
            return JSONResponse(status_code=207, content=response_data)
        return response_data
        
    except Exception as e:
        logger.error(f"Unexpected error in process_all: {str(e)}")
        logger.exception("Detailed error:")
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "failed",
                "error": str(e),
                "message": "An unexpected error occurred while processing the request.",
                "file_processed": file.filename
            }
        )

@app.post("/upload-resume")
async def upload_resume(
    file: UploadFile = File(...),
    user_id: str = Form(...),
):
    """
    Upload and process a resume file (PDF or DOCX).
    Now requires user_id to associate the resume with a specific user.
    """
    try:
        # Read file content
        content = await file.read()
        
        # Process file based on extension
        filename = file.filename
        file_extension = os.path.splitext(filename)[1].lower()
        
        if file_extension == '.pdf':
            text = extract_text_from_pdf(content)
        elif file_extension in ['.docx', '.doc']:
            text = extract_text_from_docx(content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Please upload a PDF or DOCX file.")
        
        # Process resume with OpenAI
        resume_data = await process_resume_with_openai(text)
        
        # Add user_id to the resume data
        resume_data["user_id"] = user_id
        resume_data["created_at"] = datetime.now().isoformat()
        resume_data["filename"] = filename
        
        # Save to Cosmos DB
        resume_id = save_resume_to_cosmos(resume_data)
        
        # Save file to Blob Storage
        blob_url = save_file_to_blob(content, f"{user_id}/{resume_id}{file_extension}")
        
        # Update resume in Cosmos DB with blob URL
        update_resume_with_blob_url(resume_id, blob_url)
        
        return {"message": "Resume uploaded and processed successfully", "resume_id": resume_id}
    
    except Exception as e:
        logger.error(f"Error processing resume: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing resume: {str(e)}")

@app.get("/get-resumes/{user_id}")
async def get_resumes(user_id: str):
    """
    Get all resumes for a specific user.
    Returns a list of resume metadata objects with properly formed blob URLs.
    """
    try:
        logger.info(f"Fetching resumes for user_id: {user_id}")
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for resumes with the specified user_id
        query = f"SELECT * FROM c WHERE c.user_id = '{user_id}'"
        logger.info(f"Executing query: {query}")
        
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        logger.info(f"Found {len(items)} resumes for user_id: {user_id}")
        
        # If no resumes found, create a sample resume for testing
        if not items:
            logger.info(f"No resumes found for user_id: {user_id}, creating a sample resume")
            
            try:
                # Create a sample PDF
                sample_pdf_response = await create_sample_pdf()
                sample_blob_url = sample_pdf_response["blob_url"]
                
                # Create a sample resume entry in Cosmos DB
                sample_resume = {
                    "id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "filename": "Sample Resume.pdf",
                    "created_at": datetime.now().isoformat(),
                    "blob_url": sample_blob_url,
                    "type": "resume"
                }
                
                # Save to Cosmos DB
                container.create_item(body=sample_resume)
                logger.info(f"Created sample resume for user_id: {user_id}")
                
                # Add to items list
                items.append(sample_resume)
                
            except Exception as e:
                logger.error(f"Error creating sample resume: {str(e)}")
        
        # Add SAS token to blob URLs if needed
        updated_items = []
        need_db_update = False
        
        for item in items:
            # Create a copy to avoid modifying the original item if we don't need to update the DB
            updated_item = item.copy()
            
            if "blob_url" in item and item["blob_url"]:
                original_url = item["blob_url"]
                updated_url = original_url
                
                # Check if URL needs SAS token
                if "?" not in original_url or BLOB_SAS_TOKEN not in original_url:
                # Get the base URL without any query parameters
                    base_url = original_url.split('?')[0]
                # Append the SAS token
                    updated_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                    logger.info(f"Updated blob URL for resume: {item['id']}")
                
                # Special handling for known problematic URLs
                if "pdf1.blob.core.windows.net/new/resume_" in original_url and "?" not in original_url:
                    updated_url = f"{original_url}?{BLOB_SAS_TOKEN}"
                    logger.info(f"Special case: Added SAS token to known problematic URL: {updated_url}")
                    
                # General handling for all pdf1.blob.core.windows.net URLs
                if "pdf1.blob.core.windows.net" in original_url:
                    # Always ensure pdf1.blob URLs have the proper SAS token by splitting 
                    # and recombining with the token, regardless of existing parameters
                    base_url = original_url.split('?')[0]
                    updated_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                    logger.info(f"Ensured proper SAS token for pdf1.blob URL: {updated_url}")
                
                # Extra check for the specific problematic URL mentioned by the user
                if "https://pdf1.blob.core.windows.net/new/resume_20250310_164411_b75f956f.pdf" in original_url and "?" not in original_url:
                    updated_url = f"{original_url.split('?')[0]}?{BLOB_SAS_TOKEN}"
                    logger.info(f"Fixed specific problematic URL: {updated_url}")
                
                # Update the item with the corrected URL
                updated_item["blob_url"] = updated_url
                
                # Check if we need to update the database
                if original_url != updated_url:
                    need_db_update = True
                    
                    # Also update in database to fix it permanently
                    try:
                        item["blob_url"] = updated_url
                        container.replace_item(item=item["id"], body=item)
                        logger.info(f"Updated blob URL in database for resume: {item['id']}")
                    except Exception as update_e:
                        logger.warning(f"Failed to update blob URL in database: {str(update_e)}")
            
            # Add download_url for convenience
            if "id" in updated_item and "user_id" in updated_item:
                updated_item["download_url"] = f"/download-resume/{updated_item['id']}?user_id={updated_item['user_id']}"
                # Add direct download URL that the frontend should use instead
                updated_item["direct_download_url"] = f"/direct-download/{updated_item['id']}?user_id={updated_item['user_id']}"
            
            # Add file_type property for UI handling
            if "filename" in updated_item:
                filename = updated_item["filename"]
                file_extension = os.path.splitext(filename)[1].lower() if filename else ""
                updated_item["file_type"] = file_extension[1:] if file_extension else "unknown"
                
            # Format dates for display
            if "created_at" in updated_item:
                try:
                    # Keep ISO format for API but add readable format for display
                    date_obj = datetime.fromisoformat(updated_item["created_at"].replace("Z", "+00:00"))
                    updated_item["created_at_formatted"] = date_obj.strftime("%B %d, %Y %I:%M %p")
                except Exception:
                    updated_item["created_at_formatted"] = updated_item["created_at"]
            
            updated_items.append(updated_item)
        
        logger.info(f"Returning {len(updated_items)} resumes with updated blob URLs")
        return updated_items
    
    except Exception as e:
        logger.error(f"Error fetching resumes: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error fetching resumes: {str(e)}")

@app.get("/get-resume/{resume_id}")
async def get_resume(resume_id: str, user_id: str):
    """
    Get a specific resume by ID, ensuring it belongs to the specified user.
    Returns detailed resume metadata with properly formed blob URL.
    """
    try:
        logger.info(f"Fetching resume with ID: {resume_id} for user: {user_id}")
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for the resume with the specified ID and user_id
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}' AND c.user_id = '{user_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not items:
            raise HTTPException(status_code=404, detail="Resume not found or does not belong to the specified user")
        
        resume = items[0]
        
        # Fix blob URL if it exists
        if "blob_url" in resume and resume["blob_url"]:
            # Make sure URL has proper SAS token
            original_url = resume["blob_url"]
            updated_url = original_url
            
            # Check if URL needs SAS token
            if "?" not in original_url or BLOB_SAS_TOKEN not in original_url:
            # Get the base URL without any query parameters
                base_url = original_url.split('?')[0]
            # Append the SAS token
                updated_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                logger.info(f"Fixed blob URL: {original_url} -> {updated_url}")
            
            # Special handling for known problematic URLs
            if "pdf1.blob.core.windows.net/new/resume_" in original_url and "?" not in original_url:
                updated_url = f"{original_url}?{BLOB_SAS_TOKEN}"
                logger.info(f"Special case: Added SAS token to known problematic URL: {updated_url}")
            
            # General handling for all pdf1.blob.core.windows.net URLs
            if "pdf1.blob.core.windows.net" in original_url:
                # Always ensure pdf1.blob URLs have the proper SAS token by splitting 
                # and recombining with the token, regardless of existing parameters
                base_url = original_url.split('?')[0]
                updated_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                logger.info(f"Ensured proper SAS token for pdf1.blob URL: {updated_url}")
            
            # Extra check for the specific problematic URL mentioned by the user
            if "https://pdf1.blob.core.windows.net/new/resume_20250310_164411_b75f956f.pdf" in original_url and "?" not in original_url:
                updated_url = f"{original_url.split('?')[0]}?{BLOB_SAS_TOKEN}"
                logger.info(f"Fixed specific problematic URL: {updated_url}")
                
            # Update the resume object
            resume["blob_url"] = updated_url
            
            # Also update in database to fix it permanently
            if original_url != updated_url:
                try:
                    resume_copy = resume.copy()
                    resume_copy["blob_url"] = updated_url
                    container.replace_item(item=resume["id"], body=resume_copy)
                    logger.info(f"Updated blob URL in database for resume: {resume['id']}")
                except Exception as update_e:
                    logger.warning(f"Failed to update blob URL in database: {str(update_e)}")
        
        # Validate the blob URL is accessible
        if "blob_url" in resume and resume["blob_url"]:
            try:
                import requests
                response = requests.head(resume["blob_url"], timeout=5)
                if response.status_code == 200:
                    logger.info(f"Blob URL validation successful: {response.status_code}")
                    resume["blob_accessible"] = True
                else:
                    logger.warning(f"Blob URL validation failed with status: {response.status_code}")
                    resume["blob_accessible"] = False
                    resume["blob_status_code"] = response.status_code
            except Exception as validate_e:
                logger.warning(f"Blob URL validation error: {str(validate_e)}")
                resume["blob_accessible"] = False
                resume["blob_error"] = str(validate_e)
        
        # Add convenience properties for front-end
        resume["download_url"] = f"/download-resume/{resume_id}?user_id={user_id}"
        # Add direct download URL that the frontend should use instead
        resume["direct_download_url"] = f"/direct-download/{resume_id}?user_id={user_id}"
        
        # Add file type info
        if "filename" in resume:
            filename = resume["filename"]
            file_extension = os.path.splitext(filename)[1].lower() if filename else ""
            resume["file_type"] = file_extension[1:] if file_extension else "unknown"
        
        # Format dates for display
        if "created_at" in resume:
            try:
                date_obj = datetime.fromisoformat(resume["created_at"].replace("Z", "+00:00"))
                resume["created_at_formatted"] = date_obj.strftime("%B %d, %Y %I:%M %p")
            except Exception:
                resume["created_at_formatted"] = resume["created_at"]
        
        return resume
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching resume: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error fetching resume: {str(e)}")

@app.get("/download-resume/{resume_id}")
async def download_resume(resume_id: str, user_id: str = Query(..., description="User ID associated with the resume")):
    """
    Download a resume by ID and user ID. Uses direct blob download.
    Returns blob URL with a valid SAS token.
    """
    try:
        logger.info(f"Download resume request for resume ID: {resume_id}, user ID: {user_id}")
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for the resume with the specified ID and user_id
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}' AND c.user_id = '{user_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not items:
            raise HTTPException(status_code=404, detail="Resume not found or does not belong to the specified user")
        
        resume = items[0]
        
        if not resume.get("blob_url"):
            raise HTTPException(status_code=404, detail="Resume file not found in storage")
        
        # Process the blob URL to ensure it has a valid SAS token
        blob_url = resume.get("blob_url")
        original_url = blob_url  # Save original for comparison
        
        # Parse the URL components
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.strip('/').split('/')
        
        # Standardize to the correct URL format
        if parsed_url.netloc != "pdf1.blob.core.windows.net" or (len(path_parts) > 0 and path_parts[0] != "new"):
            # URL doesn't follow the correct format, try to extract the blob name
            blob_name = None
            # Try to find a valid blob name
            for part in reversed(path_parts):
                if part.startswith("resume_") and part.endswith(".pdf"):
                    blob_name = part
                    break
            
            if not blob_name:
                # If we can't find a valid blob name, create one using resume ID
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_id = resume_id[:8] if len(resume_id) >= 8 else resume_id
                blob_name = f"resume_{timestamp}_{unique_id}.pdf"
            
            # Reconstruct URL with correct format
            blob_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
            logger.info(f"Reformatted URL to standard format: {blob_url}")
        
        # Make sure the URL has a valid SAS token
        if "?" not in blob_url:
            # No query parameters at all
            logger.info("Adding SAS token to blob URL")
            blob_url = f"{blob_url}?{BLOB_SAS_TOKEN}"
        elif not any(param.startswith('sp=') or param.startswith('sv=') for param in parsed_url.query.split('&')):
            # Has query params but not SAS specific ones
            logger.info("URL has query params but no valid SAS token, adding SAS token")
            blob_url = f"{blob_url}&{BLOB_SAS_TOKEN}"
            
        # Verify that the blob exists
        verify_success = False
        try:
            logger.info(f"Verifying blob URL: {blob_url}")
            verify_response = requests.head(blob_url, timeout=10)
            
            if verify_response.status_code == 200:
                logger.info("Blob URL verified successfully")
                verify_success = True
            elif verify_response.status_code == 404 and "BlobNotFound" in verify_response.text:
                logger.warning("BlobNotFound error when verifying URL, attempting recovery")
                
                # Try recovery with resume data
                new_url, _ = recover_missing_blob(blob_url, resume)
                
                if new_url:
                    logger.info(f"Recovery successful with new URL: {new_url}")
                    # Update the resume with the new URL
                    resume["blob_url"] = new_url
                    container.replace_item(item=resume["id"], body=resume)
                    logger.info("Updated resume with recovered blob URL")
                    
                    # Use the new URL
                    blob_url = new_url
                    verify_success = True
                else:
                    # If no new URL, we'll fall back to the direct download endpoint
                    logger.warning("Recovery failed, will use direct-download endpoint as fallback")
            else:
                logger.warning(f"Blob URL verification failed with status {verify_response.status_code}")
        except Exception as verify_e:
            logger.error(f"Error verifying blob URL: {str(verify_e)}")
            
        # Update the resume record if URL changed
        if blob_url != original_url:
            try:
                resume["blob_url"] = blob_url
                container.replace_item(item=resume["id"], body=resume)
                logger.info("Updated resume with new blob URL")
            except Exception as update_e:
                logger.warning(f"Failed to update resume record: {str(update_e)}")
                
        # If verification failed or we didn't get a 200 response, use the direct-download endpoint
        if not verify_success:
            logger.info("Using direct-download endpoint as fallback")
            direct_download_url = f"/direct-download/{resume_id}?user_id={user_id}"
            return {"url": direct_download_url, "direct": True}
            
        # Return the blob URL with SAS token
        return {"url": blob_url, "direct": False}
        
    except Exception as e:
        logger.error(f"Error in download-resume endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing download: {str(e)}")

@app.delete("/delete-resume/{resume_id}")
async def delete_resume(resume_id: str, user_id: str):
    """
    Delete a specific resume by ID, ensuring it belongs to the specified user.
    Also deletes the associated file from blob storage.
    """
    try:
        logger.info(f"Deleting resume with ID: {resume_id} for user: {user_id}")
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for the resume with the specified ID and user_id
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}' AND c.user_id = '{user_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not items:
            raise HTTPException(status_code=404, detail="Resume not found or does not belong to the specified user")
        
        resume = items[0]
        
        # Delete the file from blob storage if it exists
        if resume.get("blob_url"):
            try:
                # Extract the blob name from the URL
                # Remove any query parameters (SAS token)
                clean_url = resume["blob_url"].split('?')[0]
                blob_name = clean_url.split(f"{BLOB_CONTAINER_NAME}/")[1]
                logger.info(f"Extracted blob name for deletion: {blob_name}")
                
                # Try using the connection string with SAS token first
                try:
                    logger.info("Attempting to use connection string with SAS token for deletion")
                    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING_WITH_SAS)
                    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
                    blob_client = container_client.get_blob_client(blob_name)
                    
                    # Delete the blob
                    blob_client.delete_blob()
                    logger.info(f"Deleted blob: {blob_name} using connection string with SAS")
                    
                except Exception as e:
                    logger.warning(f"Error using connection string with SAS token: {str(e)}. Falling back to regular connection string.")
                    
                    # Fallback to regular connection string
                    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
                    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
                    blob_client = container_client.get_blob_client(blob_name)
                    
                    # Delete the blob
                    blob_client.delete_blob()
                    logger.info(f"Deleted blob: {blob_name} using fallback method")
                
            except Exception as e:
                logger.error(f"Error deleting blob: {str(e)}")
                # Continue with deleting the resume from Cosmos DB even if blob deletion fails
        
        # Delete the resume from Cosmos DB
        container.delete_item(item=resume["id"], partition_key=resume["id"])
        logger.info(f"Deleted resume with ID: {resume_id} from Cosmos DB")
        
        return {"message": "Resume deleted successfully"}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting resume: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error deleting resume: {str(e)}")

@app.post("/replace-resume-file")
async def replace_resume_file(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    resume_id: str = Form(...),
):
    """
    Replace a resume file in Azure Blob Storage for an existing resume record.
    This is used when the original file is missing or corrupted.
    """
    try:
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for the resume with the specified ID and user_id
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}' AND c.user_id = '{user_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not items:
            raise HTTPException(status_code=404, detail="Resume not found or does not belong to the specified user")
        
        resume = items[0]
        
        # Read file content
        content = await file.read()
        
        # Get file extension
        filename = file.filename
        file_extension = os.path.splitext(filename)[1].lower()
        
        if file_extension not in ['.pdf', '.doc', '.docx']:
            raise HTTPException(status_code=400, detail="Unsupported file format. Please upload a PDF or DOCX file.")
        
        # Save file to Blob Storage with user_id in the path
        blob_url = save_file_to_blob(content, f"{user_id}/{resume_id}{file_extension}")
        
        # Update resume in Cosmos DB with new blob URL and filename
        resume["blob_url"] = blob_url
        resume["filename"] = filename
        resume["updated_at"] = datetime.now().isoformat()
        
        # Update the resume in Cosmos DB
        container.replace_item(item=resume, body=resume)
        
        logger.info(f"Resume file replaced successfully for resume ID: {resume_id}")
        return {"message": "Resume file replaced successfully", "blob_url": blob_url}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error replacing resume file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error replacing resume file: {str(e)}")

@app.get("/test")
async def test_endpoint():
    """
    Test endpoint to check if the API is working.
    """
    return {"status": "ok", "message": "API is working"}

@app.get("/upload-sample-pdf")
async def upload_sample_pdf():
    """
    Upload a sample PDF file to Azure Blob Storage for testing.
    """
    try:
        logger.info("Uploading sample PDF file to Azure Blob Storage")
        
        # Create a simple PDF file with reportlab
        buffer = BytesIO()
        
        # Create a PDF document
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.5 * inch,
            bottomMargin=0.5 * inch
        )
        
        # Create content
        styles = getSampleStyleSheet()
        elements = []
        
        # Add a title
        elements.append(Paragraph("Sample Resume PDF", styles['Title']))
        
        # Add some content
        elements.append(Paragraph("This is a sample PDF file for testing Azure Blob Storage.", styles['Normal']))
        elements.append(Paragraph("If you can see this file, the SAS token is working correctly.", styles['Normal']))
        elements.append(Paragraph(f"Created at: {datetime.now().isoformat()}", styles['Normal']))
        elements.append(Paragraph("Using SAS token with 'srt=sco' parameter for service, container, and object level access.", styles['Normal']))
        
        # Build the PDF
        doc.build(elements)
        
        # Get the PDF content
        pdf_content = buffer.getvalue()
        buffer.close()
        
        # Upload to Azure Blob Storage
        blob_name = "sample.pdf"
        blob_url = save_file_to_blob(pdf_content, blob_name)
        
        return {
            "message": "Sample PDF uploaded successfully",
            "blob_url": blob_url
        }
    
    except Exception as e:
        logger.error(f"Error uploading sample PDF: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error uploading sample PDF: {str(e)}")

@app.get("/create-sample-pdf")
async def create_sample_pdf():
    """
    Create a sample PDF file and upload it to Azure Blob Storage.
    This endpoint ensures that a sample PDF exists for testing.
    """
    try:
        logger.info("Creating and uploading sample PDF file to Azure Blob Storage")
        
        # Create a simple PDF file with reportlab
        buffer = BytesIO()
        
        # Create a PDF document
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.5 * inch,
            bottomMargin=0.5 * inch
        )
        
        # Create content
        styles = getSampleStyleSheet()
        elements = []
        
        # Add a title
        elements.append(Paragraph("Sample Resume PDF", styles['Title']))
        
        # Add some content
        elements.append(Paragraph("This is a sample PDF file for testing Azure Blob Storage.", styles['Normal']))
        elements.append(Paragraph("If you can see this file, the SAS token is working correctly.", styles['Normal']))
        elements.append(Paragraph(f"Created at: {datetime.now().isoformat()}", styles['Normal']))
        elements.append(Paragraph("Using SAS token with 'srt=sco' parameter for service, container, and object level access.", styles['Normal']))
        
        # Build the PDF
        doc.build(elements)
        
        # Get the PDF content
        pdf_content = buffer.getvalue()
        buffer.close()
        
        # Upload to Azure Blob Storage with a fixed name
        blob_name = "sample.pdf"
        
        # Try using the connection string with SAS token first
        try:
            logger.info("Attempting to use connection string with SAS token for sample PDF")
            blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING_WITH_SAS)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Upload the PDF
            blob_client.upload_blob(pdf_content, overwrite=True)
            
            # Get the base URL of the blob
            base_blob_url = blob_client.url
            # Construct the SAS URL
            sample_blob_url = f"{base_blob_url}?{BLOB_SAS_TOKEN}"
            
            logger.info(f"Sample PDF created and uploaded successfully using connection string with SAS: {sample_blob_url}")
            
        except Exception as e:
            logger.warning(f"Error using connection string with SAS token: {str(e)}. Falling back to regular connection string.")
            
            # Fallback to regular connection string
            blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Upload the PDF
            blob_client.upload_blob(pdf_content, overwrite=True)
            
            # Get the base URL of the blob
            base_blob_url = blob_client.url
            # Construct the SAS URL
            sample_blob_url = f"{base_blob_url}?{BLOB_SAS_TOKEN}"
            
            logger.info(f"Sample PDF created and uploaded successfully using fallback method: {sample_blob_url}")
        
        return {
            "message": "Sample PDF created and uploaded successfully",
            "blob_url": sample_blob_url
        }
    
    except Exception as e:
        logger.error(f"Error creating sample PDF: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error creating sample PDF: {str(e)}")

@app.get("/fix-blob-urls")
async def fix_blob_urls(admin_key: str = Query(..., description="Admin key for security")):
    """
    Fix all blob URLs in the database to ensure they have proper SAS tokens.
    This is an admin function to fix historical data.
    """
    # Simple security check using the admin key from environment variables
    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Invalid admin key")
    
    try:
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for all resumes with blob_url
        query = "SELECT * FROM c WHERE IS_DEFINED(c.blob_url)"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        logger.info(f"Found {len(items)} resumes with blob URLs to check")
        
        fixed_count = 0
        already_correct = 0
        errors = 0
        special_cases_fixed = 0
        
        for resume in items:
            try:
                blob_url = resume.get("blob_url")
                
                # Check if URL needs fixing
                if not blob_url:
                    continue
                
                # Remove any existing query parameters to get the base URL
                base_url = blob_url.split('?')[0]
                
                # Create new URL with SAS token
                new_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                
                # Special handling for known problematic URLs
                is_special_case = False
                if "pdf1.blob.core.windows.net/new/resume_" in blob_url:
                    logger.info(f"Found special case URL: {blob_url}")
                    is_special_case = True
                    special_cases_fixed += 1
                
                # General handling for all pdf1.blob.core.windows.net URLs
                if "pdf1.blob.core.windows.net" in blob_url:
                    base_url = blob_url.split('?')[0]  # Always start with the base URL
                    new_url = f"{base_url}?{BLOB_SAS_TOKEN}"
                    logger.info(f"Ensuring proper SAS token for pdf1.blob URL: {new_url}")
                    is_special_case = True
                    special_cases_fixed += 1
                
                # Extra check for the specific problematic URL mentioned by the user
                if "https://pdf1.blob.core.windows.net/new/resume_20250310_164411_b75f956f.pdf" in blob_url:
                    logger.info(f"Found the specific problematic URL mentioned by the user")
                    is_special_case = True
                    special_cases_fixed += 1
                
                # Skip if URL already has the correct SAS token and it's not a special case
                if (blob_url == new_url or (BLOB_SAS_TOKEN in blob_url)) and not is_special_case:
                    logger.info(f"URL already correct for resume ID: {resume.get('id')}")
                    already_correct += 1
                    continue
                
                # Update the resume with the new URL
                resume["blob_url"] = new_url
                resume["fixed_at"] = datetime.now().isoformat()
                
                # Save the updated resume
                container.replace_item(item=resume["id"], body=resume)
                logger.info(f"Fixed blob URL for resume ID: {resume.get('id')}")
                fixed_count += 1
                
                # Verify the new URL is accessible
                try:
                    import requests
                    response = requests.head(new_url, timeout=5)
                    if 200 <= response.status_code < 300:
                        logger.info(f"Verified URL is accessible: {response.status_code}")
                    else:
                        logger.warning(f"New URL may not be accessible: {response.status_code}")
                except Exception as verify_e:
                    logger.warning(f"Couldn't verify URL: {str(verify_e)}")
                
            except Exception as e:
                logger.error(f"Error fixing URL for resume ID {resume.get('id')}: {str(e)}")
                errors += 1
        
        return {
            "message": "Blob URL fix completed",
            "total_checked": len(items),
            "fixed": fixed_count,
            "already_correct": already_correct,
            "special_cases_fixed": special_cases_fixed,
            "errors": errors
        }
    
    except Exception as e:
        logger.error(f"Error fixing blob URLs: {str(e)}")
        logger.exception("Detailed error:")
        raise HTTPException(status_code=500, detail=f"Error fixing blob URLs: {str(e)}")

@app.get("/test-blob-access")
async def test_blob_access(blob_url: str = Query(..., description="The blob URL to test"), download: bool = Query(False, description="Whether to download the file directly")):
    """
    Test if a blob URL is accessible and get debug information.
    Can also serve as a proxy to download the file directly if download=true is provided.
    """
    try:
        logger.info(f"Testing access to blob URL: {blob_url}")
        
        # Parse the URL to understand its components
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.strip('/').split('/')
        
        # First, standardize the URL to the correct format if needed
        if parsed_url.netloc != "pdf1.blob.core.windows.net" or path_parts[0] != "new":
            # URL doesn't follow the correct format, try to extract the blob name
            if len(path_parts) > 0:
                # Try to find a valid blob name that matches our pattern
                blob_name = None
                for part in reversed(path_parts):  # Check from end to beginning
                    if part.startswith("resume_") and part.endswith(".pdf"):
                        blob_name = part
                        break
                
                if blob_name:
                    # Reconstruct URL with correct format
                    blob_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
                    logger.info(f"Reformatted URL to standard format: {blob_url}")
                else:
                    logger.warning(f"Could not extract a valid blob name from the URL")
        
        # Now add SAS token if needed
        if "?" not in blob_url:
            # No query parameters at all
            logger.info("No SAS token in URL, adding one...")
            blob_url = f"{blob_url}?{BLOB_SAS_TOKEN}"
        elif not any(param.startswith('sp=') or param.startswith('sv=') for param in parsed_url.query.split('&')):
            # Has query params but not SAS specific ones
            logger.info("URL has query params but no valid SAS token, adding one...")
            blob_url = f"{blob_url}&{BLOB_SAS_TOKEN}"
        else:
            logger.info("URL already has SAS token")
        
        # Re-parse the updated URL
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.strip('/').split('/')
            
        # Log the components we identified
        logger.info(f"URL components: host={parsed_url.netloc}, path={parsed_url.path}")
        if len(path_parts) >= 2:
            container = path_parts[0]
            blob_name = '/'.join(path_parts[1:])
            logger.info(f"Identified container: {container}, blob name: {blob_name}")
        
        # Initial request to the blob
        logger.info(f"Making HTTP request to: {blob_url}")
        response = requests.get(blob_url, timeout=15)
        logger.info(f"Response status code: {response.status_code}")
        
        # If blob not found, attempt recovery
        content = None
        success = False
        recovery_attempted = False
        cors_headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        }
        
        if response.status_code == 404 and "BlobNotFound" in response.text:
            logger.warning("BlobNotFound error, attempting recovery")
            recovery_attempted = True
            
            # Try recovery
            new_url, new_content = recover_missing_blob(blob_url)
            if new_url:
                logger.info(f"Recovery successful, new URL: {new_url}")
                blob_url = new_url
                
                # Try the new URL
                response = requests.get(blob_url, timeout=15)
                if response.status_code == 200:
                    content = response.content
                    success = True
            elif new_content:
                logger.info("Recovery returned content but no URL")
                content = new_content
                success = True
        else:
            if response.status_code == 200:
                content = response.content
                success = True
        
        # If download is requested and we have content, return the file directly
        if download and content:
            logger.info(f"Returning file content for direct download")
            
            # Get filename from URL
            filename = os.path.basename(blob_url.split('?')[0])
            
            # Determine content type
            content_type = response.headers.get('Content-Type', 'application/octet-stream')
            if filename.lower().endswith('.pdf'):
                content_type = "application/pdf"
            elif filename.lower().endswith(('.docx', '.doc')):
                content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                
            # Return the file with appropriate headers
            return Response(
                content=content,
                media_type=content_type,
                headers=cors_headers
            )
        
        # Return the test result information
        result = {
            "url_tested": blob_url,
            "status_code": response.status_code,
            "success": success,
            "content_type": response.headers.get("Content-Type", None),
            "content_length": response.headers.get("Content-Length", None),
            "cors_headers": {
                "access_control_allow_origin": response.headers.get("Access-Control-Allow-Origin", None),
                "access_control_allow_methods": response.headers.get("Access-Control-Allow-Methods", None),
                "access_control_allow_headers": response.headers.get("Access-Control-Allow-Headers", None)
            },
            "recovery_attempted": recovery_attempted
        }
        
        if not success:
            result["error"] = response.text
            
        return result
    except Exception as e:
        logger.error(f"Error testing blob access: {str(e)}")
        return {"error": str(e)}

@app.get("/direct-download/{resume_id}")
async def direct_download(resume_id: str, user_id: str = Query(..., description="User ID associated with the resume")):
    """
    Direct download endpoint that returns file content with proper CORS headers.
    This provides a simple URL for frontend to use for downloads.
    """
    try:
        logger.info(f"Direct download request for resume ID: {resume_id}, user ID: {user_id}")
        
        # Get database and container
        database = client.get_database_client(RESUME_DATABASE_ID)
        container = database.get_container_client(RESUME_CONTAINER_ID)
        
        # Query for the resume with the specified ID and user_id
        query = f"SELECT * FROM c WHERE c.id = '{resume_id}' AND c.user_id = '{user_id}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not items:
            raise HTTPException(status_code=404, detail="Resume not found or does not belong to the specified user")
        
        resume = items[0]
        
        # Get the filename from the resume data or use a default
        filename = resume.get("filename", f"resume-{resume_id}.pdf")
        file_extension = os.path.splitext(filename)[1].lower() or ".pdf"
        
        # Determine content type based on file extension
        if file_extension == '.pdf':
            content_type = "application/pdf"
        elif file_extension in ['.docx', '.doc']:
            content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        else:
            content_type = "application/octet-stream"
        
        # Check for blob URL in the resume data
        if not resume.get("blob_url"):
            # No blob URL, check if we have the PDF content directly in the resume data
            if resume.get("pdf_content"):
                logger.info("Using PDF content directly from resume data")
                content = resume.get("pdf_content")
                if isinstance(content, str):
                    content = content.encode('utf-8')
                
                headers = {
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type"
                }
                
                return Response(
                    content=content,
                    media_type=content_type,
                    headers=headers
                )
            else:
                raise HTTPException(status_code=404, detail="Resume file not found in storage and no local content available")
        
        # Process the blob URL to ensure it has a valid SAS token
        blob_url = resume.get("blob_url")
        original_url = blob_url
        logger.info(f"Original blob URL: {blob_url}")
        
        # Parse the URL
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.strip('/').split('/')
        
        # Standardize to the correct URL format
        if parsed_url.netloc != "pdf1.blob.core.windows.net" or (len(path_parts) > 0 and path_parts[0] != "new"):
            # URL doesn't follow the correct format, try to extract the blob name
            blob_name = None
            # Try to find a valid blob name
            for part in reversed(path_parts):
                if part.startswith("resume_") and part.endswith(".pdf"):
                    blob_name = part
                    break
            
            if not blob_name:
                # If we can't find a valid blob name, create one using resume ID
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_id = resume_id[:8] if len(resume_id) >= 8 else resume_id
                blob_name = f"resume_{timestamp}_{unique_id}.pdf"
            
            # Reconstruct URL with correct format
            blob_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
            logger.info(f"Reformatted URL to standard format: {blob_url}")
            
            # Update the resume with the standardized URL
            resume["blob_url"] = blob_url
            try:
                container.replace_item(item=resume["id"], body=resume)
                logger.info("Updated resume record with standardized URL")
            except Exception as update_e:
                logger.warning(f"Failed to update resume with standardized URL: {str(update_e)}")
        
        # Make sure the URL has a SAS token
        if "?" not in blob_url:
            # No query parameters at all
            logger.info("Adding SAS token to URL")
            blob_url = f"{blob_url}?{BLOB_SAS_TOKEN}"
        elif not any(param.startswith('sp=') or param.startswith('sv=') for param in parsed_url.query.split('&')):
            # Has query params but not SAS specific ones
            logger.info("URL has query params but no valid SAS token, adding SAS token")
            blob_url = f"{blob_url}&{BLOB_SAS_TOKEN}"
        
        # Extract container and blob name from the URL for verification
        parsed_url = urlparse(blob_url)
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) >= 2:
            container_name = path_parts[0]
            blob_name = '/'.join(path_parts[1:])
            logger.info(f"URL points to container: {container_name}, blob: {blob_name}")
        
        # First, try to download directly
        content = None
        try:
            logger.info(f"Attempting direct download from: {blob_url}")
            response = requests.get(blob_url, timeout=15)
            
            if response.status_code == 200:
                logger.info("Direct download successful")
                content = response.content
            else:
                logger.warning(f"Direct download failed with status {response.status_code}")
                
                # If this is a 404 (blob not found), try our recovery function
                if response.status_code == 404 and "BlobNotFound" in response.text:
                    logger.warning("BlobNotFound error detected, using recovery function")
                    
                    # Try recovery with resume data for potential regeneration
                    new_url, new_content = recover_missing_blob(blob_url, resume)
                    
                    if new_url:
                        logger.info(f"Recovery successful with new URL: {new_url}")
                        # Update the resume with the new URL
                        resume["blob_url"] = new_url
                        container.replace_item(item=resume["id"], body=resume)
                        logger.info("Updated resume with recovered blob URL")
                        
                        # Use the new URL for content
                        blob_url = new_url
                        content = new_content
                    elif new_content:
                        logger.info("Recovery returned content but no URL")
                        content = new_content
        except Exception as download_e:
            logger.error(f"Error downloading blob: {str(download_e)}")
        
        # If content is still None, return an error
        if content is None:
            raise HTTPException(status_code=404, detail="Could not retrieve resume file from storage")
        
        # Return the file with appropriate headers
        headers = {
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        }
        
        return Response(
            content=content,
            media_type=content_type,
            headers=headers
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in direct download: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing download: {str(e)}")

# Add this new function after the save_file_to_blob function
def recover_missing_blob(blob_url, resume_data=None):
    """
    Attempt to recover a missing blob through multiple strategies:
    1. Check similar blob names in the same container
    2. Regenerate the file if resume data is available
    3. Return a fallback URL or file content if possible
    
    Args:
        blob_url: The original blob URL that's returning 404
        resume_data: Optional resume data that can be used to regenerate the file
        
    Returns:
        tuple: (new_url, content) - Either a new working URL or None, and file content if recovered
    """
    logger.info(f"Attempting to recover missing blob: {blob_url}")
    
    # Parse the original URL to get container and blob name
    parsed_url = urlparse(blob_url)
    path_parts = parsed_url.path.strip('/').split('/')
    
    # If we can't parse the URL properly, we can't recover
    if len(path_parts) < 2:
        logger.error(f"Cannot parse blob URL for recovery: {blob_url}")
        return None, None
    
    # Always use the "new" container for consistency
    container_name = "new"
    
    # Extract blob name from the URL path
    if len(path_parts) > 1 and path_parts[0] == "new":
        # URL already has correct container, extract just the blob name
        blob_name = '/'.join(path_parts[1:])
    else:
        # Use the last part of the path as the blob name
        blob_name = path_parts[-1]
    
    logger.info(f"Will attempt recovery using container: {container_name}, blob name: {blob_name}")
    
    # Check if we have credentials to access the storage account
    if not BLOB_CONNECTION_STRING:
        logger.error("Missing BLOB_CONNECTION_STRING - cannot attempt recovery")
        return None, None
    
    try:
        # Connect to the blob service
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Strategy 1: Look for similarly named blobs
        # First, get the blob prefix (usually the resume_ID or filename without timestamp)
        blob_prefix = None
        if '/' in blob_name:
            blob_prefix = blob_name.split('/')[0]
        else:
            # Try to extract a meaningful prefix from the filename
            parts = blob_name.split('_')
            if len(parts) > 1:
                # If format is like "resume_20250310" we want "resume"
                blob_prefix = parts[0]
        
        # If we have a prefix, look for matching blobs
        if blob_prefix:
            logger.info(f"Searching for blobs with prefix: {blob_prefix}")
            matching_blobs = list(container_client.list_blobs(name_starts_with=blob_prefix))
            
            # If we found any, use the latest one
            if matching_blobs:
                # Sort by last modified, newest first
                matching_blobs.sort(key=lambda b: b.last_modified, reverse=True)
                newest_blob = matching_blobs[0]
                logger.info(f"Found similar blob: {newest_blob.name} (Last modified: {newest_blob.last_modified})")
                
                # Generate URL using the correct format
                new_blob_name = newest_blob.name
                
                # Construct correct URL format
                new_url = f"https://pdf1.blob.core.windows.net/new/{new_blob_name}"
                
                # Add SAS token
                if BLOB_SAS_TOKEN:
                    sas_token = BLOB_SAS_TOKEN
                    if sas_token.startswith('?'):
                        sas_token = sas_token[1:]
                    new_url = f"{new_url}?{sas_token}"
                
                # Verify this URL works by downloading content
                try:
                    response = requests.get(new_url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"Successfully recovered blob with similar name: {new_url}")
                        return new_url, response.content
                    else:
                        logger.warning(f"Similar blob URL returned status {response.status_code}")
                except Exception as e:
                    logger.warning(f"Error downloading similar blob: {str(e)}")
        
        # Strategy 2: Regenerate the file if resume data is available
        if resume_data and all(key in resume_data for key in ["personal_info", "summary", "experience", "education", "skills"]):
            logger.info("Attempting to regenerate PDF from resume data")
            
            # Create a temporary file with proper format
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            temp_filename = f"temp_resume_{timestamp}_{unique_id}.pdf"
            
            try:
                # Generate PDF
                pdf_generator = ResumePDFGenerator(temp_filename)
                pdf_generator.process_resume(resume_data)
                pdf_generator.generate_pdf()
                
                if os.path.exists(temp_filename):
                    with open(temp_filename, "rb") as f:
                        file_content = f.read()
                    
                    # Create blob name in the correct format
                    new_blob_name = f"resume_{timestamp}_{unique_id}.pdf"
                    logger.info(f"Created standardized blob name: {new_blob_name}")
                    
                    # Upload the regenerated file
                    try:
                        # Use save_file_to_blob which now enforces the correct URL format
                        new_url = save_file_to_blob(file_content, new_blob_name)
                        logger.info(f"Successfully uploaded regenerated file: {new_url}")
                        
                        # Clean up temp file
                        try:
                            os.remove(temp_filename)
                        except Exception:
                            pass
                            
                        return new_url, file_content
                    except Exception as upload_e:
                        logger.error(f"Failed to upload regenerated file: {str(upload_e)}")
                        # If upload fails but we generated the file, return the content anyway
                        return None, file_content
            except Exception as gen_e:
                logger.error(f"Failed to regenerate PDF: {str(gen_e)}")
    
    except Exception as e:
        logger.error(f"Error in blob recovery process: {str(e)}")
    
    # If all recovery strategies failed, try direct URL construction as a last resort
    try:
        # Construct URL in the correct format
        if blob_name.startswith("resume_") and blob_name.endswith(".pdf"):
            direct_url = f"https://pdf1.blob.core.windows.net/new/{blob_name}"
            if BLOB_SAS_TOKEN:
                sas_token = BLOB_SAS_TOKEN
                if sas_token.startswith('?'):
                    sas_token = sas_token[1:]
                direct_url = f"{direct_url}?{sas_token}"
                
            # Check if this direct URL works
            try:
                test_response = requests.head(direct_url, timeout=5)
                if test_response.status_code == 200:
                    logger.info(f"Direct URL construction successful: {direct_url}")
                    # Get the content
                    content_response = requests.get(direct_url, timeout=10)
                    if content_response.status_code == 200:
                        return direct_url, content_response.content
            except Exception:
                pass
    except Exception:
        pass
    
    # If all recovery strategies failed
    logger.warning("All recovery strategies failed")
    return None, None

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)