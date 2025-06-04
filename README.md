# Skill Proficiency AI Tagger

A Streamlit-based web application that leverages GenAI for automated course-to-skills tagging at proficiency levels. The application provides an intuitive interface for uploading course data, processing it through AI pipelines, and downloading structured results.

## 🚀 Features

- **File Upload & Validation**: Upload SFW (Skills Framework) and sector-specific course files with automatic validation
- **AI-Powered Processing**: Two-round AI pipeline for skill extraction and proficiency tagging
- **Checkpoint System**: Resume interrupted processing from previous checkpoints
- **Real-time Status Updates**: Live progress tracking with Streamlit spinners and captions
- **Results Preview & Download**: Interactive preview of processed data with CSV download options
- **Sector-Specific Processing**: Support for multiple sectors with automatic preprocessing
- **Session Management**: Persistent session state with database cleanup utilities

## 📁 Project Structure

```
src/
├── app.py                          # Main Streamlit application entry point
├── config.py                       # Configuration settings
├── controllers/
│   └── upload_controller.py        # File upload handling logic
├── frontend/
│   ├── homepage.py                 # Homepage with action cards and status
│   ├── upload_page.py              # File upload interface
│   ├── checkpoint_page.py          # Checkpoint loading interface
│   ├── results_page.py             # Results preview and download
│   ├── sidebar_page.py             # Navigation sidebar
│   └── components/                 # Reusable UI components
├── services/
│   ├── db/                         # Database and session management
│   ├── ingestion/                  # File processing pipelines
│   ├── llm_pipeline/               # AI processing workflows
│   ├── validation/                 # Input validation services
│   └── checkpoint/                 # Checkpoint management
├── models/                         # Data schemas and prompt templates
├── utils/                          # Utility functions
└── exceptions/                     # Custom exception classes
```

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.10+
- Docker (optional, for containerized deployment)

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd jst-gt
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Run the application**
   ```bash
   cd src
   streamlit run app.py
   ```

   The application will be available at `http://localhost:8501`

### Docker Deployment

1. **Build the Docker image**
   ```bash
   docker build -t skill-proficiency-ai-tagger .
   ```

2. **Run locally for testing**
   ```bash
   docker run -p 8501:8501 skill-proficiency-ai-tagger
   ```

3. **Production deployment with resource limits**
   ```bash
   docker run -d \
     --name streamlit-app \
     -p 8501:8501 \
     --memory=512m \
     --cpus=0.5 \
     --restart=unless-stopped \
     skill-proficiency-ai-tagger
   ```

## 📖 Usage Guide

### 1. **Homepage Navigation**
- Access the main interface at `homepage()`
- View status messages and available actions via `home_status_messages()`
- Use action cards to start new processing or load checkpoints

### 2. **File Upload Process**
- Navigate to the upload page using `upload_file_page()`
- Select your target sector using `sector_selector()`
- Upload both SFW and sector files via `file_selector()`
- Files are validated through `upload_controller.py`

### 3. **Processing Pipeline**
- Core processing handled by `handle_core_processing()`
- Two-round AI pipeline for comprehensive skill extraction
- Real-time progress updates through Streamlit interface
- Automatic checkpoint creation for resumability

### 4. **Checkpoint Recovery**
- Access checkpoint loading via `checkpoint_page()`
- Resume interrupted processing from saved state
- Automatic validation of checkpoint integrity

### 5. **Results Management**
- Preview results in `results_page()`
- Download processed CSV files using `view_download_csvs()`
- Session management through `session_management.py`

## 🔧 Configuration

### Environment Variables
Configure the application through the .env file:

```env
# Example configuration
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET_NAME=your_s3_bucket
```

### Sector Configuration
Sector-specific settings are managed in `config.yaml` and skill mappings in `skill_rac_chart.yaml`.

## 🏗️ Application Architecture

### Frontend Components
- **Page Header**: Consistent branding via `create_header()`
- **Sidebar Navigation**: Multi-page navigation through `sidebar_nav()`
- **Help & Contact**: User guidance via `sidebar_help()` and `sidebar_contact()`

### State Management
- Session state initialization through `init_session_state()`
- Page configuration via `configure_page()`
- Database cleanup utilities in `wipe_db()`

### Processing Pipeline
1. **File Validation**: Input validation through `input_validation.py`
2. **Preprocessing**: Sector-specific preprocessing via `sector_file_processing.py`
3. **AI Processing**: Two-round pipeline in `combined_pipeline.py`
4. **Results Generation**: Output formatting and storage

## 🔍 Key Features

### Session State Management
The application uses Streamlit's session state for:
- `app_stage`: Current application stage tracking
- `processing`: Processing status flag
- `csv_yes`: Results availability flag
- `pkl_yes`: Checkpoint availability flag
- `results`: Processed data storage

### File Validation
- Filename validation through `validate_sector_filename()`
- Content validation for required columns and data types
- Automatic preprocessing for sector-specific formats

### Error Handling
Custom exception classes in `exceptions/`:
- `FileValidationError`
- `DataValidationError`
- `StorageError`

## 🛡️ Health Monitoring

The Docker container includes health checks:
```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1
```

## 📄 Documentation

Additional documentation and user guides are available through the application's help section. Download the comprehensive user format guide via the sidebar help menu.

## 🚦 Application Flow

1. **Initialization**: `main()` → `configure_page()`
2. **Navigation**: `demo_sidebar()` for consistent navigation
3. **Stage Routing**: Based on `st.session_state.app_stage`:
   - `"initial_choice"` → `homepage()`
   - `"uploading_new"` → `upload_file_page()`
   - `"load_checkpoint"` → `checkpoint_page()`
   - `"results_ready"` → `results_page()`

This architecture ensures a smooth, intuitive user experience while maintaining robust error handling and state management throughout the application lifecycle.
