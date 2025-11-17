import time
import difflib
import re
from enum import Enum
from typing import List, Dict, Tuple
from dma_sdk.utils import prepare_api_url, prepare_headers, post_data, get_data


__all__ = [
    'create_organization',
    'translate_queries',
    'view_translation_results_as_html',
    'translate_queries_and_render_results',
    'view_last_translation',
    'get_context_info',
]

DEFAULT_HOST = "https://app.datafold.com"
DEFAULT_ORG_TOKEN = "zda4wct*ZBF3ybt3vfz"


class TranslationJobStatus(str, Enum):
    """Translation job statuses (for polling)"""

    DONE = "done"
    FAILED = "failed"


class TranslationStatus(str, Enum):
    """Translation result statuses (for individual queries)"""

    NO_TRANSLATION_ATTEMPTS = "no_translation_attempts"
    VALIDATION_PENDING = "validation_pending"
    INVALID_TRANSLATION = "invalid_translation"
    VALID_TRANSLATION = "valid_translation"


class FailureReason(str, Enum):
    """Reasons why a translation agent failed to complete its task"""

    MAX_ITERATIONS = "max_iterations"
    TOOL_ERROR = "tool_error"
    RESIGNATION = "resignation"


_notebook_host = None
_current_api_key = None
_identity = None
_last_project_id = None
_last_translation_id = None


def get_context_info() -> dict[str, str]:
    """
    Collect basic identity information from Databricks runtime.

    This function must be called within a Databricks notebook environment.
    We collect basic identity information to help track and resolve any issues
    with SQL translation and provide you with the best experience. This data is
    used internally by Datafold only and helps us:
    - Diagnose translation errors specific to your workspace configuration
    - Improve translation quality based on real usage patterns
    - Provide better support when you need assistance

    Returns:
        dict: Context information including workspace_id, workspace_url, cluster_id, notebook_path, and user
    """
    try:
        # Access the global dbutils object provided by Databricks runtime
        import __main__

        dbutils = getattr(__main__, 'dbutils', None)
        if dbutils is None:
            return {}

        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return {
            'workspace_id': context.workspaceId().get(),
            'workspace_url': context.browserHostName().get(),
            'cluster_id': context.clusterId().get(),
            'notebook_path': context.notebookPath().get(),
            'user': context.userName().get(),
        }
    except Exception:
        # If not running in Databricks or any error occurs, return empty dict
        return {}


def _get_identity() -> dict[str, str] | None:
    """Get the identity."""
    global _identity
    if _identity is not None:
        return _identity
    return None


def _set_identity(identity: dict[str, str] | None) -> None:
    """Set the identity."""
    global _identity
    _identity = identity


def _get_host(host: str | None) -> str:
    """Get the host to use, checking notebook-level variable first.
    If a host is explicitly provided, update _notebook_host for future calls.
    """
    global _notebook_host
    if host is not None:
        _notebook_host = host
        return host
    if _notebook_host is not None:
        return _notebook_host
    return DEFAULT_HOST


def _get_current_api_key(org_token: str | None, host: str | None = None) -> str | None:
    """Get the current API key."""
    global _current_api_key
    if _current_api_key is not None:
        return _current_api_key
    elif org_token is not None:
        host = _get_host(host)
        api_key, org_id = create_organization(org_token, host)
        _set_current_api_key(api_key)
        return api_key
    else:
        return None


def _set_current_api_key(api_key: str) -> None:
    """Set the current API key."""
    global _current_api_key
    _current_api_key = api_key


def create_organization(org_token: str, host: str | None = None) -> Tuple[str, int]:
    """
    Call the /org API endpoint to create a new organization.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        org_token: Donor org token from where to copy the organization from

    Returns:
        Tuple of (api_key, org_id)
    """
    host = _get_host(host)
    url = prepare_api_url(host, "org")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {org_token}"}

    response = post_data(url, headers=headers)
    result = response.json()
    api_key = result['api_token']
    _set_current_api_key(api_key)
    org_id = result['org_id']

    print(f"✓ Organization created with id {org_id}")

    return api_key, org_id


def translate_queries(api_key: str, queries: List[str], host: str | None = None) -> Tuple[int, int]:
    """
    Main entry point to translate a query end to end.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication
        queries: List of SQL queries to translate

    Returns:
        Tuple of (project_id, translation_id)
    """
    host = _get_host(host)

    # Create DMA project
    data_sources = _get_data_sources(api_key, host)
    source_data_source_id = [d for d in data_sources if d['type'] != "databricks"][0]['id']
    target_data_source_id = [d for d in data_sources if d['type'] == "databricks"][0]['id']
    project = _create_dma_project(
        api_key, source_data_source_id, target_data_source_id, 'Databricks Notebook Project', host
    )
    project_id = project['id']
    print(f"✓ Project created with id {project_id}")

    # Upload queries to translate
    _upload_queries(host=host, api_key=api_key, project_id=project_id, queries=queries)
    print(f"✓ Queries uploaded")

    # Start translating queries
    translation_id = _start_translation(api_key, project_id, host)
    print(f"✓ Started translation with id {translation_id}")

    # Store for later retrieval
    global _last_project_id, _last_translation_id
    _last_project_id = project_id
    _last_translation_id = translation_id

    return project_id, translation_id


def view_translation_results_as_html(
    api_key: str, project_id: int, translation_id: int, host: str | None = None
) -> str:
    """
    View translation results

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate
        translation_id: Translation ID used to translate
    Returns:
        str: html string to be displayed in Jupyter Notebook
    """
    host = _get_host(host)

    translation_results = _wait_for_translation_results(
        api_key, project_id, translation_id, 5, host
    )
    return _translation_results_html(translation_results)

def view_translation_results_as_dict(
    api_key: str, project_id: int, translation_id: int, host: str | None = None
) -> dict:
    """
    View translation results

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate
        translation_id: Translation ID used to translate
    Returns:
        str: html string to be displayed in Jupyter Notebook
    """
    host = _get_host(host)

    translation_results = _wait_for_translation_results(
        api_key, project_id, translation_id, 5, host
    )
    return translation_results

def translate_queries_and_render_results(
    queries: List[str],
    source_type: str | None = None,
    target_type: str | None = None,
    org_token: str | None = None,
    include_identity: bool = True,
    host: str | None = None
) -> None:
    """
    Translate SQL queries and render results.

    Args:
        queries: List of SQL queries to translate
        source_type: Source database type (e.g., 'snowflake', 'redshift').
                     If None, uses first non-target data source
        target_type: Target database type (e.g., 'bigquery', 'databricks').
                     If None, uses 'databricks' for backward compatibility
        org_token: Organization token for authentication (defaults to DEFAULT_ORG_TOKEN)
        include_identity: Whether to collect and send identity info (default: True)
        host: Host URL for Datafold instance (defaults to DEFAULT_HOST)
    """
    # Use default org token if not provided
    if org_token is None:
        org_token = DEFAULT_ORG_TOKEN

    # Auto-collect identity if include_identity is True
    identity = None
    if include_identity:
        identity = get_context_info()

    api_key = _get_current_api_key(org_token, host)
    _set_identity(identity)
    if api_key is None:
        raise ValueError(
            "API key is not set. Please call create_organization or set the API key manually."
        )

    # Get host
    host = _get_host(host)

    # Create DMA project with specified source and target types
    data_sources = _get_data_sources(api_key, host)

    # Determine target type (default to databricks for backward compatibility)
    if target_type is None:
        target_type = 'databricks'

    # Find source and target data sources
    if source_type:
        source_ds = next((d for d in data_sources if d['type'] == source_type), None)
        if source_ds is None:
            raise ValueError(f"No {source_type} data source found. Please configure a {source_type} data source.")
    else:
        # Use first non-target data source
        source_ds = next((d for d in data_sources if d['type'] != target_type), None)
        if source_ds is None:
            raise ValueError(f"No source data source found (looking for non-{target_type}).")

    target_ds = next((d for d in data_sources if d['type'] == target_type), None)
    if target_ds is None:
        raise ValueError(f"No {target_type} data source found. Please configure a {target_type} data source.")

    source_data_source_id = source_ds['id']
    target_data_source_id = target_ds['id']

    project_name = f"{source_ds['type'].title()} to {target_ds['type'].title()} Translation"
    project = _create_dma_project(
        api_key, source_data_source_id, target_data_source_id, project_name, host
    )
    project_id = project['id']
    print(f"✓ Project created with id {project_id}")

    # Upload queries to translate
    _upload_queries(host=host, api_key=api_key, project_id=project_id, queries=queries)
    print(f"✓ Queries uploaded")

    # Start translating queries
    translation_id = _start_translation(api_key, project_id, host)
    print(f"✓ Started translation with id {translation_id}")

    # Store for later retrieval
    global _last_project_id, _last_translation_id
    _last_project_id = project_id
    _last_translation_id = translation_id

    # Wait for and display results
    translation_results = _wait_for_translation_results(
        api_key, project_id, translation_id, 5, host
    )
    html = _translation_results_html(translation_results)

    from IPython.display import HTML, display
    display(HTML(html))

def translate_queries_and_get_results(queries: List[str],
    org_token: str | None = None,
    include_identity: bool = True,
    host: str | None = None) -> dict:
    if org_token is None:
        org_token = DEFAULT_ORG_TOKEN

    # Auto-collect identity if include_identity is True
    identity = None
    if include_identity:
        identity = get_context_info()

    api_key = _get_current_api_key(org_token, host)
    _set_identity(identity)
    if api_key is None:
        raise ValueError(
            "API key is not set. Please call create_organization or set the API key manually."
        )
    project_id, translation_id = translate_queries(api_key, queries, host)
    translation_results = view_translation_results_as_dict(api_key, project_id, translation_id)
    return translation_results


def view_last_translation(
    org_token: str | None = None,
    host: str | None = None,
) -> None:
    """
    Re-display the results of the last translation.

    Useful for debugging - fetches and displays results without re-running the translation.

    Args:
        org_token: Organization token for authentication (defaults to DEFAULT_ORG_TOKEN)
        host: Host URL for Datafold instance (defaults to DEFAULT_HOST)

    Example:
        view_last_translation()
    """
    global _last_project_id, _last_translation_id

    if _last_project_id is None or _last_translation_id is None:
        print("No previous translation found. Run translate_queries_and_render_results() first.")
        return

    # Use default org token if not provided
    if org_token is None:
        org_token = DEFAULT_ORG_TOKEN

    api_key = _get_current_api_key(org_token, host)
    if api_key is None:
        raise ValueError(
            "API key is not set. Please call create_organization or set the API key manually."
        )

    print(f"Fetching results for project {_last_project_id}, translation {_last_translation_id}...")

    # Fetch results directly without waiting (results should already be available)
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{_last_project_id}/translate/jobs/{_last_translation_id}")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    response = get_data(url, headers=headers)
    result = response.json()

    html = _translation_results_html(result)

    from IPython.display import HTML, display

    display(HTML(html))


def _get_data_sources(api_key: str, host: str | None = None) -> List[Dict]:
    """
    Fetch all data sources from the Datafold API.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication

    Returns:
        List of data source dictionaries
    """
    host = _get_host(host)
    url = prepare_api_url(host, "api/v1/data_sources")
    headers = prepare_headers(api_key)
    response = get_data(url, headers=headers)
    return response.json()


def _create_dma_project(
    api_key: str, source_ds_id: int, target_ds_id: int, name: str, host: str | None
) -> Dict:
    """
    Create a DMA project.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication
        source_ds_id: Source data source ID
        target_ds_id: Target data source ID
        name: Project name

    Returns:
        Created project dictionary
    """
    host = _get_host(host)
    url = prepare_api_url(host, "api/internal/dma/projects")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    payload = {
        "name": name,
        "from_data_source_id": source_ds_id,
        "to_data_source_id": target_ds_id,
        "version": 2,
        "settings": {
            "error_on_zero_diff": False,
            "transform_group_creation_strategy": "group_individual_operations",
            "experimental": {
                "import_sql_files_as_script_objects": True,
                "infer_schema_from_scripts": True,
                "generate_synthetic_data": True,
            },
        },
    }

    response = post_data(url, json_data=payload, headers=headers)
    return response.json()['project']


def _upload_queries(
    api_key: str, project_id: int, queries: List[str], host: str = DEFAULT_HOST
) -> Dict:
    """
    Upload multiple queries to be translated.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Your API authentication token
        project_id: The project ID to upload to
        queries: List of queries to upload

    Returns:
        dict: Response with upload statistics including per-file results
    """
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{project_id}/files")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    payload = {
        'files': [
            {"filename": f"query_{i+1}.sql", "content": query} for i, query in enumerate(queries)
        ]
    }
    response = post_data(url, json_data=payload, headers=headers)
    return response.json()


def _start_translation(api_key: str, project_id: int, host: str = DEFAULT_HOST) -> int:
    """
    Start translation

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate

    Returns:
        int: Translation task ID
    """
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{project_id}/translate/jobs")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    response = post_data(
        url,
        json_data={"project_id": project_id, "identity": _get_identity()},
        headers=headers,
    )
    translation_id = response.json()["task_id"]
    return translation_id


def _wait_for_translation_results(
    api_key: str, project_id: int, translation_id: int, poll_interval: int, host: str = DEFAULT_HOST
) -> Dict:
    """
    Poll for translation completion

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate
        translation_id: Translation ID used to translate
        poll_interval: Seconds between status checks

    Returns:
        dict: Final translation result
    """
    from IPython.display import clear_output
    import sys

    host = _get_host(host)
    url = prepare_api_url(
        host, f"api/internal/dma/v2/projects/{project_id}/translate/jobs/{translation_id}"
    )
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    spinner_speed = 0.1  # seconds between spinner frames

    last_check_time = 0
    i = 0

    while True:
        current_time = time.time()

        # Check API status at poll_interval
        if current_time - last_check_time >= poll_interval:
            response = get_data(url, headers=headers)
            result = response.json()
            status = result["status"]

            if status in [TranslationJobStatus.DONE, TranslationJobStatus.FAILED]:
                # Count translations
                translated_models = result.get("translated_models", [])
                total_translations = len(translated_models)
                validated_count = sum(
                    1 for model in translated_models
                    if model.get("translation_status") == TranslationStatus.VALID_TRANSLATION
                )
                
                print(f"\r✓ Translation completed with status: {status}")
                if total_translations > 0:
                    print(f"✓ Validated {validated_count} out of {total_translations} translations")
                sys.stdout.flush()
                return result

            last_check_time = current_time

        # Update spinner display more frequently
        print(f"\r{spinner[i % len(spinner)]} Waiting for translation results...", end='')
        sys.stdout.flush()
        i += 1
        time.sleep(spinner_speed)


def _translation_results_html(translation_results: Dict) -> str:
    """
    Generate HTML representation of translation results.

    Args:
        translation_results: Translation results dictionary

    Returns:
        str: HTML string for display
    """
    html = []

    # Sort models by filename with natural sorting (query_1, query_2, ..., query_10, query_11)
    def natural_sort_key(model: Dict) -> tuple:
        """Extract numeric parts from filename for natural sorting"""
        filename = model.get('source_filename') or model['asset_name']
        # Extract numbers from the filename
        numbers = re.findall(r'\d+', filename)
        if numbers:
            return (int(numbers[0]), filename)
        return (0, filename)

    models = translation_results['translated_models']
    models_sorted = sorted(models, key=natural_sort_key)

    for model in models_sorted:
        filename = model.get('source_filename') or model['asset_name']
        status = model['translation_status']
        # Check for success status - anything else is a failure
        icon = '✅' if status == TranslationStatus.VALID_TRANSLATION else '⚠️'
        button_text = f"{icon} {filename}"
        html.append(
            f"""
        <button class="collapsible" onclick="toggleCollapse(this)">
            {button_text}
        </button>
        <div class="content">
            {_render_translated_model_as_html(model)}
        </div>
        """
        )
    if not translation_results['translated_models']:
        return """No queries were translated."""

    style = """
    <style>
        .collapsible {
            background-color: #f1f1f1;
            color: #333;
            cursor: pointer;
            padding: 18px;
            width: 100%;
            border: 1px solid #ddd;
            text-align: left;
            outline: none;
            font-size: 16px;
            font-family: sans-serif;
            margin-top: 10px;
            transition: background-color 0.3s;
        }
        .collapsible:hover {
            background-color: #e0e0e0;
        }
        .collapsible.active {
            background-color: #d0d0d0;
        }
        .collapsible::before {
            content: '▶ ';
            display: inline-block;
            margin-right: 8px;
            transition: transform 0.3s;
        }
        .collapsible.active::before {
            transform: rotate(90deg);
        }
        .content {
            padding: 0 18px;
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
            background-color: white;
        }
        .content.active {
            max-height: 10000px;
            padding: 18px;
        }
    </style>
    """

    script = """
    <script>
        function toggleCollapse(element) {
            element.classList.toggle('active');
            const content = element.nextElementSibling;
            content.classList.toggle('active');
        }
    </script>
    """

    html.insert(0, ''.join([style, script]))
    return ''.join(html)


def _render_translated_model_as_html(model: Dict) -> str:
    """
    Render a single translated model as HTML with diff highlighting.

    Args:
        model: Model dictionary containing source and target SQL

    Returns:
        str: HTML string with diff visualization
    """
    source_sql = model['source_sql']
    target_sql = model['target_sql'] or ''
    status = model['translation_status']
    asset_name = model['asset_name']

    # Determine what we have
    has_translation_result = target_sql and target_sql.strip()
    is_failed = status != TranslationStatus.VALID_TRANSLATION

    # Build warning HTML if failed
    warning_html = ""
    if is_failed:
        failure_summary = model.get('failure_summary')

        if failure_summary:
            problem = failure_summary.get('problem', '')
            error_message = failure_summary.get('error_message', '')
            solution = failure_summary.get('solution', '')
            location = failure_summary.get('location')
            reason = failure_summary.get('reason', '')

            failure_content = f"""
                <div class="failure-section">
                    <div class="failure-label">Problem:</div>
                    <div class="failure-text">{problem}</div>
                </div>
                {f'<div class="failure-section"><div class="failure-label">Location:</div><div class="failure-text">{location}</div></div>' if location else ''}
                <div class="failure-section">
                    <div class="failure-label">Error:</div>
                    <div class="failure-text">{error_message}</div>
                </div>
                <div class="failure-section">
                    <div class="failure-label">Solution:</div>
                    <div class="failure-text">{solution}</div>
                </div>
                <div class="failure-section">
                    <div class="failure-label">Reason:</div>
                    <div class="failure-text">{reason}</div>
                </div>
            """
        else:
            failure_content = f'<div class="warning-message">The translation for "{asset_name}" could not be completed. Status: {status}</div>'

        warning_html = f"""
        <div class="warning-box">
            <div class="warning-title">⚠ Translation Failed</div>
            {failure_content}
        </div>
        """

    # Build diff HTML if we have translation results
    diff_html = ""
    if has_translation_result:
        source_lines = source_sql.splitlines()
        target_lines = target_sql.splitlines()

        differ = difflib.Differ()
        diff = list(differ.compare(source_lines, target_lines))

        source_html = []
        target_html = []

        i = 0
        while i < len(diff):
            line = diff[i]

            if line.startswith('  '):  # Unchanged line
                content = line[2:]
                source_html.append(f'<div class="line unchanged">{content}</div>')
                target_html.append(f'<div class="line unchanged">{content}</div>')
                i += 1
            elif line.startswith('- '):  # Line only in source
                content = line[2:]
                source_html.append(f'<div class="line removed">{content}</div>')
                i += 1
            elif line.startswith('+ '):  # Line only in target
                content = line[2:]
                target_html.append(f'<div class="line added">{content}</div>')
                i += 1
            elif line.startswith('? '):  # Hint line (skip)
                i += 1
            else:
                i += 1

        diff_html = f"""
        <div class="sql-container">
            <div class="sql-column">
                <h3>Snowflake SQL</h3>
                {''.join(source_html)}
            </div>
            <div class="sql-column">
                <h3>Databricks SQL</h3>
                {''.join(target_html)}
            </div>
        </div>
        """

    # Assemble final HTML
    content_html = warning_html + diff_html
    if not content_html:
        content_html = "<p>No translation results available.</p>"

    return f"""
    <style>
        .warning-box {{
            background-color: #fff3cd;
            border: 1px solid #ffc107;
            border-left: 4px solid #ff9800;
            padding: 20px;
            margin: 10px 0;
            font-family: sans-serif;
        }}
        .warning-title {{
            color: #856404;
            font-weight: bold;
            font-size: 16px;
            margin-bottom: 15px;
        }}
        .warning-message {{
            color: #856404;
        }}
        .failure-section {{
            margin: 12px 0;
        }}
        .failure-label {{
            color: #856404;
            font-weight: bold;
            font-size: 13px;
            margin-bottom: 4px;
        }}
        .failure-text {{
            color: #856404;
            font-size: 13px;
            line-height: 1.5;
            white-space: pre-wrap;
        }}
        .sql-container {{
            display: flex;
            gap: 20px;
            font-family: monospace;
        }}
        .sql-column {{
            flex: 1;
            border: 1px solid #ddd;
            padding: 15px;
            background-color: #f5f5f5;
            overflow-x: auto;
        }}
        .sql-column h3 {{
            margin-top: 0;
            color: #333;
            font-family: sans-serif;
        }}
        .line {{
            font-size: 12px;
            line-height: 1.6;
            padding: 2px 4px;
            white-space: pre-wrap;
        }}
        .unchanged {{
            background-color: transparent;
        }}
        .removed {{
            background-color: #ffecec;
            color: #d73a49;
        }}
        .added {{
            background-color: #e6ffec;
            color: #22863a;
        }}
    </style>

    {content_html}
    """
