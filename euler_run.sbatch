#!/bin/bash
#SBATCH --job-name=change_me
#SBATCH --account=change_me
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=62500
#SBATCH --time=100:00:00
#SBATCH --output=/cluster/home/%u/Euler_logs/output_%j.log
#SBATCH --error=/cluster/home/%u/Euler_logs/error_%j.log

# =============================================================
# Overview:
# Batch script to run multiple Nexus-E simulations on Euler cluster
#
# Read "SETUP and USAGE" on how to define parameters/scenarios/databases (necessary), 
#      "PURPOSE" to see overview of what happens (necessary), 
#      "OUTPUTS/CHECK PROGRESS" to see where to find results and logs (necessary),
#      "WORKFLOW" to understand the steps taken (optional)
#      "MAIN SCRIPT" for the actual code (very optional)
# =============================================================

# =============================================================
# SETUP and USAGE:
# =============================================================
#   0. (once) Check if file with personal settings exists:
#      modules_playlists/euler_run.config.local
#      If not, create it by copying the example:
#        cp modules_playlists/euler_run.config.example modules_playlists/euler_run.config.local
#        (On Windows: copy modules_playlists\euler_run.config.example modules_playlists\euler_run.config.local)
#
#   1. Change the parameters at the top of this file (the #SBATCH lines) as you
#      see fit. Especially, keeping --account=change_me won't work.
#      NEVER COMMIT the specific --account values you use.
#
#   2. Edit modules_playlists/euler_run.config.local with your settings:
#      - JOBS: Array of "database|playlist" pairs to run
#              Example: "test_nuclear_all_eu|base"
#      - REPO_PATH: Path to your nexus-e-framework repository on Euler
#                   Example: "$HOME/repos/nexus-e-framework"
#
#   3. Check config.toml in your REPO_PATH to ensure correct settings (other than database and playlist)
#
#   4. Run the script to submit jobs:
#      sbatch euler_run.sbatch
#
#   5. Monitor progress via the manifest file created in ~/Euler_logs/
# =============================================================


# ============================================
# PURPOSE:
# ============================================
#   1) Gets job configurations: database and playlist pairs (edit JOBS array above)
#   2) Copies nexus-e-framework from REPO_PATH to scratch storage
#   3) Patches config.toml with database name and playlist name for each instance
#   4) Runs nexus-e-framework (same database can run multiple times with different playlists)
#   5) Records results location in shared manifest file
#
#
# ============================================
# OUTPUTS/CHECK PROGRESS:
# ============================================
#   - Manifest: ~/Euler_logs/multiRun_${TIMESTAMP}.manifest
#     Contains readable [START], [COMPLETE], and [FAILED] entries for each job
#     [START] entries include all log paths for monitoring
#     [COMPLETE] entries include results locations
#     [FAILED] entries include exit code and error log path
#   - Job logs: ~/Euler_logs/output_${JOB_ID}.log and error_${JOB_ID}.log
#   - App logs: /cluster/scratch/$USER/run_${JOB_ID}/nexus-e-framework/nexus-e.log
#   - Results: In scratch storage (copy to permanent storage if needed)
#
# ============================================
# WORKFLOW:
# ============================================
#   1. Creates experiment tag (timestamp) and manifest file in ~/Euler_logs/
#   2. Submits one job per database-playlist pair from JOBS array in modules_playlists/euler_run.config.local (each runs independently in parallel)
#   3. Each job:
#      - Creates scratch directory: /cluster/scratch/$USER/run_${SLURM_JOB_ID}/
#      - Copies from repository path (default: ~/repos/nexus-e-framework/):
#          * The whole folder except any .git folder 
#      - Patches config.toml to set:
#          * database name in [data_context].name
#          * playlist name in [modules].playlist_name
#      - Runs: nexus-e --run
#      - Results stored in: scratch/nexus-e-framework/Results/run_${DATABASE}_${PLAYLIST_NAME}/
#      - Records absolute path to results in manifest file
#
# ============================================


# ============================================================
# MAIN SCRIPT
# Note:
# 99% percent of the users should not read the code below this line!
# ============================================================
# Load configuration from local file (git-ignored, per-user settings)
# Try to locate the script directory
if [[ -n "${SLURM_SUBMIT_DIR:-}" ]]; then
  # When running via sbatch, use the directory where sbatch was called
  SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
  # When running directly, use the script's location
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

CONFIG_FILE="$SCRIPT_DIR/modules_playlists/euler_run.config.local"
echo "Looking for config at: $CONFIG_FILE"

if [[ -f "$CONFIG_FILE" ]]; then
  source "$CONFIG_FILE"
  echo "Loaded config from: $CONFIG_FILE"
else
  echo "WARNING: modules_playlists/euler_run.config.local not found"
  echo "Checked path: $CONFIG_FILE"
  echo "Please create it: cp modules_playlists/euler_run.config.example modules_playlists/euler_run.config.local"
  echo "Using fallback defaults (may not work for your setup)"

  # Fallback defaults (if no local config exists)
  JOBS=()
  REPO_PATH="${REPO_PATH:-$HOME/repos/nexus-e-framework}"
  EMAIL_ADDRESS=""
fi

# Set email address for SLURM notifications if provided
if [[ -n "${EMAIL_ADDRESS:-}" ]]; then
  export SBATCH_MAIL_USER="$EMAIL_ADDRESS"
  echo "Email notifications enabled for: $EMAIL_ADDRESS"
else
  echo "No email address configured - notifications disabled"
fi
# ============================================================
# Submit jobs for all databases or run single job
# ============================================================
if [[ -z "${DATABASE:-}" ]]; then
  # No DATABASE provided - submit one job per database
  mkdir -p "$HOME/Euler_logs"



  if [[ ${#JOBS[@]} -eq 0 ]]; then
    echo "ERROR: JOBS list is empty."
    echo "Please fill the JOBS array at the top of the script before running."
    exit 2
  fi



  EXPERIMENT_TAG="${EXPERIMENT_TAG:-$(date +%Y%m%d_%H%M%S)}"
  MANIFEST_PATH="$HOME/Euler_logs/multiRun_${EXPERIMENT_TAG}.manifest"



  {
    echo "# Multi-run manifest for ${EXPERIMENT_TAG}"
    echo "# "
    echo "# Each job creates entries:"
    echo "#   [START] - Written when job starts, includes log file paths"
    echo "#   [COMPLETE] - Written when job finishes successfully"
    echo "#   [FAILED] - Written if job fails, includes exit code"
    echo "# "
    echo "# Monitor running jobs using: tail -f <log_path> or cat <log_path>"
    echo ""
  } > "$MANIFEST_PATH"



  echo "Submitting jobs for EXPERIMENT_TAG=${EXPERIMENT_TAG}"
  for job in "${JOBS[@]}"; do
    # Split "database|playlist" into separate variables
    IFS='|' read -r db playlist <<< "$job"
    echo "  -> DATABASE=$db, PLAYLIST=$playlist"

    # Build sbatch command with optional email notifications
    SBATCH_CMD="sbatch --export=ALL,DATABASE=\"$db\",PLAYLIST_NAME=\"$playlist\",EXPERIMENT_TAG=\"$EXPERIMENT_TAG\",MANIFEST_PATH=\"$MANIFEST_PATH\""

    if [[ -n "${EMAIL_ADDRESS:-}" ]]; then
      SBATCH_CMD="$SBATCH_CMD --mail-type=END,FAIL --mail-user=\"$EMAIL_ADDRESS\""
    fi

    SBATCH_CMD="$SBATCH_CMD \"$0\""

    eval $SBATCH_CMD
  done



  echo "Submitted. Manifest will be updated as jobs finish:"
  echo "  $MANIFEST_PATH"
  exit 0
fi



# ============================================================
# Run calibration for DATABASE=${DATABASE}
# ============================================================
set -euo pipefail

# Error handler - records failure in manifest if job fails
record_failure() {
  local exit_code=$?
  FAIL_ENTRY="[FAILED] Job ${SLURM_JOB_ID} | DB: ${DATABASE} | Playlist: ${PLAYLIST_NAME}
  Exit code: ${exit_code}
  Check error log: $HOME/Euler_logs/error_${SLURM_JOB_ID}.log"
  
  if command -v flock >/dev/null 2>&1; then
    (
      flock -x 200
      printf "%s\n\n" "$FAIL_ENTRY" >&200
    ) 200>>"$MANIFEST_PATH"
  else
    printf "%s\n\n" "$FAIL_ENTRY" >>"$MANIFEST_PATH"
  fi
  
  echo "Job failed with exit code ${exit_code}. Recorded to manifest."
  exit "$exit_code"
}

# Set trap to catch any errors
trap record_failure ERR



SIM_FOLDER="run_${DATABASE}_${PLAYLIST_NAME}"



mkdir -p "$HOME/Euler_logs"



echo "Starting job=${SLURM_JOB_ID} DATABASE=${DATABASE} EXPERIMENT_TAG=${EXPERIMENT_TAG:-}"
MANIFEST_PATH="${MANIFEST_PATH:-$HOME/Euler_logs/multiRun_${EXPERIMENT_TAG:-manual}.manifest}"



SCRATCH_DIR="/cluster/scratch/$USER/run_${SLURM_JOB_ID}"
mkdir -p "$SCRATCH_DIR/nexus-e-framework/Results"
echo "Scratch dir: $SCRATCH_DIR"
echo "App log path (after start): $SCRATCH_DIR/nexus-e-framework/nexus-e.log"



# Record job start info in manifest immediately
APP_LOG="$SCRATCH_DIR/nexus-e-framework/nexus-e.log"
SLURM_OUT="$HOME/Euler_logs/output_${SLURM_JOB_ID}.log"
SLURM_ERR="$HOME/Euler_logs/error_${SLURM_JOB_ID}.log"

START_ENTRY="[START] Job ${SLURM_JOB_ID} | DB: ${DATABASE} | Playlist: ${PLAYLIST_NAME}
  App:   ${APP_LOG}
  Out:   ${SLURM_OUT}
  Err:   ${SLURM_ERR}
  Monitor: tail -f <path> or cat <path>"

if command -v flock >/dev/null 2>&1; then
  (
    flock -x 200
    printf "%s\n\n" "$START_ENTRY" >&200
  ) 200>>"$MANIFEST_PATH"
else
  printf "%s\n\n" "$START_ENTRY" >>"$MANIFEST_PATH"
fi

echo "Logged job start to manifest with log paths"



cd "$REPO_PATH" || { echo "Repo not found at $REPO_PATH"; exit 1; }



mkdir -p "$SCRATCH_DIR/nexus-e-framework/src/plugins"
cp -r src/nexus_e "$SCRATCH_DIR/nexus-e-framework/src/"
cp -r src/plugins/centiv \
      src/plugins/postprocess \
      src/plugins/update_investments \
      "$SCRATCH_DIR/nexus-e-framework/src/plugins/"
cp -r modules_playlists "$SCRATCH_DIR/nexus-e-framework/"
find . -maxdepth 1 -type f -exec cp {} "$SCRATCH_DIR/nexus-e-framework" \;



cd "$SCRATCH_DIR/nexus-e-framework" || exit 1



# -------------------------------------------------
# Patch config.toml: output_name, simulation_folder, original_name, playlist_name
# -------------------------------------------------
CONFIG="config.toml"
if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: config.toml not found" >&2
  echo "Create one by running: uv run config.toml --setup" >&2
  exit 3
fi



# Patch playlist_name in [modules] section
sed -i -E "/^\[modules\]/,/^\[/ s/^[[:space:]]*playlist_name[[:space:]]*=.*/playlist_name = \"${PLAYLIST_NAME}\"/" "$CONFIG"
# Patch name in [data_context] section
sed -i -E "/^\[data_context\]/,/^\[/ s/^[[:space:]]*name[[:space:]]*=.*/name = \"${DATABASE}\"/" "$CONFIG"



echo "Patched ${CONFIG}:"
echo "  [data_context].name      = ${DATABASE}"
echo "  [modules].playlist_name  = ${PLAYLIST_NAME}"



# -------------------
# Run the application
# -------------------
export PYTHONUNBUFFERED=1
echo "Running model..."
"$REPO_PATH/.venv/bin/nexus-e" --run
APP_EXIT_CODE=$?

echo "App finished with exit code: $APP_EXIT_CODE"

# Check if application logged any errors (even if exit code was 0)
APP_LOG_PATH="$SCRATCH_DIR/nexus-e-framework/nexus-e.log"
echo "Checking for errors in: $APP_LOG_PATH"

if [[ -f "$APP_LOG_PATH" ]]; then
  echo "Log file exists, checking for error patterns..."
  if grep -q "ERROR An error occurred" "$APP_LOG_PATH"; then
    echo "ERROR: Application reported errors in log file"
    
    FAIL_ENTRY="[FAILED] Job ${SLURM_JOB_ID} | DB: ${DATABASE} | Playlist: ${PLAYLIST_NAME}
  Reason: Application error detected in log (exit code was ${APP_EXIT_CODE})
  Check app log: ${APP_LOG_PATH}
  Check error log: $HOME/Euler_logs/error_${SLURM_JOB_ID}.log"
    
    if command -v flock >/dev/null 2>&1; then
      (
        flock -x 200
        printf "%s\n\n" "$FAIL_ENTRY" >&200
      ) 200>>"$MANIFEST_PATH"
    else
      printf "%s\n\n" "$FAIL_ENTRY" >>"$MANIFEST_PATH"
    fi
    
    echo "Recorded application error to manifest"
    exit 1
  else
    echo "No error pattern found in log file"
  fi
else
  echo "WARNING: Log file does not exist yet at $APP_LOG_PATH"
fi

# If app returned non-zero, that will be caught by the ERR trap
if [[ $APP_EXIT_CODE -ne 0 ]]; then
  exit $APP_EXIT_CODE
fi



# -------------------------
# Record run in the manifest
# -------------------------
ABS_RESULTS_PATH="$SCRATCH_DIR/nexus-e-framework/Results/${SIM_FOLDER}"

# Extract webviewer link from log if available
WEBVIEWER_LINK="Not found"
if [[ -f "$APP_LOG_PATH" ]]; then
  # Extract just the https URL from the log
  WEBVIEWER_LINK=$(grep -oP 'https://[^\s]+' "$APP_LOG_PATH" | grep "nexus-e.org/results" | tail -n 1 || echo "Not found")
fi

COMPLETE_ENTRY="[COMPLETE] Job ${SLURM_JOB_ID} | DB: ${DATABASE} | Playlist: ${PLAYLIST_NAME}
  Results: ${ABS_RESULTS_PATH}
  Folder:  ${SIM_FOLDER}
  Webviewer: ${WEBVIEWER_LINK}"



if command -v flock >/dev/null 2>&1; then
  (
    flock -x 200
    printf "%s\n\n" "$COMPLETE_ENTRY" >&200
  ) 200>>"$MANIFEST_PATH"
else
  printf "%s\n\n" "$COMPLETE_ENTRY" >>"$MANIFEST_PATH"
fi



echo "Recorded to manifest: $MANIFEST_PATH"
echo "Finished job=${SLURM_JOB_ID}"
