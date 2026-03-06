## Overview

This guide explains how to run multiple Nexus-E simulations in parallel on the Euler cluster using `euler_run.sbatch`.

**Why use this?** Instead of manually copying files and running simulations one by one, this script automates running multiple database/playlist combinations in parallel, saving significant time and effort.

**Key features:**

- **Runs on scratch storage**: Each job automatically copies only the necessary files to scratch - fast and doesn't consume Home quota
- **Centralized tracking**: All job statuses are collected in a single manifest file, including paths to nexus-e logs, SLURM output/error files, and results
- **Email notifications**: Receive an email when each job completes or fails (optional)
- **Parallel execution**: All database/playlist combinations run independently in parallel

**Files involved:**

| File                                         | Purpose                                                      |
| -------------------------------------------- | ------------------------------------------------------------ |
| `euler_run.sbatch`                           | Main batch script (submit this with `sbatch`)                |
| `modules_playlists/euler_run.config.local`   | Your personal settings: JOBS, REPO_PATH, EMAIL (git-ignored) |
| `modules_playlists/euler_run.config.example` | Template to copy for creating your `.local` file             |
| `config.toml`                                | Main Nexus-E config (database credentials, settings)         |

## Table of Contents

- [Prerequisites (may skip)](#prerequisites)
- [First Time Setup](#first-time-setup)
- [Common Workflows](#common-workflows)
- [How It Works](#how-it-works)
- [Monitoring Jobs](#monitoring-jobs)
- [Understanding the Manifest File](#understanding-the-manifest-file)
- [Troubleshooting](#troubleshooting)
- [Appendix: Understanding create_a_copy Behavior](#appendix-understanding-create_a_copy-behavior)

## Prerequisites

💡 **Already running Nexus-E on Euler?** If you've successfully run Nexus-E on Euler before, you can skip this section and go directly to [First Time Setup](#first-time-setup).
Before using `euler_run.sbatch`, ensure you have:

### 1. Access & Environment

- SSH access to Euler cluster
- Member of the `es_sansa` account (or update `#SBATCH --account` in the script)
- `uv` package manager installed and accessible

### 2. Repository Setup

- Nexus-E framework cloned to Euler (default location: `~/repos/nexus-e-framework`)
  - If you use a different path, you'll need to update `REPO_PATH` in your config file (see Step 2)
  - **Recommendation:** Keep only one repository version on Home to avoid confusion
- Your `config.toml` configured with correct database credentials, user initials, etc.
- Required databases accessible on the database server

### 3. Directory Structure

The script expects this structure:

```
~/repos/nexus-e-framework/    # Your repo (can be different path)
├── euler_run.sbatch           # The batch script
├── config.toml                # Main config (database credentials, user settings)
├── modules_playlists/
│   ├── euler_run.config.local # Your personal config (JOBS, REPO_PATH) - You make this in the Step 1.
│   ├── centiv_2050.toml              # Playlist definitions
│   └── ...
└── ...

~/Euler_logs/                  # Created automatically for logs and manifests
```


## First Time Setup

### Step 1: Create Your Personal Configuration

```bash
cd ~/repos/nexus-e-framework  # Or wherever your repo is
cp modules_playlists/euler_run.config.example modules_playlists/euler_run.config.local
```

### Step 2: Edit Your Configuration

Open `modules_playlists/euler_run.config.local` and configure:

```bash
# Define which database/playlist combinations to run
JOBS=(
  "test_nuclear_all_eu|centiv_2050"
  "test_nuclear_all_eu|suportRES"
  "my_scenario_db|centiv_2050"
)

# Path to your nexus-e-framework repository
REPO_PATH="$HOME/repos/nexus-e-framework"

# Email notifications (optional)
# Set your email to receive notifications when jobs END or FAIL
# Leave empty to disable: EMAIL_ADDRESS=""
EMAIL_ADDRESS="your.email@ethz.ch"
```

**Understanding JOBS:**

- Format: `"database_name|playlist_name"`
- `database_name`: Name of the database in MySQL
- `playlist_name`: Name of the playlist file in `modules_playlists/` (without `.toml` extension)
- Each job runs independently in parallel
- **Note:** The script automatically patches `config.toml` with these values for each job - you don't need to manually edit database or playlist settings in `config.toml`

**Understanding EMAIL_ADDRESS:**

- Optional: Set your email to receive notifications when jobs complete or fail
- Leave empty (`EMAIL_ADDRESS=""`) to disable email notifications
- Each user can set their own email in their personal `euler_run.config.local` file
- You'll receive an email for each job when it ends (success) or fails

### Step 3: Verify config.toml

Check your `config.toml` has correct settings:

- Database server credentials (`[input_database_server]`, `[output_database_server]`)
- User initials (`user_initials`)
- `create_a_copy` setting (determines which database field(s) get patched):
  - `true`: Creates a copy of the database; `original_name` will be set from JOBS
  - `false`: Uses database directly; **both** `original_name` and `copy_name` will be set from JOBS
- Any other settings **except** `original_name`/`copy_name` and `playlist_name` (these are set automatically per job)


## Common Workflows

### Full Calibration Run (Multiple Scenarios in Parallel)

Run multiple scenarios simultaneously:

```bash
cd ~/repos/nexus-e-framework
sbatch euler_run.sbatch
```


## How It Works

### Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. You run: sbatch euler_run.sbatch                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Script reads modules_playlists/euler_run.config.local    │
│    - Loads JOBS array                                       │
│    - Loads REPO_PATH                                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Creates experiment tag (timestamp)                       │
│    - Manifest file: ~/Euler_logs/multiRun_YYYYMMDD_HHMMSS   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Submits one SLURM job per database|playlist pair         │
│    Each job runs independently in parallel                  │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┬───────────────┐
        ▼                         ▼               ▼
   ┌─────────┐             ┌─────────┐       ┌─────────┐
   │ Job 1   │             │ Job 2   │  ...  │ Job N   │
   └────┬────┘             └────┬────┘       └────┬────┘
        │                       │                 │
        └───────────────────────┴─────────────────┘
                            │
                            ▼
        ┌────────────────────────────────────────┐
        │ Each job independently:                │
        │ a. Creates scratch directory           │
        │    /cluster/scratch/$USER/run_$JOBID   │
        │ b. Copies repo to scratch              │
        │ c. Patches config.toml with DB/playlist│
        │ d. Runs: uv run nexus_e/app.py         │
        │ e. Writes results to scratch           │
        │ f. Updates manifest file               │
        └────────────────────────────────────────┘
```

### What Happens in Each Job

1. **Scratch Directory**: Creates `/cluster/scratch/$USER/run_${SLURM_JOB_ID}/`
2. **Copy Files**: Copies from `REPO_PATH`:
   - Folders: `CentIv/`, `nexus_e/`, `Shared/`, `modules_playlists/`
   - All top-level files including `config.toml`
3. **Patch config.toml**: Automatically sets:
   - If `create_a_copy = true`: `[scenario].original_name` = "database_name"
   - If `create_a_copy = false`: **both** `[scenario].original_name` AND `[scenario].copy_name` = "database_name"
   - `[modules].playlist_name = "playlist_name"`
   - `output_name` and `simulation_folder`
4. **Run Simulation**: Executes `uv run nexus_e/app.py`
5. **Store Results**: Saves to `scratch/nexus-e-framework/Results/run_${DATABASE}_${PLAYLIST_NAME}/`
6. **Update Manifest**: Records completion status, results path, and webviewer link


## Monitoring Jobs

### Email Notifications

If you configured `EMAIL_ADDRESS` in your `euler_run.config.local`, you'll receive automatic email notifications:

- **When jobs complete successfully**: Email includes job ID, runtime, and basic stats
- **When jobs fail**: Email includes job ID and failure information

The emails are sent by SLURM automatically. Check your manifest file (path shown in the email) for detailed results locations and webviewer links.

### Check Job Status

```bash
# View your running/pending jobs
squeue -u $USER

# Cancel all your jobs
scancel -u $USER
```

### Monitor Progress in Real-Time

The manifest file is your central dashboard:

```bash
# Find your manifest (created when you submit jobs)
ls -lt ~/Euler_logs/multiRun_*.manifest | head -1

# Watch it update in real-time
watch -n 5 cat ~/Euler_logs/multiRun_20250124_143022.manifest
```

### Check Individual Job Logs

Each job creates three log files, output, error, and nexus-e log. The location of all files can be found in the manifest file.

## Understanding the Manifest File

The manifest file (`~/Euler_logs/multiRun_TIMESTAMP.manifest`) tracks all jobs in your batch.

### Example Manifest

```
# Multi-run manifest for 20250124_143022

[START] Job 12345678 | DB: test_nuclear_all_eu | Playlist: base
  App:   /cluster/scratch/username/run_12345678/nexus-e-framework/nexus-e.log
  Out:   /cluster/home/username/Euler_logs/output_12345678.log
  Err:   /cluster/home/username/Euler_logs/error_12345678.log
  Monitor: tail -f <path> or cat <path>

[COMPLETE] Job 12345678 | DB: test_nuclear_all_eu | Playlist: base
  Results: /cluster/scratch/username/run_12345678/nexus-e-framework/Results/run_test_nuclear_all_eu_base
  Folder:  run_test_nuclear_all_eu_base
  Webviewer: http://example.com/webviewer/...

[START] Job 12345679 | DB: test_nuclear_all_eu | Playlist: suportRES
  App:   /cluster/scratch/username/run_12345679/nexus-e-framework/nexus-e.log
  Out:   /cluster/home/username/Euler_logs/output_12345679.log
  Err:   /cluster/home/username/Euler_logs/error_12345679.log

[FAILED] Job 12345679 | DB: test_nuclear_all_eu | Playlist: suportRES
  Reason: Application error detected in log (exit code was 1)
  Check app log: /cluster/scratch/username/run_12345679/nexus-e-framework/nexus-e.log
  Check error log: /cluster/home/username/Euler_logs/error_12345679.log
```

### Entry Types

| Entry Type   | Meaning                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `[START]`    | Job started running, includes all log paths for monitoring              |
| `[COMPLETE]` | Job finished successfully, includes results location and webviewer link |
| `[FAILED]`   | Job failed, includes exit code and error log path                       |


## Troubleshooting

### Problem: "euler_run.config.local not found"

**Symptom:**

```
WARNING: modules_playlists/euler_run.config.local not found
ERROR: JOBS list is empty.
```

**Solution:**

```bash
# Create your config file
cd ~/repos/nexus-e-framework
cp modules_playlists/euler_run.config.example modules_playlists/euler_run.config.local

# Edit it with your settings
nano modules_playlists/euler_run.config.local
```

---

### Problem: Job Fails Immediately

**Symptom:** Job shows in manifest as `[FAILED]` shortly after submission

**Debug Steps:**

1. Check the error log first:

   ```bash
   cat ~/Euler_logs/error_<JOB_ID>.log
   ```

2. Check the application log:
   ```bash
   cat /cluster/scratch/$USER/run_<JOB_ID>/nexus-e-framework/nexus-e.log
   ```

**Common Causes:**

- **Database connection error**: Check credentials in `config.toml`
- **Database doesn't exist**: Verify database name in `JOBS` array
- **Playlist file missing**: Check `modules_playlists/playlist_name.toml` exists

---

### Problem: "Repo not found at $REPO_PATH"

**Symptom:**

```
Repo not found at /cluster/home/username/repos/nexus-e-framework
```

**Solution:**
Edit `modules_playlists/euler_run.config.local` with the correct path:

```bash
REPO_PATH="/cluster/home/username/actual/path/to/nexus-e-framework"
```


## Additional Resources

- [Euler Cluster Documentation](https://scicomp.ethz.ch/wiki/Euler)
- [SLURM Commands Cheatsheet](https://slurm.schedmd.com/pdfs/summary.pdf)


## Appendix: Understanding create_a_copy Behavior

The `create_a_copy` setting in `config.toml` controls how the script patches database names and how the application uses them:

### When create_a_copy = true

- **Script behavior**: Patches `[scenario].original_name` with the database from JOBS
- **Application behavior**:
  - Reads from `original_name` database
  - Creates a temporary copy with auto-generated name (timestamp + initials + random string)
  - Runs simulation on the copy
  - Postprocessing reads input data from `original_name`
  - Optionally deletes the copy after simulation (if `delete_copy_after_simulation = true`)
- **Use case**: Safe experimentation - leaves the original database untouched

### When create_a_copy = false

- **Script behavior**: Patches **both** `[scenario].original_name` AND `[scenario].copy_name` with the database from JOBS
- **Application behavior**:
  - Uses `copy_name` database directly (no copying)
  - Runs simulation on that database
  - Postprocessing reads input data from `original_name` (same database)
- **Use case**: Working directly with a specific database, faster startup (no copy time)

---

_Originally created by Ali Darudi in discussion with Claude._
