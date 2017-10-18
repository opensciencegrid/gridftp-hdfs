
/**
 * All the "boilerplate" code necessary to make the GridFTP-HDFS integration
 * function.
*/

#include <sys/resource.h>
#include <sys/prctl.h>
#include <syslog.h>
#include <sys/syscall.h>
#include <signal.h>
#include <execinfo.h>
#include <stdio.h>
#include <fcntl.h>

#include "gridftp_hdfs.h"

/*
 *  Globals for this library.
 */
globus_version_t gridftp_hdfs_local_version =
{
    0, /* major version number */
    30, /* minor/bug version number */
    1303175799,
    0 /* branch ID */
};

char err_msg[MSG_SIZE];
int local_io_block_size = 0;
int local_io_count = 0;

// global variable for username, filename and event type
char gridftp_user_name[PATH_MAX];
char gridftp_file_name[PATH_MAX];
char gridftp_transfer_type[10];

static globus_mutex_t g_hdfs_mutex;
static pthread_t g_thread_id = 0;
static int g_thread_pipe_fd = -1;
static int g_thread_saved_stderr_fd = -1;

static globus_result_t check_connection_limits(const hdfs_handle_t *, int, int);
static int dumb_sem_open(const char *fname, int flags, mode_t mode, int value);
static int dumb_sem_timedwait(int fd, int value, int secs);

static void hdfs_trev(globus_gfs_event_info_t *, void *);
inline void set_done(hdfs_handle_t *, globus_result_t);
static int  hdfs_activate(void);
static int  hdfs_deactivate(void);
static void hdfs_command(globus_gfs_operation_t, globus_gfs_command_info_t *, void *);
static void hdfs_start(globus_gfs_operation_t, globus_gfs_session_info_t *);

void
hdfs_destroy(
    void *                              user_arg);


/*
 *  Interface definitions for HDFS
 */
static globus_gfs_storage_iface_t       globus_l_gfs_hdfs_dsi_iface = 
{
    GLOBUS_GFS_DSI_DESCRIPTOR_BLOCKING | GLOBUS_GFS_DSI_DESCRIPTOR_SENDER |
        GLOBUS_GFS_DSI_DESCRIPTOR_REQUIRES_ORDERED_DATA,
    hdfs_start,
    hdfs_destroy,
    NULL, /* list */
    hdfs_send,
    hdfs_recv,
    hdfs_trev, /* trev */
    NULL, /* active */
    NULL, /* passive */
    NULL, /* data destroy */
    hdfs_command, 
    hdfs_stat,
    NULL,
    NULL
};

/*
 *  Module definitions; hooks into the Globus module system.
 *  Initialized when library loads.
 */
GlobusExtensionDefineModule(globus_gridftp_server_hdfs) =
{
    "globus_gridftp_server_hdfs",
    hdfs_activate,
    hdfs_deactivate,
    NULL,
    NULL,
    &gridftp_hdfs_local_version
};

// Custom SEGV handler due to the presence of Java handlers.
void
segv_handler (int sig)
{
  printf ("SEGV triggered in native code.\n");
  const int max_trace = 32;
  void *trace[max_trace];
  char **messages = (char **)NULL;
  int i, trace_size = 0;

  trace_size = backtrace(trace, max_trace);
  messages = backtrace_symbols(trace, trace_size);
  for (i=0; i<trace_size; ++i) {
	printf("[bt] %s\n", messages[i]);
  }
  raise (SIGQUIT);
  signal (SIGSEGV, SIG_DFL);
  raise (SIGSEGV);
}
/*
 *  Check to see if cores can be produced by gridftp; if not, turn them on.
 */
void
gridftp_check_core()
{
    int err;
    struct rlimit rlim;

    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    err = setrlimit(RLIMIT_CORE, &rlim);
    if (err) {
        snprintf(err_msg, MSG_SIZE, "Cannot set rlimits due to %s.\n", strerror(err));
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, err_msg);
    }

    int isDumpable = prctl(PR_GET_DUMPABLE);

    if (!isDumpable) {
        err = prctl(PR_SET_DUMPABLE, 1);
    }
    if (err) {
        snprintf(err_msg, MSG_SIZE, "Cannot set dumpable: %s.\n", strerror(errno));
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, err_msg);
    }

    // Reset signal handler:
    sig_t sigerr = signal (SIGSEGV, segv_handler);
    if (sigerr == SIG_ERR) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Unable to set core handler.\n");
    }
}

/*
 * Simple thread target - continuously drain
 */
static void *
hdfs_forward_log(void *user_arg)
{
    int *pipe_r = (int *)user_arg;
    FILE *fpipe = fdopen(*pipe_r, "r");
    if (!fpipe)
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Unable to reopen forwarding log descriptor at fd %d: (errno=%d, %s)\n", *pipe_r, errno, strerror(errno));
        return NULL;
    }
   globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "Starting HDFS log forwarder; messages from HDFS are prefixed with 'HDFS: '\n");
    char line_buffer[1024];
    unsigned log_count = 0;
    while (fgets(line_buffer, 1024, fpipe))
    {
        if (!strncmp(line_buffer, "\tat ", 4)) {continue;}
        else if ((line_buffer[0] == '\0') || (line_buffer[0] == '\n')) {continue;}
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "HDFS: %s", line_buffer);
        log_count++;
    }
    fclose(fpipe);
    free(user_arg);
    if (log_count)
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Stopping HDFS log forwarder; %lu messages forwarded.\n", log_count);
    }
    globus_mutex_unlock(&g_hdfs_mutex);
    return NULL;
}

/*
 * Open stderr as a pipe which is continuously drained
 * via a separate thread (forwarding to the globus logging system).
 */
static void
setup_hdfs_logging()
{
    if (globus_mutex_trylock(&g_hdfs_mutex))
    {
        // The logging thread has already been initialized.
        return;
    }

    char fd2_path[PATH_MAX];
    ssize_t bytes_in_path;
    if ((-1 == (bytes_in_path = readlink("/dev/fd/2", fd2_path, PATH_MAX-1))) && (errno != ENOENT) && (errno != EACCES))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Unable to check /dev/fd/2 as eUID %d (UID %d) to see if it is /dev/null. (errno=%d, %s)\n", geteuid(), getuid(), errno, strerror(errno));
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }
    if (bytes_in_path >= 0) {fd2_path[bytes_in_path] = '\0';}
    if ((bytes_in_path != -1) && strcmp("/dev/null", fd2_path))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "stderr does not point to /dev/null; not redirecting HDFS output.\n");
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }

    int err;
    pthread_attr_t attr;
    if ((err = pthread_attr_init(&attr)))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Unable to initialize pthread attribute: (errno=%d, %s).\n", err, strerror(err));
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }

    int pipe_fds[2];
    if (-1 == pipe2(pipe_fds, O_CLOEXEC))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Failed to open pipes for HDFS logging: (errno=%d, %s).\n", errno, strerror(errno));
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }

    if ((g_thread_saved_stderr_fd = dup(STDERR_FILENO)) == -1)
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Failed to save stderr for HDFS logging: (errno=%d, %s).\n", errno, strerror(errno));
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }

    if (-1 == dup3(pipe_fds[1], STDERR_FILENO, O_CLOEXEC))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Failed to reopen stderr for HDFS logging: (errno=%d, %s).\n", errno, strerror(errno));
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }
    close(pipe_fds[1]);
    g_thread_pipe_fd = STDERR_FILENO;

    int *pipe_ptr = malloc(sizeof(int));
    if (pipe_ptr == NULL)
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Failed to allocate pointer for pipe.\n");
        globus_mutex_unlock(&g_hdfs_mutex);
        return;
    }
    *pipe_ptr = pipe_fds[0];
    if ((err = pthread_create(&g_thread_id, &attr, hdfs_forward_log, pipe_ptr)))
    {
        globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Failed to launch thread for monitoring HDFS logging: (errno=%d, %s).\n", errno, strerror(errno));
        globus_mutex_unlock(&g_hdfs_mutex);
        free(pipe_ptr);
        return;
    }

    
}

/*
 *  Called when the HDFS module is activated.
 *  Initializes the global mutex.
 */
int
hdfs_activate(void)
{
    globus_extension_registry_add(
        GLOBUS_GFS_DSI_REGISTRY,
        "hdfs",
        GlobusExtensionMyModule(globus_gridftp_server_hdfs),
        &globus_l_gfs_hdfs_dsi_iface);
    
    return 0;
}

/*
 *  Called when the HDFS module is deactivated.
 *  Completely boilerplate
 */
int
hdfs_deactivate(void)
{
    globus_extension_registry_remove(
        GLOBUS_GFS_DSI_REGISTRY, "hdfs");

    return 0;
}

static
void
hdfs_trev(
    globus_gfs_event_info_t *           event_info,
    void *                              user_arg
)
{

    hdfs_handle_t *       hdfs_handle;
    GlobusGFSName(globus_l_gfs_hdfs_trev);

    hdfs_handle = (globus_l_gfs_hdfs_handle_t *) user_arg;
    globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Recieved a transfer event.\n");

    switch (event_info->type) {
        case GLOBUS_GFS_EVENT_TRANSFER_ABORT:
            globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Got an abort request to the HDFS client.\n");
            set_done(hdfs_handle, GLOBUS_FAILURE);
            break;
        default:
            globus_gfs_log_message(GLOBUS_GFS_LOG_ERR, "Got some other transfer event %d.\n", event_info->type);
    }
}

/*************************************************************************
 *  command
 *  -------
 *  This interface function is called when the client sends a 'command'.
 *  commands are such things as mkdir, remdir, delete.  The complete
 *  enumeration is below.
 *
 *  To determine which command is being requested look at:
 *      cmd_info->command
 *
 *      GLOBUS_GFS_CMD_MKD = 1,
 *      GLOBUS_GFS_CMD_RMD,
 *      GLOBUS_GFS_CMD_DELE,
 *      GLOBUS_GFS_CMD_RNTO,
 *      GLOBUS_GFS_CMD_RNFR,
 *      GLOBUS_GFS_CMD_CKSM,
 *      GLOBUS_GFS_CMD_SITE_CHMOD,
 *      GLOBUS_GFS_CMD_SITE_DSI
 ************************************************************************/
static void
hdfs_command(
    globus_gfs_operation_t              op,
    globus_gfs_command_info_t *         cmd_info,
    void *                              user_arg)
{
    globus_result_t                    result = GLOBUS_FAILURE;
    globus_l_gfs_hdfs_handle_t *       hdfs_handle;
    char *                             PathName;
    GlobusGFSName(hdfs_command);

    char * return_value = GLOBUS_NULL;

    hdfs_handle = (globus_l_gfs_hdfs_handle_t *) user_arg;
    
    // Get hadoop path name (ie subtract mount point)
    PathName=cmd_info->pathname;
    while (PathName[0] == '/' && PathName[1] == '/')
    {
        PathName++;
    }
    if (strncmp(PathName, hdfs_handle->mount_point, hdfs_handle->mount_point_len)==0) {
        PathName += hdfs_handle->mount_point_len;
    }
    while (PathName[0] == '/' && PathName[1] == '/')
    {
        PathName++;
    }

    errno = ENOSYS;
    hdfs_handle->pathname = PathName;
    SystemError(hdfs_handle, "command", result);
    switch (cmd_info->command) {
    case GLOBUS_GFS_CMD_MKD:
{
        errno = 0;
        if (hdfsCreateDirectory(hdfs_handle->fs, PathName) == -1) {
            if (errno) {
                SystemError(hdfs_handle, "mkdir", result);
            } else {
                GenericError(hdfs_handle, "Unable to create directory (reason unknown)", result);
            }
        } else {
            result = GLOBUS_SUCCESS;
        }
}
        break;
    case GLOBUS_GFS_CMD_RMD:
{
        int numEntries = -1;
        errno = 0;
        hdfsFileInfo *info = hdfsListDirectory(hdfs_handle->fs, PathName, &numEntries);
        if (info) {
            hdfsFreeFileInfo(info, numEntries);
            info = NULL;
            errno = 0;
        }
        if (numEntries > 0) { // NOTE: above call sets info to NULL in case of empty directory.
            errno = ENOTEMPTY;
            SystemError(hdfs_handle, "rmdir", result);
            break;
        }
        if ((numEntries < 0) && (errno != ENOENT)) {
            if (errno) {
                SystemError(hdfs_handle, "rmdir", result);
            } else { // Logic error in libhdfs: didn't set numEntries or errno.
                GenericError(hdfs_handle, "Unable to delete directory (reason unknown)", result);
            }
            break;
        }
}
        // Only case remaining is empty directory.  OK to delete; not done - fall through
    case GLOBUS_GFS_CMD_DELE:
{
        errno = 0;
        if (hdfsDelete(hdfs_handle->fs, PathName, 0) == -1) {
            if (errno) {
                SystemError(hdfs_handle, "unlink", result);
            } else {
                GenericError(hdfs_handle, "Unable to delete file (reason unknown)", result);
            }
        } else {
            result = GLOBUS_SUCCESS;
        }
}
        break;
    case GLOBUS_GFS_CMD_RNTO:
{
        const char * FromPathName=cmd_info->from_pathname;
        while (FromPathName[0] == '/' && FromPathName[1] == '/')
        {
            FromPathName++;
        }
        if (strncmp(FromPathName, hdfs_handle->mount_point, hdfs_handle->mount_point_len)==0) {
            FromPathName += hdfs_handle->mount_point_len;
        }
        while (FromPathName[0] == '/' && FromPathName[1] == '/')
        {
            FromPathName++;
        }

        errno = 0;
        if (hdfsRename(hdfs_handle->fs, FromPathName, PathName)) {
            if (errno) {
                int failed = 1;
                if (errno == EIO) {
                    hdfsDelete(hdfs_handle->fs, PathName, 0);
                    failed = hdfsRename(hdfs_handle->fs, FromPathName, PathName);
                    result = GLOBUS_SUCCESS;
                }
                if (failed) {
                    char * rename_msg = (char *)globus_malloc(1024);
                    snprintf(rename_msg, 1024, "rename from %s", FromPathName);
                    rename_msg[1023] = '\0';
                    SystemError(hdfs_handle, rename_msg, result);
                    globus_free(rename_msg);
                }
            } else {
                GenericError(hdfs_handle, "Unable to rename file (reason unknown)", result);
            }
        } else {
            result = GLOBUS_SUCCESS;
        }
}
        break;
    case GLOBUS_GFS_CMD_RNFR:
        break;
    case GLOBUS_GFS_CMD_CKSM:
{
        if ((cmd_info->cksm_offset != 0) || (cmd_info->cksm_length != -1)) {
            GenericError(hdfs_handle, "Unable to retrieve partial checksums", result);
            break;
        }
        char * value = NULL;
        if ((result = hdfs_get_checksum(hdfs_handle, PathName, cmd_info->cksm_alg, &value)) != GLOBUS_SUCCESS) {
            break;
        }
        if (value == NULL) {
            GenericError(hdfs_handle, "Unable to retrieve check", result);
            break;
        }
        return_value = value;
}
        break;
    case GLOBUS_GFS_CMD_SITE_CHMOD:
        break;
    case GLOBUS_GFS_CMD_SITE_DSI:
        break;
    case GLOBUS_GFS_CMD_SITE_RDEL:
        break;
    case GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT:
        break;
    case GLOBUS_GFS_CMD_SITE_SETNETSTACK:
        break;
    case GLOBUS_GFS_CMD_SITE_SETDISKSTACK:
        break;
    case GLOBUS_GFS_CMD_SITE_CLIENTINFO:
        break;
    case GLOBUS_GFS_CMD_SITE_CHGRP:
        break;
    case GLOBUS_GFS_CMD_SITE_UTIME:
        break;
    case GLOBUS_GFS_CMD_SITE_SYMLINKFROM:
        break;
    case GLOBUS_GFS_CMD_SITE_SYMLINK:
        break;
    case GLOBUS_GFS_CMD_DCSC:
        break;
    case GLOBUS_GFS_CMD_HTTP_PUT:
    case GLOBUS_GFS_CMD_HTTP_GET:
    case GLOBUS_GFS_CMD_HTTP_CONFIG:
    case GLOBUS_GFS_CMD_TRNC:
    case GLOBUS_GFS_CMD_SITE_TASKID:
    case GLOBUS_GFS_CMD_SITE_RESTRICT:
    case GLOBUS_GFS_CMD_SITE_CHROOT:
    case GLOBUS_GFS_CMD_SITE_SHARING:
    case GLOBUS_GFS_CMD_UPAS:
    case GLOBUS_GFS_CMD_UPRT:
    case GLOBUS_GFS_MIN_CUSTOM_CMD:
        break;
    }

    globus_gridftp_server_finished_command(op, result, return_value);

    if (return_value) {
        free(return_value);
    }
}

/*************************************************************************
 *  start
 *  -----
 *  This function is called when a new session is initialized, ie a user 
 *  connectes to the server.  This hook gives the dsi an oppertunity to
 *  set internal state that will be threaded through to all other
 *  function calls associated with this session. int                                 port;
    char *                              host;
    int                                 replicas; And an oppertunity to
 *  reject the user.
 *
 *  finished_info.info.session.session_arg should be set to an DSI
 *  defined data structure.  This pointer will be passed as the void *
 *  user_arg parameter to all other interface functions.
 * 
 *  NOTE: at nice wrapper function should exist that hides the details 
 *        of the finished_info structure, but it currently does not.  
 *        The DSI developer should jsut follow this template for now
 ************************************************************************/
void
hdfs_start(
    globus_gfs_operation_t              op,
    globus_gfs_session_info_t *         session_info)
{
    hdfs_handle_t*       hdfs_handle = NULL;
    globus_gfs_finished_info_t          finished_info;
    GlobusGFSName(hdfs_start);
    globus_result_t rc;

    int max_buffer_count = 200;
    int max_file_buffer_count = 1500;
    int load_limit = 20;
    int replicas;
    int port;
    int user_transfer_limit = -1;
    int transfer_limit = -1;

    if (globus_mutex_init(&g_hdfs_mutex, GLOBUS_NULL)) {
        SystemError(hdfs_handle, "Unable to initialize mutex", rc);
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    hdfs_handle = (hdfs_handle_t *)globus_malloc(sizeof(hdfs_handle_t));
    memset(hdfs_handle, 0, sizeof(hdfs_handle_t));

    memset(&finished_info, 0, sizeof(globus_gfs_finished_info_t));
    finished_info.type = GLOBUS_GFS_OP_SESSION_START;
    finished_info.result = GLOBUS_SUCCESS;
    finished_info.info.session.session_arg = hdfs_handle;
    finished_info.info.session.username = session_info->username;
    finished_info.info.session.home_dir = "/";

    if (!hdfs_handle) {
        MemoryError(hdfs_handle, "Unable to allocate a new HDFS handle.", rc);
        finished_info.result = rc;
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    hdfs_handle->mutex = (globus_mutex_t *)malloc(sizeof(globus_mutex_t));
    if (!(hdfs_handle->mutex)) {
        MemoryError(hdfs_handle, "Unable to allocate a new mutex for HDFS.", rc);
        finished_info.result = rc;
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    if (globus_mutex_init(hdfs_handle->mutex, GLOBUS_NULL)) {
        SystemError(hdfs_handle, "Unable to initialize mutex", rc);
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    hdfs_handle->io_block_size = 0;
    hdfs_handle->io_count = 0;

    // Copy the username from the session_info to the HDFS handle.
    size_t strlength = strlen(session_info->username)+1;
    strlength = strlength < 256 ? strlength  : 256;
    hdfs_handle->username = globus_malloc(sizeof(char)*strlength);
    if (hdfs_handle->username == NULL) {
        gridftp_user_name[0] = '\0';
        finished_info.result = GLOBUS_FAILURE;
        globus_gridftp_server_operation_finished(
            op, GLOBUS_FAILURE, &finished_info);
        return;
    }
    strncpy(hdfs_handle->username, session_info->username, strlength);
    // also copy username to global variable gridftp_user_name
    strncpy(gridftp_user_name, session_info->username, strlength);

    // Pull configuration from environment.
    hdfs_handle->replicas = 3;
    hdfs_handle->host = "hadoop-name";
    hdfs_handle->mount_point = "/mnt/hadoop";
    hdfs_handle->port = 9000;
    char * replicas_char = getenv("GRIDFTP_HDFS_REPLICAS");
    char * namenode = getenv("GRIDFTP_HDFS_NAMENODE");
    char * port_char = getenv("GRIDFTP_HDFS_PORT");
    char * mount_point_char = getenv("GRIDFTP_HDFS_MOUNT_POINT");
    char * load_limit_char = getenv("GRIDFTP_LOAD_LIMIT");
    char * global_transfer_limit_char = getenv("GRIDFTP_TRANSFER_LIMIT");
    char * default_user_limit_char = getenv("GRIDFTP_DEFAULT_USER_TRANSFER_LIMIT");

    char specific_limit_env_var[256];
    snprintf(specific_limit_env_var, 255, "GRIDFTP_%s_USER_TRANSFER_LIMIT", hdfs_handle->username);
    specific_limit_env_var[255] = '\0';
    int idx;
    for (idx=0; idx<256; idx++) {
        if (specific_limit_env_var[idx] == '\0') {break;}
        specific_limit_env_var[idx] = toupper(specific_limit_env_var[idx]);
    }
    char * specific_user_limit_char = getenv(specific_limit_env_var);

    if (!specific_user_limit_char) {
        specific_user_limit_char = default_user_limit_char;
    }
    if (specific_user_limit_char) {
        user_transfer_limit = atoi(specific_user_limit_char);
    }
    if (global_transfer_limit_char) {
        transfer_limit = atoi(global_transfer_limit_char);
    }

    if (load_limit_char != NULL) {
        load_limit = atoi(load_limit_char);
        if (load_limit < 1)
            load_limit = 20;
    }

    // Get our hostname
    hdfs_handle->local_host = globus_malloc(256);
    if (hdfs_handle->local_host) {
        memset(hdfs_handle->local_host, 0, 256);
        if (gethostname(hdfs_handle->local_host, 255)) {
            strcpy(hdfs_handle->local_host, "UNKNOWN");
        }
    }

    // Pull syslog configuration from environment.
    char * syslog_host_char = getenv("GRIDFTP_SYSLOG");
    if (syslog_host_char == NULL) {
        hdfs_handle->syslog_host = NULL;
    } else {
        hdfs_handle->syslog_host = syslog_host_char; 
        hdfs_handle->remote_host = session_info->host_id;
        openlog("GRIDFTP", 0, LOG_LOCAL2);
        hdfs_handle->syslog_msg = (char *)globus_malloc(256);
        if (hdfs_handle->syslog_msg)
            snprintf(hdfs_handle->syslog_msg, 255, "%s %s %%s %%i %%i", hdfs_handle->local_host, hdfs_handle->remote_host);
    }

    // Forward the contents of stderr to the globus logging system.
    setup_hdfs_logging();

    // Determine the maximum number of buffers; default to 200.
    char * max_buffer_char = getenv("GRIDFTP_BUFFER_COUNT");
    if (max_buffer_char != NULL) {
        max_buffer_count = atoi(max_buffer_char);
        if ((max_buffer_count < 5)  || (max_buffer_count > 1000))
            max_buffer_count = 200;
    }
    hdfs_handle->max_buffer_count = max_buffer_count;
    snprintf(err_msg, MSG_SIZE, "Max memory buffer count: %i.\n", hdfs_handle->max_buffer_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,err_msg);

    char * max_file_buffer_char = getenv("GRIDFTP_FILE_BUFFER_COUNT");
    if (max_file_buffer_char != NULL) {
        max_file_buffer_count = atoi(max_file_buffer_char);
        if ((max_file_buffer_count < max_buffer_count)  || (max_buffer_count > 50000))
            max_file_buffer_count = 3*max_buffer_count;
    }
    hdfs_handle->max_file_buffer_count = max_file_buffer_count;
    snprintf(err_msg, MSG_SIZE, "Max file buffer count: %i.\n", hdfs_handle->max_file_buffer_count);
    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,err_msg);

    if (load_limit_char != NULL) {
        load_limit = atoi(load_limit_char);
        if (load_limit < 1)
            load_limit = 20;
    }

    if (mount_point_char != NULL) {
        hdfs_handle->mount_point = mount_point_char;
    }
    hdfs_handle->mount_point_len = strlen(hdfs_handle->mount_point);

    if (replicas_char != NULL) {
        replicas = atoi(replicas_char);
        if ((replicas > 1) && (replicas < 20))
            hdfs_handle->replicas = replicas;
    }
    if (namenode != NULL)
        hdfs_handle->host = namenode;
    if (port_char != NULL) {
        port = atoi(port_char);
        if ((port >= 1) && (port <= 65535))
            hdfs_handle->port = port;
    }

    hdfs_handle->using_file_buffer = 0;

    hdfs_handle->cksm_root = "/cksums";

    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Checking current load on the server.\n");
    // Stall stall stall!
    int fd = open("/proc/loadavg", O_RDONLY);
    int bufsize = 256, nbytes=-1;
    char buf[bufsize];
    char * buf_ptr;
    char * token;
    double load;
    int ctr = 0;
    while (fd >= 0) {
        if (ctr == 10)
            break;
        ctr += 1;
        nbytes = read(fd, buf, bufsize);
        if (nbytes < 0)
            break;
        buf[nbytes-1] = '\0';
        buf_ptr = buf;
        token = strsep(&buf_ptr, " ");
        load = strtod(token, NULL);
        globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "Detected system load %.2f.\n", load);
        if ((load >= load_limit) && (load < 1000)) {
            globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "Preventing gridftp transfer startup due to system load of %.2f.\n", load);
            sleep(5*ctr);
        } else {
            break;
        }
        close(fd);
        fd = open("/proc/loadavg", O_RDONLY);
    }
    if (load > load_limit) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Failing transfer due to load %.2f over limit %d\n", load, load_limit);
        GenericError(hdfs_handle, "Server is cancelling transfer due to over-load limit", rc);
        finished_info.result = rc;
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    if ((rc = check_connection_limits(hdfs_handle, user_transfer_limit, transfer_limit)) != GLOBUS_SUCCESS) {
        finished_info.result = rc;
        globus_gridftp_server_operation_finished(op, rc, &finished_info);
        return;
    }

    globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
        "Start gridftp server; hadoop nameserver %s, port %i, replicas %i.\n",
        hdfs_handle->host, hdfs_handle->port, hdfs_handle->replicas);

    hdfs_handle->fs = hdfsConnect("default", 0);
    if (!hdfs_handle->fs) {
        finished_info.result = GLOBUS_FAILURE;
        globus_gridftp_server_operation_finished(
            op, GLOBUS_FAILURE, &finished_info);
        return;
    }

    // Parse the checksum request information
    const char * checksums_char = getenv("GRIDFTP_HDFS_CHECKSUMS");
    if (checksums_char) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum algorithms in use: %s.\n", checksums_char);
        hdfs_parse_checksum_types(hdfs_handle, checksums_char);
    } else {
        hdfs_handle->cksm_types = 0;
    }

    hdfs_handle->tmp_file_pattern = (char *)NULL;

    // Handle core limits
    gridftp_check_core();

    globus_gridftp_server_operation_finished(
        op, GLOBUS_SUCCESS, &finished_info);
}

/************************************************************************ 
 *  destroy
 *  -------
 *  This is called when a session ends, ie client quits or disconnects.
 ************************************************************************/
void
hdfs_destroy(
    void *                              user_arg)
{
    hdfs_handle_t *       hdfs_handle;
    hdfs_handle = (globus_l_gfs_hdfs_handle_t *) user_arg;

    if (hdfs_handle) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Destroying the HDFS connection.\n");
        if (hdfs_handle->fs) {
            hdfsDisconnect(hdfs_handle->fs);
            hdfs_handle->fs = NULL;
        }
        if (hdfs_handle->username)
            globus_free(hdfs_handle->username);
        if (hdfs_handle->local_host)
            globus_free(hdfs_handle->local_host);
        if (hdfs_handle->syslog_msg)
            globus_free(hdfs_handle->syslog_msg);
        remove_file_buffer(hdfs_handle);
        if (hdfs_handle->mutex) {
            globus_mutex_destroy(hdfs_handle->mutex);
            globus_free(hdfs_handle->mutex);
        }
        if (hdfs_handle->cvmfs_graft)
            free(hdfs_handle->cvmfs_graft);
        globus_free(hdfs_handle);
    }

    if (g_thread_id > 0)
    {
        if (g_thread_saved_stderr_fd >= 0)
        {
            dup2(g_thread_saved_stderr_fd, STDERR_FILENO);
            close(g_thread_saved_stderr_fd);
        }

        if (g_thread_pipe_fd >= 0)
        {
            close(g_thread_pipe_fd);
        }

        void *retval;
        pthread_join(g_thread_id, &retval);
    }

    g_thread_id = 0;
    g_thread_pipe_fd = -1;
    g_thread_saved_stderr_fd = -1;

    closelog();
}

/*************************************************************************
 *  is_done
 *  -------
 *  Check to see if a hdfs_handle is already done.
 ************************************************************************/
inline globus_bool_t
is_done(
    hdfs_handle_t *hdfs_handle)
{
    return hdfs_handle->done > 0;
}

/*************************************************************************
 *  is_close_done
 *  -------------
 *  Check to see if a hdfs_handle is already close-done.
 ************************************************************************/
inline globus_bool_t
is_close_done(
    hdfs_handle_t *hdfs_handle)
{
    return hdfs_handle->done == 2;
}

/*************************************************************************
 *  set_done
 *  --------
 *  Set the handle as done for a given reason.
 *  If the handle is already done with an error, this is a no-op.
 *  If the handle is in a success state and gets a failure, we record it.
 ************************************************************************/
inline void
set_done(
    hdfs_handle_t *hdfs_handle, globus_result_t rc)
{
    // Ignore already-done handles.
    if (is_done(hdfs_handle) && (hdfs_handle->done_status != GLOBUS_SUCCESS)) {
        return;
    }
    hdfs_handle->done = 1;
    hdfs_handle->done_status = rc;
}

/*************************************************************************
 *  set_close_done
 *  --------------
 *  Set the handle as close-done for a given reason.
 *  If the handle is already close-done, this is a no-op.
 *  If the handle was done successfully, but the close was not a success,
 *  then record it.
 ************************************************************************/
inline void
set_close_done(
    hdfs_handle_t *hdfs_handle, globus_result_t rc)
{
    // Ignore already-done handles.
    if (is_close_done(hdfs_handle)) {
        return;
    }
    hdfs_handle->done = 2;
    if ((hdfs_handle->done_status == GLOBUS_SUCCESS) && (rc != GLOBUS_SUCCESS)) {
        hdfs_handle->done_status = rc;
    }
}

/*************************************************************************
 * check_connection_limits
 * -----------------------
 * Make sure the number of concurrent connections to HDFS is below a certain
 * threshold.  If we are over-threshold, wait for a fixed amount of time (1 
 * minute) and fail the transfer.
 * Implementation baed on named POSIX semaphores.
 *************************************************************************/
globus_result_t
check_connection_limits(const hdfs_handle_t *hdfs_handle, int user_transfer_limit, int transfer_limit)
{
    GlobusGFSName(check_connection_limit);
    globus_result_t result = GLOBUS_SUCCESS;

    int user_lock_count = 0;
    if (user_transfer_limit > 0) {
        char user_sem_name[256];
        snprintf(user_sem_name, 255, "/dev/shm/gridftp-hdfs-%s-%d", hdfs_handle->username, user_transfer_limit);
        user_sem_name[255] = '\0';
        int usem = dumb_sem_open(user_sem_name, O_CREAT, 0600, user_transfer_limit);
        if (usem == -1) {
            SystemError(hdfs_handle, "Failure when determining user connection limit", result);
            return result;
        }
        if (-1 == (user_lock_count = dumb_sem_timedwait(usem, user_transfer_limit, 60))) {
            if (errno == ETIMEDOUT) {
                globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Failing transfer for %s due to user connection limit of %d.\n", hdfs_handle->username, user_transfer_limit);
                char * failure_msg = (char *)globus_malloc(1024);
                snprintf(failure_msg, 1024, "Server over the user connection limit of %d", user_transfer_limit);
                failure_msg[1023] = '\0';
                GenericError(hdfs_handle, failure_msg, result);
                globus_free(failure_msg);
            } else {
                SystemError(hdfs_handle, "Failed to check user connection semaphore", result);
            }
            return result;
        }
        // NOTE: We now purposely leak the semaphore.  It will be automatically closed when
        // the server process finishes this connection.
    }

    int global_lock_count = 0;
    if (transfer_limit > 0) {
        char global_sem_name[256];
        snprintf(global_sem_name, 255, "/dev/shm//gridftp-hdfs-overall-%d", transfer_limit);
        global_sem_name[255] = '\0';
        int gsem = dumb_sem_open(global_sem_name, O_CREAT, 0666, transfer_limit);
        if (gsem == -1) {
            SystemError(hdfs_handle, "Failure when determining global connection limit", result);
            return result;
        }
        if (-1 == (global_lock_count=dumb_sem_timedwait(gsem, transfer_limit, 60))) {
            if (errno == ETIMEDOUT) {
                globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Failing transfer for %s due to global connection limit of %d (user has %d transfers).\n", hdfs_handle->username, transfer_limit, user_lock_count);
                char * failure_msg = (char *)globus_malloc(1024);
                snprintf(failure_msg, 1024, "Server over the global connection limit of %d (user has %d transfers)", transfer_limit, user_lock_count);
                failure_msg[1023] = '\0';
                GenericError(hdfs_handle, failure_msg, result);
                globus_free(failure_msg);
            } else {
                SystemError(hdfs_handle, "Failed to check global connection semaphore", result);
            }
            return result;
        }
        // NOTE: We now purposely leak the semaphore.  It will be automatically closed when
        // the server process finishes this connection.
    }
    if ((transfer_limit > 0) || (user_transfer_limit > 0)) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Proceeding with transfer; user %s has %d active transfers (limit %d); server has %d active transfers (limit %d).\n", hdfs_handle->username, user_lock_count, user_transfer_limit, global_lock_count, transfer_limit);
    }

    return result;
}

int
dumb_sem_open(const char *fname, int flags, mode_t mode, int value) {
    int fd = open(fname, flags | O_RDWR, mode);
    if (-1 == fd) {
        return fd;
    }
    if (-1 == posix_fallocate(fd, 0, value)) {
        return -1;
    }
    fchmod(fd, mode);
    return fd;
}

int
dumb_sem_timedwait(int fd, int value, int secs) {
    struct timespec start, now, sleeptime;
    clock_gettime(CLOCK_MONOTONIC, &start);
    sleeptime.tv_sec = 0;
    sleeptime.tv_nsec = 500*1e6;
    while (1) {
        int idx = 0;
        int lock_count = 0;
        int need_lock = 1;
        for (idx=0; idx<value; idx++) {
            struct flock mylock; memset(&mylock, '\0', sizeof(mylock));
            mylock.l_type = F_WRLCK;
            mylock.l_whence = SEEK_SET;
            mylock.l_start = idx;
            mylock.l_len = 1;
            if (0 == fcntl(fd, need_lock ? F_SETLK : F_GETLK, &mylock)) {
                if (need_lock) {  // We now have the lock.
                    need_lock = 0;
                    lock_count++;
                } else if (mylock.l_type != F_UNLCK) {  // We're just seeing how many locks are taken.
                    lock_count++;
                }
                continue;
            }
            if (errno == EAGAIN || errno == EACCES || errno == EINTR) {
                lock_count++;
                continue;
            }
            return -1;
        }
        if (!need_lock) {  // we were able to take a lock.
            return lock_count;
        }
        nanosleep(&sleeptime, NULL);
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec > start.tv_sec + secs) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
}

