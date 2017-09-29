
#include "gridftp_hdfs.h"

#include <openssl/md5.h>
#include <zlib.h>
#include <stdint.h>

#include <hdfs.h>

// NOTE: Internally, CVMFS defines anything 25MB or over to be
// a "big chunk", which triggers very aggressive cache cleanup
// (sometimes, 50% of the total space).  Hence, we resize down
// to 24MB.  This aligns reasonably well with the default
// GridFTP-HDFS block size (1MB).
#define CVMFS_CHUNK_SIZE (24*1024*1024)

#define OUTPUT_BUFFER_STARTING_SIZE (4*1024)

// CRC table taken from POSIX description of algorithm.
static uint32_t const crctab[256] =
{
  0x00000000,
  0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc, 0x17c56b6b,
  0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f, 0x2f8ad6d6,
  0x2b4bcb61, 0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
  0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9, 0x5f15adac,
  0x5bd4b01b, 0x569796c2, 0x52568b75, 0x6a1936c8, 0x6ed82b7f,
  0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3, 0x709f7b7a,
  0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
  0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5, 0xbe2b5b58,
  0xbaea46ef, 0xb7a96036, 0xb3687d81, 0xad2f2d84, 0xa9ee3033,
  0xa4ad16ea, 0xa06c0b5d, 0xd4326d90, 0xd0f37027, 0xddb056fe,
  0xd9714b49, 0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
  0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1, 0xe13ef6f4,
  0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077, 0x30476dc0,
  0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c, 0x2e003dc5,
  0x2ac12072, 0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16,
  0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca, 0x7897ab07,
  0x7c56b6b0, 0x71159069, 0x75d48dde, 0x6b93dddb, 0x6f52c06c,
  0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1,
  0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
  0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e, 0xbfa1b04b,
  0xbb60adfc, 0xb6238b25, 0xb2e29692, 0x8aad2b2f, 0x8e6c3698,
  0x832f1041, 0x87ee0df6, 0x99a95df3, 0x9d684044, 0x902b669d,
  0x94ea7b2a, 0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e,
  0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2, 0xc6bcf05f,
  0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683, 0xd1799b34,
  0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59, 0x608edb80,
  0x644fc637, 0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
  0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f, 0x5c007b8a,
  0x58c1663d, 0x558240e4, 0x51435d53, 0x251d3b9e, 0x21dc2629,
  0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5, 0x3f9b762c,
  0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
  0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623, 0xf12f560e,
  0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2, 0xe6ea3d65,
  0xeba91bbc, 0xef68060b, 0xd727bbb6, 0xd3e6a601, 0xdea580d8,
  0xda649d6f, 0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
  0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7, 0xae3afba2,
  0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6, 0x9ff77d71,
  0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad, 0x81b02d74,
  0x857130c3, 0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640,
  0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c, 0x7b827d21,
  0x7f436096, 0x7200464f, 0x76c15bf8, 0x68860bfd, 0x6c47164a,
  0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e, 0x18197087,
  0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
  0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088, 0x2497d08d,
  0x2056cd3a, 0x2d15ebe3, 0x29d4f654, 0xc5a92679, 0xc1683bce,
  0xcc2b1d17, 0xc8ea00a0, 0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb,
  0xdbee767c, 0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18,
  0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4, 0x89b8fd09,
  0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5, 0x9e7d9662,
  0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06, 0xa6322bdf,
  0xa2f33668, 0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4
};

/*
 * Helper function to concatenate formatted strings onto a buffer.
 *
 * - `buffer` is the current base of the memory buffer as returned by malloc;
 *   this memory may be realloc'd to grow the total string space.
 * - `offset` is the offset into the buffer where writing should occur.
 * - `length` is the total length of the memory buffer.
 *
 * If buffer, offset, or length are NULL, it is considered an error.
 *
 * On error, buffer is free'd and NULL is returned.
 *
 * On success, the current buffer location is returned and offset / length are updated
 * appropriately.
 */
static
char *concatenate(char *buffer, globus_off_t *offset, globus_size_t *length, const char *format, ...) {
    if (buffer == NULL) {
        return NULL;
    }
    if (length == 0) {
        return buffer;
    }

    globus_size_t remaining_length = *length - *offset;
    char *cur = buffer + *offset;

    va_list args;
    va_start(args, format);
    int rc = vsnprintf(cur, remaining_length, format, args);
    va_end(args);
    if (rc < 0) {
        free(buffer);
        return NULL;
    } else if (rc >= remaining_length) {
        // Resize the buffer; add in an extra KB to avoid having to resize often.
        *length = *length - remaining_length + rc + 1 + 1024;
        remaining_length = *length - *offset;
        cur = realloc(buffer, *length);
        if (cur == NULL) {
            free(buffer);
            return NULL;
        }
        buffer = cur;
        cur = buffer + *offset;
        va_list args2;
        va_start(args2, format);
        rc = vsnprintf(cur, remaining_length, format, args2);
        va_end(args2);
        assert((rc >= 0) && (rc < remaining_length));
    }
    *offset += rc;
    return buffer;
}

/*
 * Taken from globus_gridftp_server_file.c
 * Assume md5_human is length digest_length*2+1
 * Assume md5_openssl is length digest_length
 */
static void human_readable_evp(unsigned char *md5_human, const unsigned char *md5_openssl, int digest_length) {
    unsigned int i;
    unsigned char * md5ptr = md5_human;
    for (i = 0; i < digest_length; i++) {
        sprintf(md5ptr, "%02x", md5_openssl[i]);
        md5ptr++;
        md5ptr++;
    }
    md5ptr = '\0';
}

static void human_readable_adler32(unsigned char *adler32_human, uint32_t adler32) {
    unsigned int i;
    unsigned char * adler32_char = (unsigned char*)&adler32;
    unsigned char * adler32_ptr = adler32_human;
    for (i = 0; i < sizeof(uint32_t); i++) { 
        sprintf(adler32_ptr, "%02x", adler32_char[sizeof(uint32_t)-1-i]);
        adler32_ptr++;
        adler32_ptr++;
    }
    adler32_ptr = '\0';
}

static void emit_cvmfs_chunk(hdfs_handle_t *hdfs_handle) {
    if (hdfs_handle->cur_chunk_bytes == 0) {return;}

    hdfs_handle->chunk_count++;
    if (hdfs_handle->chunk_count >= hdfs_handle->chunk_array_size) {
        hdfs_handle->chunk_array_size += 50;  // Enough to hold slightly more than 1GB of additional data.
        hdfs_handle->chunk_sha1_human = realloc(hdfs_handle->chunk_sha1_human, hdfs_handle->chunk_array_size * sizeof(char*));
        hdfs_handle->chunk_offsets = realloc(hdfs_handle->chunk_offsets, hdfs_handle->chunk_array_size * sizeof(globus_size_t));
    }
    globus_off_t prior_offset;
    if (hdfs_handle->chunk_count == 1) {
        hdfs_handle->chunk_offsets[hdfs_handle->chunk_count-1] = 0;
    } else {
        hdfs_handle->chunk_offsets[hdfs_handle->chunk_count-1] = hdfs_handle->chunk_offsets[hdfs_handle->chunk_count-2] + CVMFS_CHUNK_SIZE;
    }
    unsigned char sha1_value[EVP_MAX_MD_SIZE];
    int sha1_len;
    EVP_DigestFinal_ex(hdfs_handle->chunk_sha1, sha1_value, &sha1_len);
    EVP_DigestInit_ex(hdfs_handle->chunk_sha1, EVP_sha1(), NULL);
    hdfs_handle->chunk_sha1_human[hdfs_handle->chunk_count-1] = malloc(2*sha1_len+1);
    human_readable_evp(hdfs_handle->chunk_sha1_human[hdfs_handle->chunk_count-1], sha1_value, sha1_len);
}

static void emit_cvmfs_graft(hdfs_handle_t *hdfs_handle) {
    globus_size_t size = OUTPUT_BUFFER_STARTING_SIZE;
    hdfs_handle->cvmfs_graft = malloc(size);
    globus_off_t offset = 0;
    if (!hdfs_handle->cvmfs_graft) {return;}

    hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, "size=%lld;checksum=%s", hdfs_handle->offset, hdfs_handle->file_sha1_human);
    if (hdfs_handle->chunk_count < 2) {
        hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, ";chunk_offsets=0;chunk_checksums=%s", hdfs_handle->file_sha1_human);
    } else {
        hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, ";chunk_offsets=0");
        unsigned int idx;
        for (idx = 1; idx<hdfs_handle->chunk_count; idx++) {
            hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, ",%lld", hdfs_handle->chunk_offsets[idx]);
        }
        hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, ";chunk_checksums=%s", hdfs_handle->chunk_sha1_human[0]);
        for (idx = 1; idx<hdfs_handle->chunk_count; idx++) {
            hdfs_handle->cvmfs_graft = concatenate(hdfs_handle->cvmfs_graft, &offset, &size, ",%s", hdfs_handle->chunk_sha1_human[idx]);
        }
    }
}

/*
 *  Initialize all the checksum calculations
 */
void hdfs_initialize_checksums(hdfs_handle_t *hdfs_handle) {

    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CKSUM) {
        hdfs_handle->cksum = 0;
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CRC32) {
        hdfs_handle->crc32 = crc32(0, NULL, 0);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_ADLER32) {
        hdfs_handle->adler32 = adler32(0, NULL, 0);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_MD5) {
        MD5_Init(&hdfs_handle->md5);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CVMFS) {
        hdfs_handle->file_sha1 = EVP_MD_CTX_create();
        EVP_DigestInit_ex(hdfs_handle->file_sha1, EVP_sha1(), NULL);
        hdfs_handle->chunk_sha1 = EVP_MD_CTX_create();
        EVP_DigestInit_ex(hdfs_handle->chunk_sha1, EVP_sha1(), NULL);
    }
}

/*
 *  Update all the checksums requested
 */
void hdfs_update_checksums(hdfs_handle_t *hdfs_handle, globus_byte_t *buffer, globus_size_t nbytes) {

    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CKSUM) {
        // Checksum algorithm from POSIX standard.
        globus_size_t bc = nbytes;
        unsigned char * cp = buffer;
        uint32_t crc = hdfs_handle->cksum;
        while (bc--) {
            crc = (crc << 8) ^ crctab[((crc >> 24) ^ *cp++) & 0xFF];
        }
        hdfs_handle->cksum = crc;
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CRC32) {
        hdfs_handle->crc32 = crc32(hdfs_handle->crc32, buffer, nbytes);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_ADLER32) {
        hdfs_handle->adler32 = adler32(hdfs_handle->adler32, buffer, nbytes);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_MD5) {
        MD5_Update(&hdfs_handle->md5, buffer, nbytes);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CVMFS) {
        EVP_DigestUpdate(hdfs_handle->file_sha1, buffer, nbytes);
        globus_off_t total_bytes = hdfs_handle->cur_chunk_bytes + nbytes;
        size_t buffer_offset = 0;
        while (total_bytes >= CVMFS_CHUNK_SIZE) {  // There are at least CVMFS_CHUNK_SIZE bytes to write!
            globus_size_t new_bytes = CVMFS_CHUNK_SIZE-hdfs_handle->cur_chunk_bytes;
            EVP_DigestUpdate(hdfs_handle->chunk_sha1, buffer+buffer_offset, new_bytes);
            hdfs_handle->cur_chunk_bytes = CVMFS_CHUNK_SIZE;
            buffer_offset += new_bytes;
            nbytes -= new_bytes;
            emit_cvmfs_chunk(hdfs_handle);
            hdfs_handle->cur_chunk_bytes = 0;
            total_bytes -= CVMFS_CHUNK_SIZE;
        }
        EVP_DigestUpdate(hdfs_handle->chunk_sha1, buffer+buffer_offset, nbytes);
        hdfs_handle->cur_chunk_bytes += nbytes;
    }
}

/*
 *  Finish all the checksum calculations
 */
void hdfs_finalize_checksums(hdfs_handle_t *hdfs_handle) {

    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CKSUM) {
        globus_off_t length;
        uint32_t crc = hdfs_handle->cksum;
        for (length = hdfs_handle->offset; length; length >>= 8) {
            crc = (crc << 8) ^ crctab[((crc >> 24) ^ length) & 0xFF];
        }
        crc = ~crc & 0xFFFFFFFF;
        hdfs_handle->cksum = crc;
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum CKSUM: %u\n", hdfs_handle->cksum);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_ADLER32) {
        human_readable_adler32(hdfs_handle->adler32_human, hdfs_handle->adler32);
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum ADLER32: %s\n", hdfs_handle->adler32_human);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_MD5) {
        MD5_Final(hdfs_handle->md5_output, &hdfs_handle->md5);
        human_readable_evp(hdfs_handle->md5_output_human, hdfs_handle->md5_output, MD5_DIGEST_LENGTH);
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum MD5: %s\n", hdfs_handle->md5_output_human);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CRC32) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum CRC32: %u\n", hdfs_handle->crc32);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CVMFS) {
        unsigned char sha1_value[EVP_MAX_MD_SIZE];
        int sha1_len;
        EVP_DigestFinal_ex(hdfs_handle->file_sha1, sha1_value, &sha1_len);
        EVP_MD_CTX_destroy(hdfs_handle->file_sha1);
        human_readable_evp(hdfs_handle->file_sha1_human, sha1_value, sha1_len);
        emit_cvmfs_chunk(hdfs_handle);
        EVP_MD_CTX_destroy(hdfs_handle->chunk_sha1);
        emit_cvmfs_graft(hdfs_handle);
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO,
            "Checksum CVMFS: %s\n", hdfs_handle->cvmfs_graft);
    }
}


/*
 *  Save checksums.
 */
globus_result_t hdfs_save_checksum(hdfs_handle_t *hdfs_handle) {

    globus_result_t rc = GLOBUS_SUCCESS;

    GlobusGFSName(hdfs_save_checksum);

    if (!hdfs_handle->cksm_types || !hdfs_handle->cksm_root) {
        return rc;
    }

    hdfsFS fs = hdfsConnectAsUser("default", 0, "root");
    if (fs == NULL) {
        SystemError(hdfs_handle, "Failure in connecting to HDFS for checksum upload", rc);
        return rc;
    }

    size_t cksm_len = strlen(hdfs_handle->cksm_root);
    size_t path_len = strlen(hdfs_handle->pathname);
    size_t filelen = cksm_len + path_len + 2;
    char * filename = malloc(filelen);
    if (!filename) {
        MemoryError(hdfs_handle, "Unable to allocate new filename", rc);
    }
    memcpy(filename, hdfs_handle->cksm_root, cksm_len);
    filename[cksm_len] = '/';
    memcpy(filename+cksm_len+1, hdfs_handle->pathname, path_len);
    filename[filelen-1] = '\0';

    hdfsFile fh = hdfsOpenFile(fs, filename, O_WRONLY, 0, 0, 0);
    if (fh == NULL) {
        SystemError(hdfs_handle, "Failed to open checksum file", rc);
        return rc;
    }

    globus_size_t size = OUTPUT_BUFFER_STARTING_SIZE;
    globus_off_t offset = 0;
    char *buffer = malloc(size);
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CKSUM) {
        buffer = concatenate(buffer, &offset, &size, "CKSUM:%u\n", hdfs_handle->cksum);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CRC32) {
        buffer = concatenate(buffer, &offset, &size, "CRC32:%u\n", hdfs_handle->crc32);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_ADLER32) {
        buffer = concatenate(buffer, &offset, &size, "ADLER32:%s\n", hdfs_handle->adler32_human);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_MD5) {
        hdfs_handle->md5_output_human[MD5_DIGEST_LENGTH*2] = '\0';
        buffer = concatenate(buffer, &offset, &size, "MD5:%s\n", hdfs_handle->md5_output_human);
    }
    if (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CVMFS) {
        buffer = concatenate(buffer, &offset, &size, "CVMFS:%s\n", hdfs_handle->cvmfs_graft);
    }
    if (buffer == NULL) {
        MemoryError(hdfs_handle, "Failed to allocate checksum string", rc);
        // Returns # of bytes, -1 on err
    } else if (hdfsWrite(fs, fh, buffer, strlen(buffer)) < 0) {
        SystemError(hdfs_handle, "Failed to write checksum file", rc);
    }

    // return -1 on err
    if (hdfsCloseFile(fs, fh) < 0) {
        SystemError(hdfs_handle, "Failed to close checksum file", rc);
    }

    if (rc == GLOBUS_SUCCESS) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Saved checksums to %s.\n", filename);
    }

    free(buffer);

    // Note we purposely leak the filesystem handle, as Hadoop has disconnect issues.
    return rc;
}

static globus_result_t
hdfs_calculate_checksum(hdfs_handle_t *hdfs_handle, const char *type)
{
    globus_result_t rc = GLOBUS_SUCCESS;

    GlobusGFSName(hdfs_calculate_checksum);

    hdfs_initialize_checksums(hdfs_handle);

    hdfsFile fd = hdfsOpenFile(hdfs_handle->fs, hdfs_handle->pathname, O_RDONLY, 0, 1, 0);
    if (fd == NULL) {
        SystemError(hdfs_handle, "Failed to open file for checksumming", rc)
        return rc;
    }

    const size_t cksum_buffer_size = 1024*1024;
    void *buffer = malloc(cksum_buffer_size);
    if (buffer == NULL) {
        MemoryError(hdfs_handle, "Unable to allocate checksum temp buffer", rc);
        hdfsCloseFile(hdfs_handle->fs, fd);
        return rc;
    }
    ssize_t retval = 0;
    globus_off_t offset = 0;
    do {
        hdfs_update_checksums(hdfs_handle, buffer, retval);
        errno = 0;  // older versions of libhdfs sometimes fails to reset errno.
        retval = hdfsRead(hdfs_handle->fs, fd, buffer, cksum_buffer_size);
        if ((retval == -1) && (errno == EINTR)) {continue;}
        offset += retval;
    } while (retval > 0);
    if (retval == -1) {
        SystemError(hdfs_handle, "Failed to read from file for checksumming", rc);
        // Fall-through
    }
    hdfsCloseFile(hdfs_handle->fs, fd);

    if (rc == GLOBUS_SUCCESS) {
        hdfs_handle->offset = offset;
        hdfs_finalize_checksums(hdfs_handle);
        rc = hdfs_save_checksum(hdfs_handle);
    }
    return rc;
}

/*
 * Returns 1 if the requested checksum type is supported; 0 otherwise.
 */
static int
hdfs_checksum_type_supported(hdfs_handle_t *hdfs_handle, const char * requested_cksm) {
    return (
            (!strcasecmp("MD5", requested_cksm) && (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_MD5)) ||
            (!strcasecmp("CKSUM", requested_cksm) && (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CKSUM)) ||
            (!strcasecmp("CRC32", requested_cksm) && (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CRC32)) ||
            (!strcasecmp("ADLER32", requested_cksm) && (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_ADLER32)) ||
            (!strcasecmp("CVMFS", requested_cksm) && (hdfs_handle->cksm_types & HDFS_CKSM_TYPE_CVMFS))
           );
}

/*
 *  Retrieve checksums.
 */
static globus_result_t hdfs_get_checksum_internal(hdfs_handle_t *hdfs_handle, const char * pathname, const char * requested_cksm, char**cksm_value, int recurse);

globus_result_t hdfs_get_checksum(hdfs_handle_t *hdfs_handle, const char * pathname, const char * requested_cksm, char ** cksm_value) {
    return hdfs_get_checksum_internal(hdfs_handle, pathname, requested_cksm, cksm_value, 1);
}

globus_result_t hdfs_get_checksum_internal(hdfs_handle_t *hdfs_handle, const char * pathname, const char * requested_cksm, char**cksm_value, int recurse) {

    globus_result_t rc = GLOBUS_SUCCESS;

    GlobusGFSName(hdfs_get_checksum);

    hdfsFS fs = hdfsConnectAsUser("default", 0, "root");
    if (fs == NULL) {
        SystemError(hdfs_handle, "Failure in connecting to HDFS for checksumming.", rc);
        return rc;
    }

    // Not used in this function except in the contents of the error message.
    hdfs_handle->pathname = strdup(pathname);

    size_t cksm_len = strlen(hdfs_handle->cksm_root);
    size_t path_len = strlen(pathname);
    size_t filelen = cksm_len + path_len + 2;
    char * filename = malloc(filelen);
    if (!filename) {
        MemoryError(hdfs_handle, "Unable to allocate new filename", rc);
    }
    memcpy(filename, hdfs_handle->cksm_root, cksm_len);
    filename[cksm_len] = '/';
    memcpy(filename+cksm_len+1, pathname, path_len);
    filename[filelen-1] = '\0';

    hdfsFile fh = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
    if (fh == NULL) {
        rc = hdfs_calculate_checksum(hdfs_handle, requested_cksm);
        if (rc != GLOBUS_SUCCESS) {return rc;}
        fh = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
        if (fh == NULL) {
            SystemError(hdfs_handle, "Failed to open checksum file", rc);
            return rc;
        }
    }

    globus_size_t size = OUTPUT_BUFFER_STARTING_SIZE;
    char *buffer = malloc(size);
    char *read_buffer = malloc(OUTPUT_BUFFER_STARTING_SIZE);
    globus_off_t off = 0;
    if (!read_buffer || !buffer) {
        MemoryError(hdfs_handle, "Failed to allocate checksum read buffers", rc);
        return rc;
    }
    tSize retval = 0;
    do {
        do {
            errno = 0;  // Some versions of libhdfs forget to clear errno internally.
            retval = hdfsRead(fs, fh, read_buffer, OUTPUT_BUFFER_STARTING_SIZE-1);
        } while ((retval < 0) && errno == EINTR);

        if (retval > 0) {
            read_buffer[retval] = '\0';
            buffer = concatenate(buffer, &off, &size, "%s", read_buffer);
        }
    } while (retval > 0);
    free(read_buffer);

    if (retval < 0) {
        SystemError(hdfs_handle, "Failed to read checksum file", rc);
        free(buffer);
        return rc;
    }

    unsigned int length = 0;
    const char * ptr = buffer;
    char *cksm = malloc(size);
    char *val;
    *cksm_value = NULL;
    if (!cksm || !buffer) {
        MemoryError(hdfs_handle, "Failed to allocate checksum parse buffers", rc);
        free(buffer);
        return rc;
    }
    // Raise your hand if you hate string parsing in C.
    while (sscanf(ptr, "%s%n", cksm, &length) == 1) {
        //globus_gfs_log_message(GLOBUS_GFS_LOG_DUMP, "Checksum line: %s.\n", cksm);
        if (strlen(cksm) < 2) {
            GenericError(hdfs_handle, "Too-short entry for checksum", rc);
            break;
        }
        val = strchr(cksm, ':');
        if (val == NULL) {
            GenericError(hdfs_handle, "Invalid format of checksum entry.", rc);
            break;
        }
        *val = '\0';
        val++;
        if (*val == '\0') {
            GenericError(hdfs_handle, "Checksum value not specified", rc);
            break;
        }
        if (strcasecmp(cksm, requested_cksm) == 0) {
            *cksm_value = strdup(val);
            break;
        }
        ptr += length;
        if (*ptr == '\0') {
            break;
        }
        if (*ptr != '\n') {
            // Error;
            GenericError(hdfs_handle, "Invalid format of checksum entry (Not a newline)", rc);
            break;
        }
        ptr += 1;
        if (*ptr == '\0') {
            char * err_str = globus_common_create_string("Requested checksum type %s not found.", requested_cksm);
            GenericError(hdfs_handle, err_str, rc);
            globus_free(err_str);
            break;
        }
    }

    if (*cksm_value == NULL) {
        if (recurse && hdfs_checksum_type_supported(hdfs_handle, requested_cksm)) {
            // Try clearing the current checksum file and re-opening.
            hdfsCloseFile(fs, fh);
            hdfsDelete(fs, filename, 0);
            rc = hdfs_get_checksum_internal(hdfs_handle, pathname, requested_cksm, cksm_value, 0);
            fh = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
        }
        if ((*cksm_value == NULL) && (rc == GLOBUS_SUCCESS)) {
            GenericError(hdfs_handle, "Failed to retrieve checksum", rc);
        }
    }

    // return -1 on err; we check for fh being null here as it may have been re-opened if we regenerate the checksum file above.
    if (fh && (hdfsCloseFile(fs, fh) < 0)) {
        SystemError(hdfs_handle, "Failed to close checksum file", rc);
    }

    if (rc == GLOBUS_SUCCESS) {
        globus_gfs_log_message(GLOBUS_GFS_LOG_INFO, "Got checksum (%s:%s) for %s.\n", requested_cksm, *cksm_value, filename);
    }

    if (hdfs_handle->pathname) {
        free(hdfs_handle->pathname);
    }
    free(cksm);
    free(buffer);
    // Note we purposely leak the filesystem handle (fs), as Hadoop has disconnect issues.
    return rc;
}

void
hdfs_parse_checksum_types(hdfs_handle_t * hdfs_handle, const char * types) {

    hdfs_handle->cksm_types = 0;
    if (strstr(types, "MD5") != NULL) {
        hdfs_handle->cksm_types |= HDFS_CKSM_TYPE_MD5;
    }
    if (strstr(types, "CKSUM")) {
        hdfs_handle->cksm_types |= HDFS_CKSM_TYPE_CKSUM;
    }
    if (strstr(types, "CRC32")) {
        hdfs_handle->cksm_types |= HDFS_CKSM_TYPE_CRC32;
    }
    if (strstr(types, "ADLER32")) {
        hdfs_handle->cksm_types |= HDFS_CKSM_TYPE_ADLER32;
    }
    if (strstr(types, "CVMFS")) {
        hdfs_handle->cksm_types |= HDFS_CKSM_TYPE_CVMFS;
    }
}
